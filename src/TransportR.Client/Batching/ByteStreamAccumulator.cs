using System.Buffers.Binary;
using System.Threading.Channels;
using Microsoft.IO;

namespace TransportR.Client.Batching;

/// <summary>
/// Cuts a stream of bytes into batches of at most a byte budget, and publishes each one to a channel
/// for the send worker. No boundary of the caller's survives: a batch is cut at the budget, when the
/// flush interval has elapsed with bytes pending, or at the end.
/// </summary>
/// <remarks>
/// <para>
/// A batch is a <see cref="RecyclableMemoryStream"/> holding a 4-byte length, then the body. Its
/// in-flight slot is taken when an empty batch receives its first byte, <b>before</b> the stream is
/// rented: a full window parks the producer before it holds any batch memory, which is what bounds
/// that memory by budget × window. The slot leaves with the batch on the channel; the reader of the
/// channel owns both from then on.
/// </para>
/// <para>
/// A slot taken here and never handed off - the batch released by <see cref="CompleteAsync"/>
/// empty, by <see cref="Abandon"/> or <see cref="Dispose"/>, or refused by a closed channel - is given
/// back through the callback passed to the constructor, exactly once, with its stream disposed.
/// </para>
/// </remarks>
internal sealed class ByteStreamAccumulator : IDisposable
{
    private const int LengthPrefixBytes = sizeof(int);

    private readonly int _batchBytes;
    private readonly int _flushIntervalMs;
    private readonly RecyclableMemoryStreamManager _streams;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly SemaphoreSlim? _inFlightSlots;
    private readonly Action _releaseSlot;
    private readonly CancellationTokenSource _disposeCts = new();
    private readonly Channel<(RecyclableMemoryStream Batch, int BatchId)> _batchChannel;

    // Non-null exactly while a slot is held for it. Read and written under _lock only: a stream
    // disposed under a producer still writing into it would write into a block another stream owns.
    private RecyclableMemoryStream? _current;
    private DateTime _lastFlushTime;
    private int _batchId;
    private Task? _flushIntervalTask;
    private int _disposed;

    /// <param name="releaseSlot">
    /// Gives back a slot of <paramref name="inFlightSlots"/> taken for a batch that is never handed
    /// off. Not called without a window.
    /// </param>
    public ByteStreamAccumulator(
        int batchBytes,
        int flushIntervalMs,
        RecyclableMemoryStreamManager streams,
        SemaphoreSlim? inFlightSlots,
        Action releaseSlot)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(batchBytes, 0, nameof(batchBytes));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(flushIntervalMs, 0, nameof(flushIntervalMs));

        _batchBytes = batchBytes;
        _flushIntervalMs = flushIntervalMs;
        _streams = streams;
        _inFlightSlots = inFlightSlots;
        _releaseSlot = releaseSlot;
        _lastFlushTime = DateTime.UtcNow;

        _batchChannel = Channel.CreateUnbounded<(RecyclableMemoryStream, int)>(new UnboundedChannelOptions
        {
            SingleReader = false, // The send worker, and DiscardHeldAsync for what it left
            SingleWriter = true
        });
    }

    /// <summary>
    /// The batches, in order. Each stream is the reader's to dispose, and each holds a slot the
    /// reader gives back once the batch is acknowledged or fails.
    /// </summary>
    public ChannelReader<(RecyclableMemoryStream Batch, int BatchId)> BatchReader => _batchChannel.Reader;

    public void StartFlushInterval()
    {
        if (_flushIntervalTask != null) return;
        _flushIntervalTask = FlushIntervalLoop(_disposeCts.Token);
    }

    /// <summary>
    /// Appends <paramref name="bytes"/> to the stream. They are copied before this returns, so the
    /// caller may reuse the memory at once. A call that fails - cancelled while parked on the window,
    /// or after the accumulator was abandoned - may have appended part of the bytes; the batch it
    /// leaves open stays here, for <see cref="CompleteAsync"/>, <see cref="Abandon"/> or
    /// <see cref="Dispose"/>.
    /// </summary>
    public async ValueTask AddAsync(ReadOnlyMemory<byte> bytes, CancellationToken ct = default)
    {
        await _lock.WaitAsync(ct);
        try
        {
            while (!bytes.IsEmpty)
            {
                if (_current is null) await OpenBatchAsync(ct);

                var take = Math.Min(_batchBytes - BodyLength, bytes.Length);
                _current!.Write(bytes.Span[..take]);
                bytes = bytes[take..];

                if (BodyLength == _batchBytes) Flush();
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Hands off the open batch, if it holds any byte, and completes the channel.
    /// </summary>
    public async ValueTask CompleteAsync(CancellationToken ct = default)
    {
        await _lock.WaitAsync(ct);
        try
        {
            // An open batch with no byte in it is not sent: a batch of the byte-stream mode is
            // never empty.
            if (_current is not null)
            {
                if (BodyLength > 0) Flush();
                else DiscardCurrent();
            }
        }
        finally
        {
            _lock.Release();
        }
        _batchChannel.Writer.TryComplete();
    }

    private int BodyLength => (int)_current!.Length - LengthPrefixBytes;

    /// <summary>
    /// Takes a slot, then rents the batch. Must be called under lock, with no batch open.
    /// </summary>
    private async ValueTask OpenBatchAsync(CancellationToken ct)
    {
        // Abandon and Dispose release what is held once; a batch opened after them would be held
        // by nobody.
        if (_disposeCts.IsCancellationRequested)
            throw new OperationCanceledException("The accumulator no longer accepts bytes.");

        // Linked to _disposeCts so a producer parked here is released when the accumulator goes
        // away rather than hanging on a window that will never reopen.
        if (_inFlightSlots != null)
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _disposeCts.Token);
            await _inFlightSlots.WaitAsync(linked.Token);
        }

        RecyclableMemoryStream? batch = null;
        try
        {
            batch = _streams.GetStream();
            // Room for the length, written when the batch is cut.
            batch.SetLength(LengthPrefixBytes);
            batch.Position = LengthPrefixBytes;
        }
        catch
        {
            batch?.Dispose();
            ReleaseSlot();
            throw;
        }
        _current = batch;
    }

    /// <summary>
    /// Hands the open batch off with its slot. Must be called under lock, with a batch open.
    /// </summary>
    private void Flush()
    {
        var batch = _current!;
        var bodyLength = BodyLength;
        _current = null;
        _lastFlushTime = DateTime.UtcNow;

        Span<byte> length = stackalloc byte[LengthPrefixBytes];
        BinaryPrimitives.WriteInt32LittleEndian(length, bodyLength);
        batch.Position = 0;
        batch.Write(length);

        // Unbounded, so this fails only once the channel is complete: abandoned, disposed, or
        // completed before this batch was opened. Nobody would read the batch.
        if (!_batchChannel.Writer.TryWrite((batch, _batchId++)))
        {
            batch.Dispose();
            ReleaseSlot();
            throw new ChannelClosedException();
        }
    }

    private async Task FlushIntervalLoop(CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_flushIntervalMs / 2, token);

                await _lock.WaitAsync(token);
                try
                {
                    if (_current is not null && BodyLength > 0
                        && (DateTime.UtcNow - _lastFlushTime).TotalMilliseconds >= _flushIntervalMs)
                    {
                        Flush();
                    }
                }
                finally
                {
                    _lock.Release();
                }
            }
            catch (Exception ex) when (ex is OperationCanceledException or ChannelClosedException)
            {
                break;
            }
        }
    }

    /// <summary>
    /// Releases a producer parked on the in-flight window, accepts no further byte, and gives back
    /// the open batch and the batches nobody has read, slots included.
    /// </summary>
    public void Abandon()
    {
        try
        {
            _disposeCts.Cancel();
            _batchChannel.Writer.TryComplete();
        }
        catch (ObjectDisposedException)
        {
            // Already disposed, which released everything already.
            return;
        }
        _ = DiscardHeldAsync();
    }

    public void Dispose()
    {
        // CompleteSenderAsync disposes it and a session released meanwhile does too; the second
        // Cancel() on the disposed source would throw.
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

        // Cancel first: a producer parked on the in-flight window unwinds through the finally that
        // releases _lock, so _lock must still be usable at that point. SemaphoreSlim only needs
        // disposing when AvailableWaitHandle has been touched, which it never is here.
        _disposeCts.Cancel();
        _batchChannel.Writer.TryComplete();
        _disposeCts.Dispose();
        _ = DiscardHeldAsync();
    }

    /// <summary>
    /// Disposes the open batch and the unread ones, and gives back their slots. Under the lock,
    /// because a producer may still be writing into the open batch: cancelling has released one
    /// parked on the window, so the wait is the length of a copy.
    /// </summary>
    private async Task DiscardHeldAsync()
    {
        await _lock.WaitAsync();
        try
        {
            DiscardCurrent();

            // The channel hands each batch out once, to the reader or here.
            while (_batchChannel.Reader.TryRead(out var unread))
            {
                unread.Batch.Dispose();
                ReleaseSlot();
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Disposes the open batch and gives back its slot. Must be called under lock.
    /// </summary>
    private void DiscardCurrent()
    {
        if (_current is null) return;

        _current.Dispose();
        _current = null;
        ReleaseSlot();
    }

    private void ReleaseSlot()
    {
        if (_inFlightSlots != null) _releaseSlot();
    }
}
