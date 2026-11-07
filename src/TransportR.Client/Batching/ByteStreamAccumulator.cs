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
/// channel owns both from then on, and disposes the stream.
/// </para>
/// <para>
/// A slot taken here and never handed off - the batch released by <see cref="Accumulator{TBatch}.CompleteAsync"/>
/// empty, by <see cref="Accumulator{TBatch}.Abandon"/> or <see cref="Accumulator{TBatch}.Dispose"/>, or
/// refused by a closed channel - is given back through the callback passed to the constructor,
/// exactly once, with its stream disposed.
/// </para>
/// </remarks>
internal sealed class ByteStreamAccumulator : Accumulator<RecyclableMemoryStream>
{
    private const int LengthPrefixBytes = sizeof(int);

    private readonly int _batchBytes;
    private readonly RecyclableMemoryStreamManager _streams;
    private readonly Action _releaseSlot;

    // Non-null exactly while a slot is held for it. Read and written under the lock only: a stream
    // disposed under a producer still writing into it would write into a block another stream owns.
    private RecyclableMemoryStream? _current;

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
        : base(flushIntervalMs, inFlightSlots)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(batchBytes, 0, nameof(batchBytes));

        _batchBytes = batchBytes;
        _streams = streams;
        _releaseSlot = releaseSlot;
    }

    protected override bool HasPending => _current is not null && BodyLength > 0;

    /// <summary>
    /// Appends <paramref name="bytes"/> to the stream. They are copied before this returns, so the
    /// caller may reuse the memory at once. A call that fails - cancelled while parked on the window,
    /// or after the accumulator was abandoned - may have appended part of the bytes; the batch it
    /// leaves open stays here, for <see cref="Accumulator{TBatch}.CompleteAsync"/>,
    /// <see cref="Accumulator{TBatch}.Abandon"/> or <see cref="Accumulator{TBatch}.Dispose"/>.
    /// </summary>
    public async ValueTask AddAsync(ReadOnlyMemory<byte> bytes, CancellationToken ct = default)
    {
        await Lock.WaitAsync(ct);
        try
        {
            while (!bytes.IsEmpty)
            {
                if (_current is null) await OpenBatchAsync(ct);

                var take = Math.Min(_batchBytes - BodyLength, bytes.Length);
                _current!.Write(bytes.Span[..take]);
                bytes = bytes[take..];

                if (BodyLength == _batchBytes) await FlushAsync(ct);
            }
        }
        finally
        {
            Lock.Release();
        }
    }

    protected override async ValueTask FlushRemainderAsync(CancellationToken ct)
    {
        // An open batch with no byte in it is not sent: a batch of the byte-stream mode is never
        // empty.
        if (_current is null) return;

        if (BodyLength > 0) await FlushAsync(ct);
        else DiscardCurrent();
    }

    private int BodyLength => (int)_current!.Length - LengthPrefixBytes;

    /// <summary>
    /// Takes a slot, then rents the batch. Must be called under lock, with no batch open.
    /// </summary>
    private async ValueTask OpenBatchAsync(CancellationToken ct)
    {
        // Abandon and Dispose release what is held once; a batch opened after them would be held
        // by nobody.
        if (IsReleased)
            throw new OperationCanceledException("The accumulator no longer accepts bytes.");

        await TakeSlotAsync(ct);

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
    protected override async ValueTask FlushAsync(CancellationToken ct)
    {
        var batch = _current!;
        var bodyLength = BodyLength;
        _current = null;
        WriteLength(batch, bodyLength);

        try
        {
            // Not ct: only a closed channel may refuse the batch, the one refusal caught below. A
            // batch refused for a cancellation would be held by nobody.
            await HandOffAsync(batch, CancellationToken.None);
        }
        catch (ChannelClosedException)
        {
            // Unbounded, so this fails only once the channel is complete: abandoned, disposed, or
            // completed before this batch was opened. Nobody would read the batch.
            batch.Dispose();
            ReleaseSlot();
            throw;
        }
    }

    private static void WriteLength(RecyclableMemoryStream batch, int bodyLength)
    {
        Span<byte> length = stackalloc byte[LengthPrefixBytes];
        BinaryPrimitives.WriteInt32LittleEndian(length, bodyLength);
        batch.Position = 0;
        batch.Write(length);
    }

    protected override void OnReleased() => _ = DiscardHeldAsync();

    /// <summary>
    /// Disposes the open batch and the unread ones, and gives back their slots. Under the lock,
    /// because a producer may still be writing into the open batch: cancelling has released one
    /// parked on the window, so the wait is the length of a copy.
    /// </summary>
    private async Task DiscardHeldAsync()
    {
        await Lock.WaitAsync();
        try
        {
            DiscardCurrent();

            // The channel hands each batch out once, to the reader or here.
            while (BatchReader.TryRead(out var unread))
            {
                unread.Batch.Dispose();
                ReleaseSlot();
            }
        }
        finally
        {
            Lock.Release();
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
        if (HasWindow) _releaseSlot();
    }
}
