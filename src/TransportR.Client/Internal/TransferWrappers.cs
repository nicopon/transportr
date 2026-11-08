using System.Runtime.CompilerServices;
using TransportR.Abstractions;
using TransportR.Client.Batching;
using TransportR.Client.Sessions;

namespace TransportR.Client.Internal;

/// <summary>
/// Handle for sending data to a receiver. Wraps an IDataSession.
/// </summary>
internal class HttpSend<T>(IDataSession session, ISendPort<T> port) : Send<T>(session.TransferId)
{
    public override Task SendAsync(T item, CancellationToken cancellationToken = default)
    {
        if (session.RefusedAsCompleted() is { } refused) return refused;

        return session.WhileSending(port.SendAsync(item, cancellationToken), cancellationToken);
    }

    internal bool IsByteStream => port.IsByteStream;

    internal Task SendBytesAsync(ReadOnlyMemory<byte> bytes, CancellationToken cancellationToken)
    {
        if (session.RefusedAsCompleted() is { } refused) return refused;

        return session.WhileSending(port.SendBytesAsync(bytes, cancellationToken), cancellationToken);
    }

    public override Task SendAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)
    {
        if (session.RefusedAsCompleted() is { } refused) return refused;

        return session.WhileSending(port.SendRangeAsync(items, cancellationToken), cancellationToken);
    }

    public override Task CompleteAsync(CancellationToken cancellationToken = default)
    {
        return session.CompleteAsync(cancellationToken);
    }

    protected override async ValueTask DisposeAsyncCore()
    {
        await session.DisposeAsync();
    }
}

/// <summary>
/// Handle for receiving data from a sender. Wraps an IDataSession.
/// </summary>
internal class HttpReceive<T>(IDataSession session, IReceivePort<T> port) : Receive<T>(session.TransferId, session.BatchSize)
{
    /// <summary>
    /// Ends only once the transfer has reached its outcome, not at the last item: the acknowledgement
    /// is still queued behind it, and a caller that stops as soon as items stop arriving can abandon a
    /// transfer it in fact received in full.
    /// </summary>
    public override async IAsyncEnumerable<T> ReceiveAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in port.ReceiveAsync(cancellationToken))
        {
            yield return item;
        }

        await session.WaitForCompletionAsync(cancellationToken);
    }

    internal bool IsByteStream => port.IsByteStream;

    /// <summary>
    /// The batches of a byte-stream transfer as they were received, ending as
    /// <see cref="ReceiveAsync"/> does. Each chunk is the reader's to dispose.
    /// </summary>
    internal async IAsyncEnumerable<ByteChunk> ReceiveChunksAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var chunk in port.ReceiveChunksAsync(cancellationToken))
        {
            yield return chunk;
        }

        await session.WaitForCompletionAsync(cancellationToken);
    }

    public override Task WaitForCompletionAsync(CancellationToken cancellationToken = default)
    {
        return session.WaitForCompletionAsync(cancellationToken);
    }

    protected override async ValueTask DisposeAsyncCore()
    {
        await session.DisposeAsync();
    }
}

/// <summary>
/// Handle for sending a stream of bytes to a receiver. Wraps an IDataSession.
/// </summary>
/// <param name="append">Appends bytes to the stream; copies them before it returns.</param>
internal sealed class HttpByteSend(IDataSession session, Func<ReadOnlyMemory<byte>, CancellationToken, Task> append)
    : ByteSend(session.TransferId)
{
    public override Task WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken cancellationToken = default)
    {
        if (session.RefusedAsCompleted() is { } refused) return refused;

        return session.WhileSending(append(bytes, cancellationToken), cancellationToken);
    }

    public override Task CompleteAsync(CancellationToken cancellationToken = default)
    {
        return session.CompleteAsync(cancellationToken);
    }

    protected override async ValueTask DisposeAsyncCore()
    {
        await session.DisposeAsync();
    }
}

/// <summary>
/// Handle for receiving a stream of bytes from a sender. Wraps an IDataSession.
/// </summary>
/// <remarks>
/// <para>
/// The chunk being read is held here, between calls, until its last byte is taken; the capacity the
/// receive buffer charged for it comes back when the next one is asked for, as it does for any reader.
/// A wait for the next chunk that a caller's token cuts is kept, not repeated: the chunk it yields is
/// the next call's.
/// </para>
/// <para>
/// A release while a consumer is inside leaves the chunk and the pending wait to that consumer, which
/// lets them go as it leaves.
/// </para>
/// </remarks>
/// <param name="readChunks">The chunks of the transfer, each the reader's to dispose.</param>
internal sealed class HttpByteReceive(IDataSession session, Func<CancellationToken, IAsyncEnumerable<ByteChunk>> readChunks)
    : ByteReceive(session.TransferId)
{
    private readonly CancellationTokenSource _lifetime = new();
    private IAsyncEnumerator<ByteChunk>? _chunks;
    private Task<bool>? _moving;
    private ByteChunk? _current;
    private int _offset;
    private int _inside;
    private int _released;

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (buffer.IsEmpty)
            throw new ArgumentException("The buffer is empty: a read of 0 bytes means the transfer is over.", nameof(buffer));

        Enter();
        try
        {
            if (!await FillAsync(cancellationToken).ConfigureAwait(false)) return 0;

            var chunk = _current!;
            var count = Math.Min(buffer.Length, chunk.Length - _offset);
            chunk.Memory.Slice(_offset, count).CopyTo(buffer);
            _offset += count;
            if (_offset == chunk.Length) ReleaseCurrent();
            return count;
        }
        finally
        {
            await LeaveAsync().ConfigureAwait(false);
        }
    }

    public override async Task CopyToAsync(Stream destination, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(destination);

        Enter();
        try
        {
            while (await FillAsync(cancellationToken).ConfigureAwait(false))
            {
                // The chunk is ours from the moment it is yielded, so a write that throws or is
                // cancelled must still give its pooled memory back.
                try
                {
                    await destination.WriteAsync(_current!.Memory[_offset..], cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    ReleaseCurrent();
                }
            }
        }
        finally
        {
            await LeaveAsync().ConfigureAwait(false);
        }
    }

    public override Task WaitForCompletionAsync(CancellationToken cancellationToken = default)
    {
        return session.WaitForCompletionAsync(cancellationToken);
    }

    protected override async ValueTask DisposeAsyncCore()
    {
        await session.DisposeAsync().ConfigureAwait(false);
        Volatile.Write(ref _released, 1);
        await LetGoAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Leaves a chunk with unread bytes in <see cref="_current"/>, or ends as the transfer does:
    /// <c>false</c> once every chunk has been read and the transfer has reached its outcome.
    /// </summary>
    private async ValueTask<bool> FillAsync(CancellationToken cancellationToken)
    {
        while (_current is null)
        {
            // Once released the enumerator is gone and its token cancelled: what the session says of a
            // handle let go is the answer, not the cancellation.
            if (Volatile.Read(ref _released) != 0)
            {
                await session.WaitForCompletionAsync(cancellationToken).ConfigureAwait(false);
                return false;
            }

            _chunks ??= readChunks(_lifetime.Token).GetAsyncEnumerator(_lifetime.Token);
            _moving ??= _chunks.MoveNextAsync().AsTask();

            if (!await _moving.WaitAsync(cancellationToken).ConfigureAwait(false))
            {
                await session.WaitForCompletionAsync(cancellationToken).ConfigureAwait(false);
                return false;
            }

            _moving = null;
            var chunk = _chunks.Current;
            if (chunk.Length == 0)
            {
                chunk.Dispose();
                continue;
            }

            _current = chunk;
            _offset = 0;
        }

        return true;
    }

    private void ReleaseCurrent()
    {
        var chunk = _current;
        _current = null;
        _offset = 0;
        chunk?.Dispose();
    }

    private void Enter()
    {
        if (Interlocked.CompareExchange(ref _inside, 1, 0) != 0)
            throw new InvalidOperationException("Another read or copy of this handle is under way.");
    }

    private async ValueTask LeaveAsync()
    {
        // A full fence: the release is read after the flag is cleared, and the other way round on its side.
        Interlocked.Exchange(ref _inside, 0);
        if (Volatile.Read(ref _released) != 0) await LetGoAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Gives back the chunk in hand and the one a cut wait may have yielded, unless a consumer is inside:
    /// it does so as it leaves.
    /// </summary>
    private async ValueTask LetGoAsync()
    {
        if (Interlocked.CompareExchange(ref _inside, 1, 0) != 0) return;
        try
        {
            ReleaseCurrent();
            var chunks = _chunks;
            var moving = _moving;
            _chunks = null;
            _moving = null;
            if (chunks is null) return;

            // The session's release ends a wait on the buffer; this ends one it did not.
            await _lifetime.CancelAsync().ConfigureAwait(false);
            if (moving is not null)
            {
                try
                {
                    if (await moving.ConfigureAwait(false)) chunks.Current.Dispose();
                }
                catch
                {
                    // The wait ended in the transfer's failure or in the cancellation above: it took nothing.
                }
            }

            await chunks.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            Interlocked.Exchange(ref _inside, 0);
        }
    }
}
