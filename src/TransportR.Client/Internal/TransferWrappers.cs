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
