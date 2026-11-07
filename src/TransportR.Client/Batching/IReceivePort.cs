namespace TransportR.Client.Batching;

/// <summary>
/// The consumer's end of a session's <see cref="IChunkSink"/>: what the receiver's handle reads the
/// transfer from, and the sink the session writes it to.
/// </summary>
/// <remarks>
/// Built by the client, which knows the item type and the mode. The sink is made when the session
/// first starts receiving, and kept across reconnections: the consumer's enumeration is over it.
/// </remarks>
internal interface IReceivePort<T>
{
    /// <summary>
    /// Makes the sink the session writes to. Called once, when the session starts receiving.
    /// </summary>
    IChunkSink OpenSink();

    /// <summary>
    /// <c>true</c> when the transfer carries a stream of bytes, with no serializer
    /// (<see cref="Configuration.ClientOptions.ByteStreamBatchBytes"/>).
    /// </summary>
    bool IsByteStream { get; }

    /// <summary>
    /// The items of the transfer; for a stream of bytes, one <c>byte[]</c> per batch received.
    /// </summary>
    /// <exception cref="InvalidOperationException">The session has not started receiving.</exception>
    IAsyncEnumerable<T> ReceiveAsync(CancellationToken token);

    /// <summary>
    /// The batches of a byte-stream transfer as they were received. Each <see cref="ByteChunk"/> is
    /// the consumer's to dispose; one it leaves unread when the session is released is disposed there.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// The session has not started receiving, or the transfer decodes its batches into items.
    /// </exception>
    IAsyncEnumerable<ByteChunk> ReceiveChunksAsync(CancellationToken token);
}
