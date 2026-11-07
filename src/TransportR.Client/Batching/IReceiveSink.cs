namespace TransportR.Client.Batching;

/// <summary>
/// A chunk sink with the consumer's end of it: what a receiving session writes the transfer to,
/// and what its consumer reads the transfer from.
/// </summary>
/// <remarks>
/// Built by the client, which knows the item type and the mode
/// (<see cref="Sessions.ISessionConfig{T}.CreateReceiveSink"/>): the session takes whichever sink
/// it is given without choosing between them.
/// </remarks>
internal interface IReceiveSink<T> : IChunkSink
{
    /// <summary>
    /// The items of the transfer, in order; in the byte-stream mode, one <c>byte[]</c> per batch.
    /// </summary>
    IAsyncEnumerable<T> ReadItemsAsync(CancellationToken ct);

    /// <summary>
    /// The batches of a byte-stream transfer as they were received. Each <see cref="ByteChunk"/> is
    /// the reader's to dispose.
    /// </summary>
    /// <exception cref="InvalidOperationException">The sink decodes its batches into items.</exception>
    IAsyncEnumerable<ByteChunk> ReadChunksAsync(CancellationToken ct);
}
