namespace TransportR.Client.Batching;

/// <summary>
/// The consumer's end of a byte-stream session: makes the <see cref="ByteChunkSink"/> the session writes
/// to when it first starts receiving, and reads the chunks from it.
/// </summary>
/// <remarks>
/// The sink is kept across reconnections: the consumer's reads are over it.
/// </remarks>
/// <param name="capacity">What the receive buffer may hold, or <c>null</c> for no bound.</param>
internal sealed class ByteReceivePort(BufferCapacity? capacity)
{
    private ByteChunkSink? _sink;

    /// <summary>
    /// Makes the sink the session writes to. Called once, when the session starts receiving.
    /// </summary>
    public IChunkSink OpenSink() => _sink ??= new ByteChunkSink(new ReceiveBuffer<ByteChunk>(capacity));

    /// <summary>
    /// The chunks of the transfer as they were received. Each is the reader's to dispose; one it leaves
    /// unread when the session is released is disposed there.
    /// </summary>
    /// <exception cref="InvalidOperationException">The session has not started receiving.</exception>
    public IAsyncEnumerable<ByteChunk> ReadAllAsync(CancellationToken token) =>
        (_sink ?? throw new InvalidOperationException("Receiver not started")).ReadAllAsync(token);
}
