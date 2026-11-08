namespace TransportR.Client.Batching;

/// <summary>
/// The consumer's end of a session's <see cref="IChunkSink"/>: what the receiver's handle reads the
/// transfer from, and the sink the session writes it to.
/// </summary>
/// <remarks>
/// Built by the client, which knows the item type. The sink is made when the session
/// first starts receiving, and kept across reconnections: the consumer's enumeration is over it.
/// </remarks>
internal interface IReceivePort<T>
{
    /// <summary>
    /// Makes the sink the session writes to. Called once, when the session starts receiving.
    /// </summary>
    IChunkSink OpenSink();

    /// <summary>
    /// The items of the transfer.
    /// </summary>
    /// <exception cref="InvalidOperationException">The session has not started receiving.</exception>
    IAsyncEnumerable<T> ReceiveAsync(CancellationToken token);
}
