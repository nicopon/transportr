namespace TransportR.Client.Batching;

/// <summary>
/// A chunk sink with the consumer's end of it: what a receiving session writes the transfer to,
/// and what its consumer reads the transfer from.
/// </summary>
/// <remarks>
/// Built by the client, which knows the item type (<see cref="IReceivePort{T}.OpenSink"/>).
/// </remarks>
internal interface IReceiveSink<T> : IChunkSink
{
    /// <summary>
    /// The items of the transfer, in order.
    /// </summary>
    IAsyncEnumerable<T> ReadItemsAsync(CancellationToken ct);
}
