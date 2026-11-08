namespace TransportR.Client.Batching;

/// <summary>
/// The producer's end of a session's <see cref="IBatchSource"/>: what the sender's handle adds to
/// the transfer.
/// </summary>
/// <remarks>
/// Built by the client, which knows the item type. Before the source is opened, or once
/// it is complete and detached, an addition is dropped without a word; once it is only disposed, an
/// addition fails.
/// </remarks>
internal interface ISendPort<T>
{
    /// <summary>
    /// The source the session reads the batches from.
    /// </summary>
    IBatchSource Source { get; }

    Task SendAsync(T item, CancellationToken token);

    Task SendRangeAsync(IEnumerable<T> items, CancellationToken token);
}
