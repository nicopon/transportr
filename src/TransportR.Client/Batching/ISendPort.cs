namespace TransportR.Client.Batching;

/// <summary>
/// The producer's end of a session's <see cref="IBatchSource"/>: what the sender's handle adds to
/// the transfer.
/// </summary>
/// <remarks>
/// Built by the client, which knows the item type and the mode. Before the source is opened, or once
/// it is complete and detached, an addition is dropped without a word; once it is only disposed, an
/// addition fails.
/// </remarks>
internal interface ISendPort<T>
{
    /// <summary>
    /// The source the session reads the batches from.
    /// </summary>
    IBatchSource Source { get; }

    /// <summary>
    /// <c>true</c> when the transfer carries a stream of bytes, with no serializer
    /// (<see cref="Configuration.ClientOptions.ByteStreamBatchBytes"/>).
    /// </summary>
    bool IsByteStream { get; }

    /// <summary>
    /// Adds an item; for a stream of bytes, appends its bytes, where no boundary between two calls
    /// survives.
    /// </summary>
    Task SendAsync(T item, CancellationToken token);

    Task SendRangeAsync(IEnumerable<T> items, CancellationToken token);

    /// <summary>
    /// Appends bytes to the stream of a byte-stream transfer. They are copied before this returns.
    /// </summary>
    Task SendBytesAsync(ReadOnlyMemory<byte> bytes, CancellationToken token);
}
