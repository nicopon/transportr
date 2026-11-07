using System.Buffers;

namespace TransportR.Client.Batching;

/// <summary>
/// Where a received batch body goes once <see cref="ReceiverState{T}"/> has put it in order.
/// </summary>
/// <remarks>
/// A sink lives as long as the receive buffer behind it, across reconnections: the consumer's
/// enumeration is over that buffer, and a sink replaced around a new one would leave it waiting on
/// a buffer nothing writes to.
/// </remarks>
internal interface IChunkSink
{
    /// <summary>
    /// Hands one batch body to the consumer's side, waiting for room if the buffer is bounded.
    /// </summary>
    /// <param name="owner">The memory <paramref name="body"/> lives in.</param>
    /// <param name="body">The batch payload.</param>
    /// <param name="ct">Cancels the wait for room, not the hand-off that follows it.</param>
    /// <returns>
    /// <c>true</c> when the sink now owns <paramref name="owner"/> and will dispose it; <c>false</c>
    /// when it is still the caller's. A sink that throws has not taken it: the caller disposes it
    /// once, and a pooled buffer returned twice is handed to two renters.
    /// </returns>
    Task<bool> WriteAsync(IMemoryOwner<byte> owner, ReadOnlyMemory<byte> body, CancellationToken ct);

    /// <summary>
    /// Signals that no more batches will be written.
    /// </summary>
    void Complete();

    /// <summary>
    /// Ends delivery with <paramref name="error"/>, which the consumer is thrown once it has taken
    /// what was admitted.
    /// </summary>
    void Fail(Exception error);

    /// <summary>
    /// Returns the pooled memory of what was admitted and not taken, for a session that is released.
    /// After <see cref="Complete"/>, which cannot do it itself: a consumer still takes what was
    /// admitted once the transfer is over. What a consumer holds is not touched.
    /// </summary>
    void DiscardUnread();
}
