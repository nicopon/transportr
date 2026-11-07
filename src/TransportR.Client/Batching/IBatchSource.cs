using Microsoft.IO;

namespace TransportR.Client.Batching;

/// <summary>
/// Where the send worker takes the batches of a transfer from, in order: each a body ready to
/// register, numbered, with its in-flight slot already taken.
/// </summary>
/// <remarks>
/// The sending counterpart of <see cref="IChunkSink"/>. A batch read is the reader's: it disposes
/// the body, and gives the slot back once the batch is acknowledged, or as soon as it fails short of
/// being registered.
/// </remarks>
internal interface IBatchSource : IDisposable
{
    void StartFlushInterval();

    /// <summary>
    /// Waits until a batch can be read: <c>false</c> once the source is complete and empty.
    /// </summary>
    ValueTask<bool> WaitToReadAsync(CancellationToken ct);

    /// <summary>
    /// Reads the next batch, producing its body, or returns <c>null</c> when none is ready.
    /// </summary>
    /// <remarks>
    /// A read that throws has disposed the body it rented and given the batch's slot back.
    /// </remarks>
    ValueTask<OutgoingBatch?> TryReadAsync(CancellationToken ct);

    /// <summary>
    /// Hands off what is pending and completes the source.
    /// </summary>
    ValueTask CompleteAsync(CancellationToken ct);

    /// <summary>
    /// Releases a producer parked on the in-flight window and accepts nothing more.
    /// </summary>
    void Abandon();
}

/// <summary>
/// A batch read from an <see cref="IBatchSource"/>. <paramref name="Body"/> holds a 4-byte count,
/// then the payload.
/// </summary>
/// <param name="Count">The items of the batch; in the byte-stream mode, the bytes of its payload.</param>
internal readonly record struct OutgoingBatch(RecyclableMemoryStream Body, int BatchId, int Count);
