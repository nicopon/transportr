using Microsoft.IO;
using TransportR.Client.Protocol;

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
    /// <summary>
    /// The chunk type the batches of this source are written with.
    /// </summary>
    ChunkType BatchType { get; }

    /// <summary>
    /// Opens the source on the send window of the session, once, before anything else is called.
    /// </summary>
    /// <param name="inFlightSlots"><c>null</c> when the window is unbounded.</param>
    /// <param name="releaseSlot">Gives back the slot of a batch that never reaches the wire.</param>
    void Open(SemaphoreSlim? inFlightSlots, Action releaseSlot);

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

    /// <summary>
    /// Cuts the producer's end loose from a source that is complete and disposed: what it adds from
    /// then on is dropped without a word, where a source that was only disposed refuses it.
    /// </summary>
    void Detach();
}

/// <summary>
/// A batch read from an <see cref="IBatchSource"/>. <paramref name="Body"/> holds a 4-byte count,
/// then the payload.
/// </summary>
/// <param name="Count">The items of the batch; 0 for a source of bytes, which has none.</param>
internal readonly record struct OutgoingBatch(RecyclableMemoryStream Body, int BatchId, int Count);
