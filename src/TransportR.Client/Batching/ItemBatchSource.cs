using Microsoft.IO;
using TransportR.Interfaces;

namespace TransportR.Client.Batching;

/// <summary>
/// Serializes each batch of a <see cref="BatchAccumulator{T}"/> as the send worker reads it.
/// </summary>
/// <remarks>
/// Serializing in the read keeps one channel and one worker between the producer and the wire, and
/// leaves the slot where the accumulator took it, before the serialization. A batch is read once, so
/// <see cref="IBatchSerializer{T}.SerializeBatchAsync"/> runs at most once per batch.
/// </remarks>
/// <param name="releaseSlot">Gives back the slot of a batch that cannot be serialized.</param>
internal sealed class ItemBatchSource<T>(
    BatchAccumulator<T> accumulator,
    IBatchSerializer<T> serializer,
    RecyclableMemoryStreamManager streams,
    Action releaseSlot) : IBatchSource
{
    public void StartFlushInterval() => accumulator.StartFlushInterval();

    public ValueTask<bool> WaitToReadAsync(CancellationToken ct) =>
        accumulator.BatchReader.WaitToReadAsync(ct);

    /// <summary>
    /// Reads the next batch and serializes it behind its item count.
    /// </summary>
    public async ValueTask<OutgoingBatch?> TryReadAsync(CancellationToken ct)
    {
        if (!accumulator.BatchReader.TryRead(out var next)) return null;

        var (items, batchId) = next;
        RecyclableMemoryStream? body = null;
        try
        {
            body = streams.GetStream();
            var countBytes = BitConverter.GetBytes(items.Count);
            await body.WriteAsync(countBytes.AsMemory(), ct);
            await serializer.SerializeBatchAsync(body, items);
        }
        catch
        {
            body?.Dispose();
            releaseSlot();
            throw;
        }
        return new OutgoingBatch(body, batchId, items.Count);
    }

    public ValueTask CompleteAsync(CancellationToken ct) => accumulator.CompleteAsync(ct);

    public void Abandon() => accumulator.Abandon();

    public void Dispose() => accumulator.Dispose();
}
