using Microsoft.IO;
using TransportR.Interfaces;
using TransportR.Protocol;

namespace TransportR.Client.Batching;

/// <summary>
/// The items of a transfer, batched and then serialized as the send worker reads each batch.
/// </summary>
/// <remarks>
/// Serializing in the read keeps one channel and one worker between the producer and the wire, and
/// leaves the slot where the accumulator took it, before the serialization. A batch is read once, so
/// <see cref="IBatchSerializer{T}.SerializeBatchAsync"/> runs at most once per batch.
/// </remarks>
internal sealed class ItemBatchSource<T>(
    IBatchSerializer<T> serializer,
    RecyclableMemoryStreamManager streams,
    int batchSize,
    int flushIntervalMs) : IBatchSource, ISendPort<T>
{
    private BatchAccumulator<T>? _accumulator;
    private Action _releaseSlot = static () => { };

    public IBatchSource Source => this;

    public ChunkType BatchType => ChunkType.Batch;

    private BatchAccumulator<T> Accumulator =>
        _accumulator ?? throw new InvalidOperationException("The source is not open.");

    public void Open(SemaphoreSlim? inFlightSlots, Action releaseSlot)
    {
        _releaseSlot = releaseSlot;
        _accumulator = new BatchAccumulator<T>(batchSize, flushIntervalMs, inFlightSlots);
    }

    public void StartFlushInterval() => Accumulator.StartFlushInterval();

    public ValueTask<bool> WaitToReadAsync(CancellationToken ct) =>
        Accumulator.BatchReader.WaitToReadAsync(ct);

    /// <summary>
    /// Reads the next batch and serializes it behind its item count. A batch that cannot be
    /// serialized gives its slot back.
    /// </summary>
    public async ValueTask<OutgoingBatch?> TryReadAsync(CancellationToken ct)
    {
        if (!Accumulator.BatchReader.TryRead(out var next)) return null;

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
            _releaseSlot();
            throw;
        }
        return new OutgoingBatch(body, batchId, items.Count);
    }

    public ValueTask CompleteAsync(CancellationToken ct) => Accumulator.CompleteAsync(ct);

    public void Abandon() => _accumulator?.Abandon();

    public void Dispose() => _accumulator?.Dispose();

    public void Detach() => _accumulator = null;

    public async Task SendAsync(T item, CancellationToken token)
    {
        if (_accumulator is { } accumulator)
            await accumulator.AddAsync(item, token);
    }

    public async Task SendRangeAsync(IEnumerable<T> items, CancellationToken token)
    {
        if (_accumulator is { } accumulator)
            await accumulator.AddRangeAsync(items, token);
    }
}
