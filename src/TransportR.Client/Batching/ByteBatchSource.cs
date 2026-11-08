using Microsoft.IO;
using TransportR.Protocol;

namespace TransportR.Client.Batching;

/// <summary>
/// The bytes of a transfer, cut into batches by a <see cref="ByteStreamAccumulator"/> and handed to
/// the send worker as they were cut: each is a body already, its length in front.
/// </summary>
internal sealed class ByteBatchSource(
    int batchBytes,
    int flushIntervalMs,
    RecyclableMemoryStreamManager streams) : IBatchSource
{
    private const int LengthPrefixBytes = sizeof(int);

    private ByteStreamAccumulator? _accumulator;

    public ChunkType BatchType => ChunkType.StreamBatch;

    private ByteStreamAccumulator Accumulator =>
        _accumulator ?? throw new InvalidOperationException("The source is not open.");

    public void Open(SemaphoreSlim? inFlightSlots, Action releaseSlot) =>
        _accumulator = new ByteStreamAccumulator(batchBytes, flushIntervalMs, streams, inFlightSlots, releaseSlot);

    public void StartFlushInterval() => Accumulator.StartFlushInterval();

    public ValueTask<bool> WaitToReadAsync(CancellationToken ct) =>
        Accumulator.BatchReader.WaitToReadAsync(ct);

    public ValueTask<OutgoingBatch?> TryReadAsync(CancellationToken ct) =>
        ValueTask.FromResult<OutgoingBatch?>(Accumulator.BatchReader.TryRead(out var next)
            ? new OutgoingBatch(next.Batch, next.BatchId, (int)next.Batch.Length - LengthPrefixBytes)
            : null);

    public ValueTask CompleteAsync(CancellationToken ct) => Accumulator.CompleteAsync(ct);

    public void Abandon() => _accumulator?.Abandon();

    /// <summary>
    /// Disposes the accumulator, which disposes the batches nobody read and gives back their slots.
    /// </summary>
    public void Dispose() => _accumulator?.Dispose();

    public void Detach() => _accumulator = null;

    public async Task SendBytesAsync(ReadOnlyMemory<byte> bytes, CancellationToken token)
    {
        if (_accumulator is { } accumulator)
            await accumulator.AddAsync(bytes, token);
    }
}
