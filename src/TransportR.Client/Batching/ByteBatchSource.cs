namespace TransportR.Client.Batching;

/// <summary>
/// Hands the send worker the batches of a <see cref="ByteStreamAccumulator"/> as they were cut: each
/// is a body already, its length in front.
/// </summary>
internal sealed class ByteBatchSource(ByteStreamAccumulator accumulator) : IBatchSource
{
    private const int LengthPrefixBytes = sizeof(int);

    public void StartFlushInterval() => accumulator.StartFlushInterval();

    public ValueTask<bool> WaitToReadAsync(CancellationToken ct) =>
        accumulator.BatchReader.WaitToReadAsync(ct);

    public ValueTask<OutgoingBatch?> TryReadAsync(CancellationToken ct) =>
        ValueTask.FromResult<OutgoingBatch?>(accumulator.BatchReader.TryRead(out var next)
            ? new OutgoingBatch(next.Batch, next.BatchId, (int)next.Batch.Length - LengthPrefixBytes)
            : null);

    public ValueTask CompleteAsync(CancellationToken ct) => accumulator.CompleteAsync(ct);

    public void Abandon() => accumulator.Abandon();

    /// <summary>
    /// Disposes the accumulator, which disposes the batches nobody read and gives back their slots.
    /// </summary>
    public void Dispose() => accumulator.Dispose();
}
