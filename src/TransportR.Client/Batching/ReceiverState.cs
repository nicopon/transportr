using System.Buffers;
using TransportR.Client.Protocol;

namespace TransportR.Client.Batching;

/// <summary>
/// Hands the batches of a transfer to the consumer's sink in order, once each.
/// </summary>
/// <remarks>
/// A stream carries them in order, a resend included: the sender resends from the batch the receiver
/// expects next, so a batch already in is a duplicate and one ahead of the expected one is a
/// protocol violation, refused rather than held.
/// </remarks>
internal class ReceiverState
{
    /// <summary>
    /// Result of processing a batch, indicating what ACK to send.
    /// </summary>
    public readonly record struct ReceiveResult
    {
        public bool IsProcessed { get; init; }
        public int? BatchIdToAck { get; init; }
    }

    private readonly IChunkSink _sink;

    public int NextExpectedBatchId { get; private set; } = 0;

    public ReceiverState(IChunkSink sink, int startBatchId = 0)
    {
        _sink = sink;
        NextExpectedBatchId = startBatchId;
    }

    /// <summary>
    /// Processes an incoming batch.
    /// </summary>
    /// <param name="batchId">The batch identifier.</param>
    /// <param name="owner">Memory owner for the batch payload (will be disposed after processing).</param>
    /// <param name="compressedBody">The compressed batch payload.</param>
    /// <returns>Result indicating what ACK to send.</returns>
    /// <exception cref="WireProtocolViolationException">
    /// The batch is ahead of <see cref="NextExpectedBatchId"/>. Its memory has been returned.
    /// </exception>
    public async Task<ReceiveResult> ProcessBatchAsync(int batchId, IMemoryOwner<byte> owner, ReadOnlyMemory<byte> compressedBody, CancellationToken ct = default)
    {
        if (batchId < NextExpectedBatchId)
        {
            owner.Dispose();
            return new ReceiveResult { IsProcessed = true, BatchIdToAck = batchId };
        }

        if (batchId == NextExpectedBatchId)
        {
            await WriteAsync(owner, compressedBody, ct);

            NextExpectedBatchId++;

            return new ReceiveResult { IsProcessed = true, BatchIdToAck = batchId };
        }

        owner.Dispose();
        throw new WireProtocolViolationException(
            $"Batch {batchId} arrived while batch {NextExpectedBatchId} was expected.");
    }

    /// <summary>
    /// Writes a chunk to the sink, and returns its memory unless the sink took it.
    /// </summary>
    private async Task WriteAsync(IMemoryOwner<byte> owner, ReadOnlyMemory<byte> body, CancellationToken ct)
    {
        var taken = false;
        try
        {
            taken = await _sink.WriteAsync(owner, body, ct);
        }
        finally
        {
            if (!taken) owner.Dispose();
        }
    }

    /// <summary>
    /// Signals that no more batches will be received.
    /// </summary>
    public void Complete()
    {
        _sink.Complete();
    }
}
