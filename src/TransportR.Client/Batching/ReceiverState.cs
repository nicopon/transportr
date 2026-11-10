using System.Buffers;

namespace TransportR.Client.Batching;

/// <summary>
/// Manages receiver-side batch reordering.
/// Handles out-of-order batch arrival and ensures in-order delivery to the consumer channel.
/// </summary>
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
    private readonly Dictionary<int, (IMemoryOwner<byte> Owner, ReadOnlyMemory<byte> Body)> _reorderBuffer;

    public int NextExpectedBatchId { get; private set; } = 0;

    /// <summary>
    /// Chunks currently held out of order, waiting for the one before them. Counted rather than
    /// bounded: these hold serialized bytes outside the receive buffer's budget, and over a single
    /// ordered stream the count should stay at zero. A bound built before a measurement showed
    /// otherwise would be a second budget guarding nothing.
    /// </summary>
    public int ReorderedChunks => _reorderBuffer.Count;

    public ReceiverState(IChunkSink sink, int startBatchId = 0)
    {
        _sink = sink;
        _reorderBuffer = [];
        NextExpectedBatchId = startBatchId;
    }

    /// <summary>
    /// Processes an incoming batch, handling reordering.
    /// </summary>
    /// <param name="batchId">The batch identifier.</param>
    /// <param name="owner">Memory owner for the batch payload (will be disposed after processing).</param>
    /// <param name="compressedBody">The compressed batch payload.</param>
    /// <returns>Result indicating what ACK to send.</returns>
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

            while (_reorderBuffer.ContainsKey(NextExpectedBatchId))
            {
                var (bufOwner, bufBody) = _reorderBuffer[NextExpectedBatchId];
                _reorderBuffer.Remove(NextExpectedBatchId);
                await WriteAsync(bufOwner, bufBody, ct);
                NextExpectedBatchId++;
            }

            return new ReceiveResult { IsProcessed = true, BatchIdToAck = batchId };
        }
        else
        {
            // Out-of-order batch: buffer it and ACK (TCP guarantees eventual delivery)
            _reorderBuffer[batchId] = (owner, compressedBody);
            return new ReceiveResult { IsProcessed = true, BatchIdToAck = batchId };
        }
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
    /// Drops the chunks held out of order, returning their pooled memory.
    /// </summary>
    /// <remarks>
    /// Called when a reconnection replaces this state. Dropping them is correct — the sender
    /// resends everything from <see cref="NextExpectedBatchId"/>, which is below every id held
    /// here — but the buffers they sit in come from a pool, and abandoning them with the
    /// instance would leak one set per reconnection that had chunks in hand.
    /// </remarks>
    public void DiscardReordered()
    {
        foreach (var (owner, _) in _reorderBuffer.Values) owner.Dispose();
        _reorderBuffer.Clear();
    }

    /// <summary>
    /// Signals that no more batches will be received.
    /// </summary>
    public void Complete()
    {
        _sink.Complete();
    }
}
