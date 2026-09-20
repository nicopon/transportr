using System.Buffers;
using TransportR.Protocol;

namespace TransportR.Client.SignalR.Batching;

/// <summary>
/// Manages receiver-side batch reordering and deserialization.
/// Handles out-of-order batch arrival and ensures in-order delivery to the consumer channel.
/// </summary>
internal class ReceiverState<T>
{
    /// <summary>
    /// Result of processing a batch, indicating what ACK to send.
    /// </summary>
    public readonly record struct ReceiveResult
    {
        public bool IsProcessed { get; init; }
        public int? BatchIdToAck { get; init; }
    }

    private readonly ReceiveBuffer<T> _buffer;
    private readonly Dictionary<int, (IMemoryOwner<byte> Owner, ReadOnlyMemory<byte> Body)> _reorderBuffer;
    private readonly BatchProtocol<T> _protocol;

    public int NextExpectedBatchId { get; private set; } = 0;

    /// <summary>
    /// Chunks currently held out of order, waiting for the one before them. Counted rather than
    /// bounded: these hold serialized bytes outside the receive buffer's budget, and over a single
    /// ordered stream the count should stay at zero. A bound built before a measurement showed
    /// otherwise would be a second budget guarding nothing.
    /// </summary>
    public int ReorderedChunks => _reorderBuffer.Count;

    public ReceiverState(ReceiveBuffer<T> buffer, BatchProtocol<T> protocol, int startBatchId = 0)
    {
        _buffer = buffer;
        _reorderBuffer = [];
        _protocol = protocol;
        NextExpectedBatchId = startBatchId;
    }
    
    /// <summary>
    /// Processes an incoming batch, handling reordering and deserialization.
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
            try
            {
                await DeserializeAndWrite(compressedBody, ct);
            }
            finally
            {
                owner.Dispose();
            }
            
            NextExpectedBatchId++;

            while (_reorderBuffer.ContainsKey(NextExpectedBatchId))
            {
                var (bufOwner, bufBody) = _reorderBuffer[NextExpectedBatchId];
                _reorderBuffer.Remove(NextExpectedBatchId);
                try
                {
                    await DeserializeAndWrite(bufBody, ct);
                }
                finally
                {
                    bufOwner.Dispose();
                }
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
    /// Hands a chunk to the receive buffer, which charges it against the configured capacity
    /// before it is decoded and parks here for as long as the consumer is behind.
    /// </summary>
    /// <remarks>
    /// The chunk's payload length is what gets charged — the wire size, which is the only size
    /// available without knowing what <typeparamref name="T"/> costs on the heap. The sender
    /// strips the item-count prefix before writing, so this is already the serialized body alone.
    /// </remarks>
    private Task DeserializeAndWrite(ReadOnlyMemory<byte> compressedBody, CancellationToken ct)
    {
        return _buffer.WriteChunkAsync(
            compressedBody.Length,
            () => _protocol.ReadBatchBodyFromMemoryAsync(compressedBody, ct),
            ct);
    }

    /// <summary>
    /// Signals that no more batches will be received.
    /// </summary>
    public void Complete()
    {
        _buffer.Complete();
    }
}
