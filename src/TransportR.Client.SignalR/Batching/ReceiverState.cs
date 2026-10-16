using System.Buffers;
using System.Runtime.InteropServices;
using TransportR.Interfaces;
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
    private readonly IBatchSerializer<T> _serializer;

    public int NextExpectedBatchId { get; private set; } = 0;

    /// <summary>
    /// Chunks currently held out of order, waiting for the one before them. Counted rather than
    /// bounded: these hold serialized bytes outside the receive buffer's budget, and over a single
    /// ordered stream the count should stay at zero. A bound built before a measurement showed
    /// otherwise would be a second budget guarding nothing.
    /// </summary>
    public int ReorderedChunks => _reorderBuffer.Count;

    public ReceiverState(ReceiveBuffer<T> buffer, IBatchSerializer<T> serializer, int startBatchId = 0)
    {
        _buffer = buffer;
        _reorderBuffer = [];
        _serializer = serializer;
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
    /// <remarks>
    /// The token cancels the wait for capacity and the hand-off, not the decoding between them:
    /// nothing under the serializer interface takes one.
    /// </remarks>
    private Task DeserializeAndWrite(ReadOnlyMemory<byte> compressedBody, CancellationToken ct)
    {
        return _buffer.WriteChunkAsync(
            compressedBody.Length,
            () => ReadBatchBodyFromMemoryAsync(_serializer, compressedBody),
            ct);
    }

    /// <summary>
    /// Decodes a batch body straight out of the memory it was read into.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Internal rather than private because the serializer tests enter through here: how the body
    /// reaches the serializer is part of what they check — the stream wraps the received memory
    /// instead of copying it, which is what lets a reader allocate through a pool.
    /// </para>
    /// <para>
    /// Takes no cancellation token, because there is nowhere to honour one:
    /// <see cref="IBatchSerializer{T}.DeserializeBatchAsync"/> does not accept one, and the source
    /// is memory already read off the wire. A token accepted here and passed nowhere would tell a
    /// caller that decoding is interruptible when it is not.
    /// </para>
    /// </remarks>
    internal static async Task<List<T>?> ReadBatchBodyFromMemoryAsync(
        IBatchSerializer<T> serializer, ReadOnlyMemory<byte> bodyBytes)
    {
        // An array-backed body is wrapped rather than copied; only a body that is not can force
        // the copy in the other branch, which does not happen on the read path.
        Stream source = MemoryMarshal.TryGetArray(bodyBytes, out var segment)
            ? new MemoryStream(segment.Array!, segment.Offset, segment.Count, writable: false)
            : new MemoryStream(bodyBytes.ToArray());

        using (source)
        {
            try
            {
                return await serializer.DeserializeBatchAsync(source);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // The body arrived whole, framed by its header, so a resend after a reconnection
                // decodes no better: left retryable, this reconnects forever on the same batch.
                throw new UndecodableBatchException(
                    $"A batch body could not be decoded by {serializer.GetType().Name}: {ex.Message}", ex);
            }
        }
    }

    /// <summary>
    /// Drops the chunks held out of order, returning their pooled memory.
    /// </summary>
    /// <remarks>
    /// Called when a reconnection replaces this state. Dropping them is correct — the sender
    /// resends everything from <see cref="NextExpectedBatchId"/>, which is below every id held
    /// here — but the buffers they sit in come from a pool and were previously abandoned with
    /// the instance, one leak per reconnection that had chunks in hand.
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
        _buffer.Complete();
    }
}
