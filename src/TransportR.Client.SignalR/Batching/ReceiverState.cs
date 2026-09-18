using System.Buffers;
using System.Threading.Channels;
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

    private readonly ChannelWriter<T> _writer;
    private readonly Dictionary<int, (IMemoryOwner<byte> Owner, ReadOnlyMemory<byte> Body, int Count)> _buffer;
    private readonly BatchProtocol<T> _protocol;

    public int NextExpectedBatchId { get; private set; } = 0;

    public ReceiverState(ChannelWriter<T> writer, BatchProtocol<T> protocol, int startBatchId = 0)
    {
        _writer = writer;
        _buffer = [];
        _protocol = protocol;
        NextExpectedBatchId = startBatchId;
    }
    
    /// <summary>
    /// Processes an incoming batch, handling reordering and deserialization.
    /// </summary>
    /// <param name="batchId">The batch identifier.</param>
    /// <param name="count">Number of items in the batch.</param>
    /// <param name="owner">Memory owner for the batch payload (will be disposed after processing).</param>
    /// <param name="compressedBody">The compressed batch payload.</param>
    /// <returns>Result indicating what ACK to send.</returns>
    public async Task<ReceiveResult> ProcessBatchAsync(int batchId, int count, IMemoryOwner<byte> owner, ReadOnlyMemory<byte> compressedBody)
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
                await DeserializeAndWrite(compressedBody, count);
            }
            finally
            {
                owner.Dispose();
            }
            
            NextExpectedBatchId++;

            while (_buffer.ContainsKey(NextExpectedBatchId))
            {
                var (bufOwner, bufBody, bufCount) = _buffer[NextExpectedBatchId];
                _buffer.Remove(NextExpectedBatchId);
                try
                {
                    await DeserializeAndWrite(bufBody, bufCount);
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
            _buffer[batchId] = (owner, compressedBody, count);
            return new ReceiveResult { IsProcessed = true, BatchIdToAck = batchId };
        }
    }

    private async Task DeserializeAndWrite(ReadOnlyMemory<byte> compressedBody, int count)
    {
         var items = await _protocol.ReadBatchBodyFromMemoryAsync(compressedBody, CancellationToken.None);

        if (items != null)
        {
            foreach (var item in items)
            {
                await _writer.WriteAsync(item);
            }
        }
    }

    /// <summary>
    /// Signals that no more batches will be received.
    /// </summary>
    public void Complete()
    {
        _writer.TryComplete();
    }
}
