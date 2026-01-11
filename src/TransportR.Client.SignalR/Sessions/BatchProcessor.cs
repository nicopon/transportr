using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using TransportR.Client.SignalR.Batching;
using TransportR.Client.SignalR.Internal;
using TransportR.Protocol;

namespace TransportR.Client.SignalR.Sessions;

/// <summary>
/// Handles the business logic of batching, serialization, and protocol messages.
/// Manages sender/receiver state, unacked batches, and flow control.
/// </summary>
internal class BatchProcessor<T> : IAsyncDisposable
{
    private readonly ISessionContext _context;
    private readonly string _transferId;
    private readonly int _batchSize;
    private readonly string _dataUrl;
    private readonly BatchProtocol _protocol;
    
    // Channels
    private readonly Channel<IControlMessage> _sendChannel;
    private Channel<T>? _receiveChannel;
    
    // Sender State
    private BatchAccumulator<T>? _batchAccumulator;
    // --- Internal Logic ---
    
    /// <summary>
    /// Thread-safe wrapper for unacked batch payload.
    /// Uses reference counting to prevent disposal while being resent.
    /// </summary>
    // --- Internal Logic ---

    private readonly ConcurrentDictionary<int, BatchEntry> _unackedBatches = new();

    private Task? _serializationWorker;
    private int _pendingBatches = 0;
    private int _lastAckedBatchId = -1;
    
    // Receiver State
    private ReceiverState<T>? _receiverState;
    
    // Synchronization
    private TaskCompletionSource? _completeAckReceived;
    
    // Stats
    private long _itemsSent;
    private long _itemsReceived;
    private long _acksReceived;
    private long _acksSent;

    public ChannelReader<IControlMessage> OutgoingMessages => _sendChannel.Reader;
    public long ItemsSent => Interlocked.Read(ref _itemsSent);
    public long ItemsReceived => Interlocked.Read(ref _itemsReceived);
    public long AcksReceived => Interlocked.Read(ref _acksReceived);
    public long AcksSent => Interlocked.Read(ref _acksSent);
    public int PendingBatches => _pendingBatches;
    public bool HasUnackedBatches => !_unackedBatches.IsEmpty;
    public int UnackedCount => _unackedBatches.Count;
    public int NextExpectedBatchId => _receiverState?.NextExpectedBatchId ?? 0;

    public BatchProcessor(ISessionContext context, string transferId, int batchSize, string dataUrl)
    {
        _context = context;
        _transferId = transferId;
        _batchSize = batchSize;
        _dataUrl = dataUrl;
        _protocol = new BatchProtocol(context.Serializer);
        _sendChannel = Channel.CreateUnbounded<IControlMessage>();
    }

    public Task StartSenderAsync(CancellationToken token)
    {
        _unackedBatches.Clear();
        _batchAccumulator = new BatchAccumulator<T>(_batchSize, _context.BatchFlushIntervalMs);        _batchAccumulator.StartFlushInterval();
        
        _serializationWorker = Task.Run(() => SerializationWorkerAsync(token));
        
        return Task.CompletedTask;
    }

    public Task StartReceiverAsync()
    {
        _receiveChannel ??= Channel.CreateBounded<T>(10000);
        int startBatchId = _receiverState?.NextExpectedBatchId ?? 0;
        _receiverState = new ReceiverState<T>(_receiveChannel.Writer, _protocol, startBatchId);
        return Task.CompletedTask;
    }

    public async Task SendAsync(T item, CancellationToken token)
    {
        if (_batchAccumulator != null)
            await _batchAccumulator.AddAsync(item, token);
    }

    public async Task SendRangeAsync(IEnumerable<T> items, CancellationToken token)
    {
        if (_batchAccumulator != null)
            await _batchAccumulator.AddRangeAsync(items, token);
    }

    public IAsyncEnumerable<T> ReceiveAsync(CancellationToken token)
    {
        if (_receiveChannel == null) throw new InvalidOperationException("Receiver not started");
        return _receiveChannel.Reader.ReadAllAsync(token);
    }

    public async Task CompleteSenderAsync(CancellationToken token)
    {
        if (_batchAccumulator != null)
        {
            await _batchAccumulator.CompleteAsync(token);
            if (_serializationWorker != null)
            {
                await _serializationWorker;
                _serializationWorker = null;
            }
            _batchAccumulator.Dispose();
            _batchAccumulator = null;
        }
    }
    
    public void CompleteReceiver()
    {
        _receiveChannel?.Writer.TryComplete();
        _sendChannel.Writer.TryComplete(); // Allow Control WriteLoop to exit
    }
    
    public void CompleteSenderChannel()
    {
        _sendChannel.Writer.TryComplete();
    }
    
    public Task WaitForCompleteAckAsync(CancellationToken token)
    {
        return _completeAckReceived?.Task.WaitAsync(token) ?? Task.CompletedTask;
    }
    
    public void ResetCompleteAck()
    {
        _completeAckReceived = new TaskCompletionSource();
    }

    public async Task ProcessChunkAsync(ChunkData chunk)
    {
        switch (chunk.Header.Type)
        {
            case ChunkType.Batch:
                await HandleBatchChunkAsync(chunk);
                break;
            case ChunkType.BatchAck:
                HandleBatchAck(chunk.Header.BatchId);
                break;
            case ChunkType.Ping:
                await HandlePingAsync();
                break;
            case ChunkType.Pong:
                HandlePong(chunk.Header.BatchId);
                break;
            case ChunkType.Complete:
                await HandleCompleteAsync(chunk);
                break;
            case ChunkType.CompleteAck:
                HandleCompleteAck();
                break;
        }
    }
    private async Task SerializationWorkerAsync(CancellationToken ct)
    {
        await foreach (var (items, batchId) in _batchAccumulator!.BatchReader.ReadAllAsync(ct))
        {
            try
            {
                await SerializeAndSendBatch(items, batchId, ct);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                _context.Logger.LogWarning("Serialization worker error for batch {BatchId}: {Message}", batchId, ex.Message);
            }
        }
    }
    
    private int _maxBatchIdSent = -1;

    private async Task SerializeAndSendBatch(List<T> items, int batchId, CancellationToken ct)
    {
        using var memoryStream = _context.StreamManager.GetStream();
        var countBytes = BitConverter.GetBytes(items.Count);
        await memoryStream.WriteAsync(countBytes.AsMemory(), ct);
        await _protocol.SerializeBatchAsync(memoryStream, items);
        
        var totalLen = (int)memoryStream.Length;
        var owner = new PooledMemoryOwner(memoryStream.GetBuffer().AsMemory(0, totalLen));
        
        // Wrap in BatchEntry
        var entry = new BatchEntry(owner, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        
        // 1 reference for _unackedBatches (initial ref = 1)
        _unackedBatches[batchId] = entry;

        // 1 reference for BatchControlMessage (we must retain it)
        var entryForMessage = entry.TryRetain();
        // entryForMessage cannot be null here since it was just created
        
        Interlocked.Increment(ref _pendingBatches);
        Interlocked.Add(ref _itemsSent, items.Count);
        
        lock (this)
        {
            if (batchId > _maxBatchIdSent) _maxBatchIdSent = batchId;
        }
        
        // entryForMessage IS the Internal.BatchEntry (TryRetain returns IMemoryOwner? NO. TryRetain returns IMemoryOwner<byte>?)
        // Wait, Internal.BatchEntry.TryRetain returns IMemoryOwner<byte>.
        // BUT BatchControlMessage expects BatchEntry.
        
        // My extracted class:
        // public IMemoryOwner<byte>? TryRetain()
        
        // Ah! If I return IMemoryOwner, I lose the BatchEntry wrapper reference management?
        // No. The methods are ON BatchEntry.
        
        // BatchControlMessage takes `BatchEntry Entry`.
        // So I need to pass `entry`.
        // BUT I need to increment ref count manually?
        // BatchEntry.TryRetain returns the Payload (IMemoryOwner). It increments the ref count.
        // It DOES NOT return the BatchEntry itself.
        
        // I need a method `RetainEntry()`? Or just call `TryRetain()` and ignore return (since return is payload)?
        // Yes. `TryRetain()` increments `_refCount`.
        // So `entry.TryRetain()` increments to 2.
        
        await _sendChannel.Writer.WriteAsync(new BatchControlMessage(batchId, entry), ct);
    }
    
    private async Task HandleBatchChunkAsync(ChunkData chunk)
    {
        if (_receiverState == null) return;
        
        var owner = new PooledMemoryOwner(chunk.Payload);
        var rxResult = await _receiverState.ProcessBatchAsync(chunk.Header.BatchId, chunk.Header.Count, owner, owner.Memory);
        
        Interlocked.Add(ref _itemsReceived, chunk.Header.Count);

        if (rxResult.BatchIdToAck.HasValue)
        {
            await _sendChannel.Writer.WriteAsync(new ControlChunkMessage(ChunkType.BatchAck, rxResult.BatchIdToAck.Value));
            Interlocked.Increment(ref _acksSent);
            UpdateLastAckedBatchId(rxResult.BatchIdToAck.Value);
        }
    }

    private void HandleBatchAck(int batchId)
    {
        if (_unackedBatches.TryRemove(batchId, out var entry)) 
        {
             // Release the reference held by the Dictionary
             entry.Release(); 
             Interlocked.Decrement(ref _pendingBatches);
        }
        Interlocked.Increment(ref _acksReceived);
    }
    
    private async Task HandlePingAsync()
    {
        int nextExpected = _receiverState?.NextExpectedBatchId ?? 0;
        await _sendChannel.Writer.WriteAsync(new ControlChunkMessage(ChunkType.Pong, nextExpected));
        _context.Logger.LogDebug("Received Ping, sent Pong(nextExpected={NextExpected}) for Transfer={TransferId}", nextExpected, _transferId);
    }
    
    // Event callback for Pong? Or just expose state?
    public event Action<int>? OnPongReceived;
    public event Action? OnCompleteAckReceived;
    public event Func<Task>? OnCompleteReceived;

    private void HandlePong(int nextExpectedBatchId)
    {
        OnPongReceived?.Invoke(nextExpectedBatchId);
    }
    
    public async Task SendCompleteSignalAsync(CancellationToken token)
    {
        _completeAckReceived = new TaskCompletionSource();
        // Send the Last Batch ID so receiver can verify if it missed anything
        await _sendChannel.Writer.WriteAsync(new ControlChunkMessage(ChunkType.Complete, _maxBatchIdSent));
    }
    
    private async Task HandleCompleteAsync(ChunkData chunk)
    {
        int lastBatchIdFromSender = chunk.Header.BatchId;
        int nextExpected = _receiverState?.NextExpectedBatchId ?? 0;
        
        // Validation: Have we received all batches up to lastBatchId?
        // If lastBatchIdFromSender is -1, it means 0 batches sent. NextExpected should be 0.
        // If lastBatchIdFromSender is 10. We expect to have processed 0..10. So NextExpected should be 11.
        // So condition is: NextExpected <= lastBatchIdFromSender.
        
        if (nextExpected <= lastBatchIdFromSender)
        {
            _context.Logger.LogWarning("Ignored Premature Complete. Missing batches. NextExpected={Next}, LastSent={Last} (Transfer={TransferId})", 
                nextExpected, lastBatchIdFromSender, _transferId);
            return; // Ignore complete signal. Sender will retry.
        }

        _context.Logger.LogInformation("Received Valid Complete (LastBatch={Last}). NextExpected={Next}. Sending CompleteAck (Transfer={TransferId})", 
            lastBatchIdFromSender, nextExpected, _transferId);
            
        await _sendChannel.Writer.WriteAsync(new ControlChunkMessage(ChunkType.CompleteAck, 0));
        if (OnCompleteReceived != null) await OnCompleteReceived.Invoke();
    }
    
    private void HandleCompleteAck()
    {
        _completeAckReceived?.TrySetResult();
        OnCompleteAckReceived?.Invoke();
    }
    
    public async Task ResendCompleteAckAsync()
    {
        _context.Logger.LogInformation("Resending CompleteAck for Transfer={TransferId}", _transferId);
        await _sendChannel.Writer.WriteAsync(new ControlChunkMessage(ChunkType.CompleteAck, 0));
    }
    
    private void UpdateLastAckedBatchId(int ackId)
    {
        int current;
        do {
            current = _lastAckedBatchId;
            if (ackId <= current) break;
        } while (Interlocked.CompareExchange(ref _lastAckedBatchId, ackId, current) != current);
    }


    /// <summary>
    /// Resends unacked batches directly to the stream (bypass channel).
    /// </summary>
    public async Task ResendUnackedBatchesAsync(Stream outputStream, Guid transferGuid, bool didPingPong, int nextExpectedFromPong, CancellationToken token)
    {
        foreach (var kvp in _unackedBatches.OrderBy(x => x.Key))
        {
            if (token.IsCancellationRequested) return;
            
            // Skip batches the receiver already has
            if (didPingPong && kvp.Key < nextExpectedFromPong)
            {
                if (_unackedBatches.TryRemove(kvp.Key, out var removedEntry))
                {
                    removedEntry.Release();
                    // Implicit ACK: Update stats and pending/unacked counters
                    Interlocked.Decrement(ref _pendingBatches);
                    Interlocked.Increment(ref _acksReceived);
                    
                    _context.Logger.LogDebug("Skipped resend of batch {BatchId} (receiver has it) for Transfer={TransferId}", kvp.Key, _transferId);
                }
                continue;
            }
            
            // Try retain payload for usage
            var memoryOwner = kvp.Value.TryRetain();
            if (memoryOwner == null) 
            {
                 // Reference count was 0 or disposed, means it was Acked/Removed concurrently
                 _context.Logger.LogDebug("Race condition avoided: Batch {BatchId} was disposed/acked concurrently during resend (Transfer={TransferId})", kvp.Key, _transferId);
                 continue;
            }

            try
            {
                var payloadMem = memoryOwner.Memory;
                var count = BinaryPrimitives.ReadInt32LittleEndian(payloadMem.Span[..4]);
                var body = payloadMem[4..];
                
                await ChunkIO.WriteBatchAsync(outputStream, transferGuid, kvp.Key, count, body, token);
                await outputStream.FlushAsync(token);
                _context.Logger.LogInformation("Resent unacked batch {BatchId} on reconnect (Transfer={TransferId})", kvp.Key, _transferId);
            }
            catch (Exception ex)
            {
                _context.Logger.LogWarning("Failed to resend unacked batch {BatchId}: {Message}", kvp.Key, ex.Message);
                throw;
            }
            finally
            {
                kvp.Value.Release();
            }
        }
    }
    


    public ValueTask DisposeAsync()
    {
        _sendChannel.Writer.TryComplete();
        _serializationWorker = null; // Should look into canceling it if running?
        _batchAccumulator?.Dispose();
        
        foreach(var kvp in _unackedBatches)
        {
            // Force release/dispose
            kvp.Value.Release(); 
        }
        _unackedBatches.Clear();
        return ValueTask.CompletedTask;
    }
}
