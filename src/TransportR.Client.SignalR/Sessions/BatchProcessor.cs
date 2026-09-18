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
    private readonly ISessionContext<T> _context;
    private readonly string _transferId;
    private readonly int _batchSize;
    private readonly string _dataUrl;
    private readonly BatchProtocol<T> _protocol;
    
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

    // In-flight window (ClientOptions.MaxInFlightBatches). Slots are taken by the accumulator when
    // a batch is handed off and released here when it leaves _unackedBatches. Null = unbounded.
    private SemaphoreSlim? _inFlightSlots;

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

    public BatchProcessor(ISessionContext<T> context, string transferId, int batchSize, string dataUrl)
    {
        _context = context;
        _transferId = transferId;
        _batchSize = batchSize;
        _dataUrl = dataUrl;
        _protocol = new BatchProtocol<T>(context.Serializer);
        _sendChannel = Channel.CreateUnbounded<IControlMessage>();
    }

    public Task StartSenderAsync(CancellationToken token)
    {
        _unackedBatches.Clear();

        // maxCount is set as well as initialCount: an over-release means the slot accounting is
        // wrong somewhere, and we would rather find out than silently widen the window.
        _inFlightSlots = _context.Options.MaxInFlightBatches is { } max
            ? new SemaphoreSlim(max, max)
            : null;

        _batchAccumulator = new BatchAccumulator<T>(_batchSize, _context.BatchFlushIntervalMs, _inFlightSlots);
        _batchAccumulator.StartFlushInterval();
        
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
        // The accumulator took a slot for this batch. From the moment it lands in _unackedBatches,
        // the ack path owns giving it back; before that, this method does.
        var registered = false;
        try
        {
            await SerializeAndSendBatchCore(items, batchId, () => registered = true, ct);
        }
        catch
        {
            if (!registered) ReleaseInFlightSlot();
            throw;
        }
    }

    private async Task SerializeAndSendBatchCore(List<T> items, int batchId, Action onRegistered, CancellationToken ct)
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
        onRegistered();

        // 1 reference for BatchControlMessage. TryRetain increments the entry's ref count and
        // hands back the payload; the message carries the entry itself, so the return is unused.
        // Cannot be null here - the entry was created on the line above.
        entry.TryRetain();

        Interlocked.Increment(ref _pendingBatches);
        Interlocked.Add(ref _itemsSent, items.Count);
        
        lock (this)
        {
            if (batchId > _maxBatchIdSent) _maxBatchIdSent = batchId;
        }
        
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
             ReleaseInFlightSlot();
        }
        Interlocked.Increment(ref _acksReceived);
    }

    /// <summary>
    /// Gives an in-flight slot back to the producer. Called from every path that removes a batch
    /// from <see cref="_unackedBatches"/>, and from the serialization path when a batch fails
    /// before it ever gets there. Missing one of those closes the window permanently.
    /// </summary>
    /// <remarks>
    /// This runs on the ACK-reading loop (the Control stream read task), which is distinct from
    /// both the serialization worker and the data write loop. It only ever releases, never waits,
    /// which is what keeps the window free of deadlock.
    /// </remarks>
    private void ReleaseInFlightSlot()
    {
        if (_inFlightSlots == null) return;

        try
        {
            _inFlightSlots.Release();
        }
        catch (SemaphoreFullException)
        {
            // Accounting bug: a batch was released twice. Logged rather than thrown, because this
            // runs on the loop that processes acknowledgements and killing it would stall the
            // transfer outright.
            _context.Logger.LogError(
                "In-flight slot over-released for Transfer={TransferId}. The send window is now wider than configured.",
                _transferId);
        }
        catch (ObjectDisposedException)
        {
            // Session torn down while an ack was in flight; nothing left to bound.
        }
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
                    // Same as an explicit ack: the batch is done, give the slot back. Without this
                    // the window shrinks by one on every resumed batch and never reopens.
                    ReleaseInFlightSlot();
                    
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

        // Dispose the accumulator first: it cancels its own token, which releases a producer
        // parked on the in-flight window before that window is disposed underneath it.
        _batchAccumulator?.Dispose();

        // Not disposed on purpose: a producer may still be unwinding out of a cancelled wait on
        // it, and SemaphoreSlim only needs disposing once AvailableWaitHandle has been used.
        _inFlightSlots = null;
        
        foreach(var kvp in _unackedBatches)
        {
            // Force release/dispose
            kvp.Value.Release(); 
        }
        _unackedBatches.Clear();
        return ValueTask.CompletedTask;
    }
}
