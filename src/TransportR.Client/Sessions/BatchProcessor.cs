using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.IO;
using TransportR.Abstractions;
using TransportR.Client.Batching;
using TransportR.Client.Internal;
using TransportR.Interfaces;
using TransportR.Protocol;

namespace TransportR.Client.Sessions;

/// <summary>
/// Handles the business logic of batching, serialization, and protocol messages.
/// Manages sender/receiver state, unacked batches, and flow control.
/// </summary>
internal class BatchProcessor<T> : IAsyncDisposable
{
    private readonly ISessionContext<T> _context;
    private readonly string _transferId;
    private readonly int _batchSize;
    
    // Channels
    private readonly Channel<IControlMessage> _sendChannel;
    private ReceiveBuffer<T>? _receiveBuffer;
    private ByteChunkSink? _byteSink;
    private IChunkSink? _sink;

    // Null in the byte-stream mode, the only one without a serializer.
    private readonly IBatchSerializer<T>? _serializer;
    private readonly bool _byteStream;
    
    // Sender State: one accumulator, the one of the mode.
    private BatchAccumulator<T>? _batchAccumulator;
    private ByteStreamAccumulator? _byteAccumulator;

    private readonly ConcurrentDictionary<int, BatchEntry> _unackedBatches = new();

    private Task? _serializationWorker;

    // In-flight window (ClientOptions.MaxInFlightBatches). Slots are taken by the accumulator - when
    // a batch is handed off, or in the byte-stream mode when it is opened - and released here when
    // the batch leaves _unackedBatches. Null = unbounded.
    private SemaphoreSlim? _inFlightSlots;

    private int _pendingBatches = 0;
    
    // Receiver State
    private ReceiverState<T>? _receiverState;
    
    // Synchronization
    private TaskCompletionSource? _completeAckReceived;
    
    // Stats
    private long _itemsSent;
    private long _itemsReceived;
    private long _acksReceived;
    private long _lastAckAtMs = Environment.TickCount64;
    private long _acksSent;
    private long _payloadBytes;

    public ChannelReader<IControlMessage> OutgoingMessages => _sendChannel.Reader;
    public long ItemsSent => Interlocked.Read(ref _itemsSent);
    public long ItemsReceived => Interlocked.Read(ref _itemsReceived);
    public long AcksReceived => Interlocked.Read(ref _acksReceived);

    /// <summary>
    /// When the last acknowledgement arrived, on the <see cref="Environment.TickCount64"/> clock, or
    /// when this processor was created if none has.
    /// </summary>
    public long LastAckAtMs => Volatile.Read(ref _lastAckAtMs);
    public long AcksSent => Interlocked.Read(ref _acksSent);

    /// <summary>
    /// Serialized payload bytes that crossed the wire for this session, resends included and chunk
    /// headers excluded.
    /// </summary>
    public long PayloadBytes => Interlocked.Read(ref _payloadBytes);
    public int PendingBatches => _pendingBatches;
    public bool HasUnackedBatches => !_unackedBatches.IsEmpty;
    public int UnackedCount => _unackedBatches.Count;
    public int NextExpectedBatchId => _receiverState?.NextExpectedBatchId ?? 0;

    public BatchProcessor(ISessionContext<T> context, string transferId, int batchSize)
    {
        _context = context;
        _transferId = transferId;
        _batchSize = batchSize;
        _sendChannel = Channel.CreateUnbounded<IControlMessage>();

        _byteStream = context.Options.ByteStreamBatchBytes is not null;
        if (_byteStream)
        {
            if (typeof(T) != typeof(byte[]))
                throw new InvalidOperationException("The byte-stream mode carries byte[] only.");
        }
        else
        {
            _serializer = context.Serializer
                ?? throw new InvalidOperationException("A serializer is required outside the byte-stream mode.");
        }
    }

    public Task StartSenderAsync(CancellationToken token)
    {
        _unackedBatches.Clear();

        // maxCount is set as well as initialCount: an over-release means the slot accounting is
        // wrong somewhere, and we would rather find out than silently widen the window.
        _inFlightSlots = _context.Options.MaxInFlightBatches is { } max
            ? new SemaphoreSlim(max, max)
            : null;

        if (_byteStream)
        {
            _byteAccumulator = new ByteStreamAccumulator(
                _context.Options.ByteStreamBatchBytes!.Value, _context.BatchFlushIntervalMs,
                _context.StreamManager, _inFlightSlots, ReleaseInFlightSlot);
            _byteAccumulator.StartFlushInterval();

            _serializationWorker = Task.Run(() => StreamBatchWorkerAsync(token));
            return Task.CompletedTask;
        }

        _batchAccumulator = new BatchAccumulator<T>(_batchSize, _context.BatchFlushIntervalMs, _inFlightSlots);
        _batchAccumulator.StartFlushInterval();
        
        _serializationWorker = Task.Run(() => SerializationWorkerAsync(token));
        
        return Task.CompletedTask;
    }

    public Task StartReceiverAsync()
    {
        // Buffer and sink are kept across reconnections: the consumer's enumeration is over this
        // buffer, and a new one would leave it waiting on a buffer nothing writes to.
        if (_sink is null)
        {
            if (_byteStream)
            {
                _byteSink = new ByteChunkSink(new ReceiveBuffer<ByteChunk>(_context.Options.ReceiveBufferCapacity));
                _sink = _byteSink;
            }
            else
            {
                _receiveBuffer = new ReceiveBuffer<T>(_context.Options.ReceiveBufferCapacity);
                _sink = new ItemChunkSink<T>(_receiveBuffer, _serializer!);
            }
        }

        int startBatchId = _receiverState?.NextExpectedBatchId ?? 0;

        // A reconnection replaces the state; the chunks it held out of order go back to the pool
        // rather than away with the instance. The sender resends them from startBatchId.
        _receiverState?.DiscardReordered();

        _receiverState = new ReceiverState<T>(_sink, startBatchId);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Adds an item; in the byte-stream mode, appends its bytes to the stream, where no boundary
    /// between two calls survives.
    /// </summary>
    public async Task SendAsync(T item, CancellationToken token)
    {
        if (_batchAccumulator != null)
            await _batchAccumulator.AddAsync(item, token);
        else if (_byteAccumulator != null)
            await _byteAccumulator.AddAsync((byte[])(object)item!, token);
    }

    public async Task SendRangeAsync(IEnumerable<T> items, CancellationToken token)
    {
        if (_batchAccumulator != null)
        {
            await _batchAccumulator.AddRangeAsync(items, token);
        }
        else if (_byteAccumulator != null)
        {
            foreach (var item in items)
                await _byteAccumulator.AddAsync((byte[])(object)item!, token);
        }
    }

    /// <summary>
    /// Appends bytes to the stream of a byte-stream transfer. They are copied before this returns.
    /// </summary>
    public async Task SendBytesAsync(ReadOnlyMemory<byte> bytes, CancellationToken token)
    {
        if (_byteAccumulator != null)
            await _byteAccumulator.AddAsync(bytes, token);
    }

    /// <summary>
    /// The items of the transfer; in the byte-stream mode, one <c>byte[]</c> per batch received.
    /// </summary>
    public IAsyncEnumerable<T> ReceiveAsync(CancellationToken token)
    {
        if (_byteStream)
            return (IAsyncEnumerable<T>)(object)CopyChunksAsync(ReceiveChunksAsync(token), token);

        if (_receiveBuffer == null) throw new InvalidOperationException("Receiver not started");
        return _receiveBuffer.ReadAllAsync(token);
    }

    /// <summary>
    /// The batches of a byte-stream transfer as they were received. Each <see cref="ByteChunk"/> is
    /// the consumer's to dispose; one it leaves unread when the session is released is disposed there.
    /// </summary>
    public IAsyncEnumerable<ByteChunk> ReceiveChunksAsync(CancellationToken token)
    {
        if (!_byteStream) throw new InvalidOperationException("Not a byte-stream transfer");
        if (_byteSink == null) throw new InvalidOperationException("Receiver not started");
        return _byteSink.ReadAllAsync(token);
    }

    private static async IAsyncEnumerable<byte[]> CopyChunksAsync(
        IAsyncEnumerable<ByteChunk> chunks, [EnumeratorCancellation] CancellationToken token)
    {
        await foreach (var chunk in chunks.WithCancellation(token).ConfigureAwait(false))
        {
            try
            {
                yield return chunk.Memory.ToArray();
            }
            finally
            {
                chunk.Dispose();
            }
        }
    }

    public async Task CompleteSenderAsync(CancellationToken token)
    {
        if (_batchAccumulator != null)
            await _batchAccumulator.CompleteAsync(token);
        else if (_byteAccumulator != null)
            await _byteAccumulator.CompleteAsync(token);
        else
            return;

        if (_serializationWorker != null)
        {
            await _serializationWorker;
            _serializationWorker = null;
        }
        _batchAccumulator?.Dispose();
        _batchAccumulator = null;
        _byteAccumulator?.Dispose();
        _byteAccumulator = null;
    }
    
    /// <summary>
    /// Ends delivery to the consumer. The outgoing channel is deliberately left open: the
    /// acknowledgement the receiver still owes its sender may have to be written again on a
    /// reopened stream, and a completed writer makes that resend throw instead.
    /// </summary>
    public void CompleteReceiver()
    {
        _sink?.Complete();
    }
    
    public void CompleteSenderChannel()
    {
        _sendChannel.Writer.TryComplete();
    }

    /// <summary>
    /// Releases a producer parked on the send window of a transfer that has failed. Items the
    /// accumulator still holds are dropped with it; bytes, with their batches' slots and streams.
    /// </summary>
    public void AbandonSender()
    {
        _batchAccumulator?.Abandon();
        _byteAccumulator?.Abandon();
    }
    
    public Task WaitForCompleteAckAsync(CancellationToken token)
    {
        return _completeAckReceived?.Task.WaitAsync(token) ?? Task.CompletedTask;
    }
    
    public async Task ProcessChunkAsync(ChunkData chunk, CancellationToken ct = default)
    {
        Interlocked.Add(ref _payloadBytes, chunk.Header.PayloadLength);

        switch (chunk.Header.Type)
        {
            case ChunkType.Batch:
            case ChunkType.StreamBatch:
                await HandleBatchChunkAsync(chunk, ct);
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
            case ChunkType.Abort:
                HandleAbort((AbortReason)chunk.Header.BatchId);
                break;
            default:
                throw new WireProtocolViolationException($"Unknown chunk type: {(byte)chunk.Header.Type}");
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
                // A batch that is never sent is a gap the receiver's Complete check cannot see when it
                // is the last one: carrying on reported such a transfer as completed, short of that
                // batch. The worker stops and the session fails.
                _context.Logger.LogError(ex, "Batch {BatchId} could not be serialized (Transfer={TransferId})", batchId, _transferId);
                OnSerializationFailed?.Invoke(ex);
                return;
            }
        }
    }
    
    /// <summary>
    /// The send worker of the byte-stream mode. Batches it leaves in the channel when it stops are
    /// the accumulator's to dispose.
    /// </summary>
    private async Task StreamBatchWorkerAsync(CancellationToken ct)
    {
        await foreach (var (batch, batchId) in _byteAccumulator!.BatchReader.ReadAllAsync(ct))
        {
            try
            {
                await SendStreamBatch(batch, batchId, ct);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                // Same gap as a batch that cannot be serialized: the worker stops and the session fails.
                _context.Logger.LogError(ex, "Batch {BatchId} could not be registered (Transfer={TransferId})", batchId, _transferId);
                OnSerializationFailed?.Invoke(ex);
                return;
            }
        }
    }

    private async Task SendStreamBatch(RecyclableMemoryStream batch, int batchId, CancellationToken ct)
    {
        // The batch came with its slot. From the moment it lands in _unackedBatches, the ack path
        // owns giving it back; before that, this method does.
        var registered = false;
        try
        {
            using (batch)
            {
                await RegisterBatchAsync(batch, batchId, (int)batch.Length - sizeof(int), () => registered = true, ct);
            }
        }
        catch
        {
            if (!registered) ReleaseInFlightSlot();
            throw;
        }
    }

    private int _maxBatchIdSent = -1;

    /// <summary>
    /// Raises <see cref="_maxBatchIdSent"/> to <paramref name="batchId"/> if it is higher.
    /// </summary>
    /// <remarks>
    /// Read from the completion path on another thread, so the write has to be atomic. It does not
    /// need a lock to be: a monotonic maximum is what compare-and-exchange is for.
    /// </remarks>
    private void UpdateMaxBatchIdSent(int batchId)
    {
        int current;
        do
        {
            current = _maxBatchIdSent;
            if (batchId <= current) return;
        } while (Interlocked.CompareExchange(ref _maxBatchIdSent, batchId, current) != current);
    }

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
        // Not null: this is the item mode, which is not built without a serializer.
        await _serializer!.SerializeBatchAsync(memoryStream, items);

        await RegisterBatchAsync(memoryStream, batchId, items.Count, onRegistered, ct);
    }

    /// <summary>
    /// Copies a batch - its 4-byte count, then its body - out of <paramref name="memoryStream"/> into a
    /// <see cref="BatchEntry"/>, registers it as unacknowledged and queues it for the write loop. The
    /// stream stays the caller's to dispose. <paramref name="onRegistered"/> runs once the entry is in
    /// <see cref="_unackedBatches"/>: from then on, the ack path owns the batch's slot.
    /// </summary>
    private async Task RegisterBatchAsync(
        RecyclableMemoryStream memoryStream, int batchId, int count, Action onRegistered, CancellationToken ct)
    {
        var totalLen = (int)memoryStream.Length;
        var owner = new PooledMemoryOwner(memoryStream.GetBuffer().AsMemory(0, totalLen));
        
        // Wrap in BatchEntry
        var entry = new BatchEntry(owner);
        
        // 1 reference for _unackedBatches (initial ref = 1)
        _unackedBatches[batchId] = entry;
        onRegistered();

        // 1 reference for BatchControlMessage. TryRetain increments the entry's ref count and
        // hands back the payload; the message carries the entry itself, so the return is unused.
        // Cannot be null here - the entry was created on the line above.
        entry.TryRetain();

        Interlocked.Increment(ref _pendingBatches);
        Interlocked.Add(ref _itemsSent, count);
        
        UpdateMaxBatchIdSent(batchId);
        
        await _sendChannel.Writer.WriteAsync(new BatchControlMessage(batchId, entry), ct);
    }
    
    private async Task HandleBatchChunkAsync(ChunkData chunk, CancellationToken ct)
    {
        if (_receiverState == null) return;

        // Checked before any memory is rented. An UndecodableBatchException, because it is the one
        // that makes the session tell the sender; a plain protocol violation would only end this side.
        if ((chunk.Header.Type == ChunkType.StreamBatch) != _byteStream)
            throw new UndecodableBatchException("A batch of a kind this transfer does not carry was received.");

        // The copy is what is handed on: the byte-stream mode gives this owner away, never the
        // chunk's own, whose copies would each dispose it.
        var owner = new PooledMemoryOwner(chunk.Payload);
        var rxResult = await _receiverState.ProcessBatchAsync(chunk.Header.BatchId, owner, owner.Memory, ct);
        
        Interlocked.Add(ref _itemsReceived, chunk.Header.Count);

        // Chunks held out of order carry serialized bytes that no budget counts. Over an ordered
        // stream this should never fire; it is here so that "should never" is something a run can
        // contradict, rather than a second budget built against an intuition.
        if (_receiverState.ReorderedChunks > 0)
        {
            _context.Logger.LogDebug(
                "Holding {Count} out-of-order chunk(s) for Transfer={TransferId}, outside the receive budget",
                _receiverState.ReorderedChunks, _transferId);
        }

        if (rxResult.BatchIdToAck.HasValue)
        {
            await _sendChannel.Writer.WriteAsync(new ControlChunkMessage(ChunkType.BatchAck, rxResult.BatchIdToAck.Value));
            Interlocked.Increment(ref _acksSent);
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
        CountAck();
    }

    private void CountAck()
    {
        Interlocked.Increment(ref _acksReceived);
        Volatile.Write(ref _lastAckAtMs, Environment.TickCount64);
    }

    /// <summary>
    /// Gives an in-flight slot back to the producer. Called from every path that removes a batch
    /// from <see cref="_unackedBatches"/>, from the send worker when a batch fails before it ever
    /// gets there, and from <see cref="ByteStreamAccumulator"/> for a batch it never hands off.
    /// Missing one of those closes the window permanently.
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
    
    public event Action<int>? OnPongReceived;
    public event Action? OnCompleteAckReceived;
    public event Func<Task>? OnCompleteReceived;
    public event Action<AbortReason>? OnAbortReceived;

    /// <summary>
    /// Raised once, on the send worker, when a batch cannot be serialized - in the byte-stream mode,
    /// registered. The worker has stopped: nothing after that batch will be sent.
    /// </summary>
    public event Action<Exception>? OnSerializationFailed;

    private void HandlePong(int nextExpectedBatchId)
    {
        OnPongReceived?.Invoke(nextExpectedBatchId);
    }
    
    public async Task SendCompleteSignalAsync()
    {
        // One for the whole closing sequence: the receiver acknowledges once, so a retry that replaced
        // it would lose an acknowledgement landing between two attempts.
        _completeAckReceived ??= new TaskCompletionSource();
        // Send the Last Batch ID so receiver can verify if it missed anything
        await _sendChannel.Writer.WriteAsync(new ControlChunkMessage(ChunkType.Complete, _maxBatchIdSent));
    }
    
    private async Task HandleCompleteAsync(ChunkData chunk)
    {
        int lastBatchIdFromSender = chunk.Header.BatchId;
        int nextExpected = _receiverState?.NextExpectedBatchId ?? 0;
        
        // Validation: every batch up to lastBatchId must already be in.
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

        // The session enters its closing phase before the acknowledgement can be written: it reads the
        // end of the control stream that carries it as the hub having read it only in that phase.
        if (OnCompleteReceived != null) await OnCompleteReceived.Invoke();
        await _sendChannel.Writer.WriteAsync(new ControlChunkMessage(ChunkType.CompleteAck, 0));
    }
    
    private void HandleCompleteAck()
    {
        _completeAckReceived?.TrySetResult();
        OnCompleteAckReceived?.Invoke();
    }

    /// <summary>
    /// The peer gave up before the transfer completed. Releases whatever this side has parked -
    /// a producer on the in-flight window, a consumer on the receive buffer - with the reason
    /// carried on the wire, then tells <see cref="DataSession{T}"/> so it can fail the state
    /// machine and pass the same reason to callers waiting elsewhere (CompleteAsync,
    /// WaitForCompletionAsync).
    /// </summary>
    private void HandleAbort(AbortReason reason)
    {
        Fail(reason.Describe());
        OnAbortReceived?.Invoke(reason);
    }

    /// <summary>
    /// Releases whatever is parked on a transfer that cannot finish: a producer on the send window at
    /// once, a consumer once it has taken what was already admitted. Ending the enumeration quietly
    /// instead would tell the consumer it had everything.
    /// </summary>
    public void Fail(string reason)
    {
        _receiveBuffer?.Complete(new TransferFailedException(_transferId, reason));
        AbandonSender();
    }

    /// <summary>
    /// Enqueues Abort on the outgoing channel. Enqueuing is not reaching the wire: the caller
    /// confirms that by waiting on <see cref="HttpStreamManager.WriteTask"/> for the direction this
    /// side writes on, not on this call returning.
    /// </summary>
    public void EnqueueAbort(AbortReason reason) =>
        _sendChannel.Writer.TryWrite(new ControlChunkMessage(ChunkType.Abort, (int)reason));

    public async Task ResendCompleteAckAsync()
    {
        _context.Logger.LogInformation("Resending CompleteAck for Transfer={TransferId}", _transferId);
        await _sendChannel.Writer.WriteAsync(new ControlChunkMessage(ChunkType.CompleteAck, 0));
    }
    
    /// <summary>
    /// Resends unacked batches directly to the stream (bypass channel).
    /// </summary>
    public async Task ResendUnackedBatchesAsync(Stream outputStream, Guid transferGuid, bool didPingPong, int nextExpectedFromPong, CancellationToken token)
    {
        // A snapshot: ordering the dictionary itself sizes an array and then copies into it, and a
        // batch registered in between throws, which ends the stream the resend is part of.
        foreach (var kvp in _unackedBatches.ToArray().OrderBy(x => x.Key))
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
                    CountAck();
                    // Same as an explicit ack: the batch is done, give the slot back. Without this
                    // the window shrinks by one on every resumed batch and never reopens.
                    ReleaseInFlightSlot();
                    
                    _context.Logger.LogDebug("Skipped resend of batch {BatchId} (receiver has it) for Transfer={TransferId}", kvp.Key, _transferId);
                }
                continue;
            }
            
            try
            {
                if (!await WriteBatchAsync(outputStream, transferGuid, kvp.Key, kvp.Value, token))
                {
                    _context.Logger.LogDebug("Race condition avoided: Batch {BatchId} was disposed/acked concurrently during resend (Transfer={TransferId})", kvp.Key, _transferId);
                    continue;
                }

                await outputStream.FlushAsync(token);
                _context.Logger.LogInformation("Resent unacked batch {BatchId} on reconnect (Transfer={TransferId})", kvp.Key, _transferId);
            }
            catch (Exception ex)
            {
                _context.Logger.LogWarning("Failed to resend unacked batch {BatchId}: {Message}", kvp.Key, ex.Message);
                throw;
            }
        }
    }

    /// <summary>
    /// Writes a batch from its entry, under a reference of its own. An entry its acknowledgement
    /// released in the meantime has nothing left to write: that returns false.
    /// </summary>
    public async Task<bool> WriteBatchAsync(Stream outputStream, Guid transferGuid, int batchId, BatchEntry entry, CancellationToken token)
    {
        var payload = entry.TryRetain();
        if (payload == null) return false;

        try
        {
            var memory = payload.Memory;
            var count = BinaryPrimitives.ReadInt32LittleEndian(memory.Span[..4]);
            var type = _byteStream ? ChunkType.StreamBatch : ChunkType.Batch;
            await ChunkIO.WriteBatchAsync(outputStream, transferGuid, type, batchId, count, memory[4..], token);
            Interlocked.Add(ref _payloadBytes, memory.Length);
            return true;
        }
        finally
        {
            entry.Release();
        }
    }
    


    public ValueTask DisposeAsync()
    {
        _sendChannel.Writer.TryComplete();
        _serializationWorker = null;

        // Dispose the accumulator first: it cancels its own token, which releases a producer
        // parked on the in-flight window before that window is disposed underneath it.
        _batchAccumulator?.Dispose();
        _byteAccumulator?.Dispose();

        // Not disposed on purpose: a producer may still be unwinding out of a cancelled wait on
        // it, and SemaphoreSlim only needs disposing once AvailableWaitHandle has been used.
        _inFlightSlots = null;

        // Same reason, other end: a read loop parked on the byte gate has to be let go. The
        // session token reaches it too, but only one of the two is an explicit release, and the
        // parked loop cannot reach the finally that would otherwise complete the buffer.
        _sink?.Complete();
        _byteSink?.DiscardUnread();
        _receiverState?.DiscardReordered();
        
        foreach(var kvp in _unackedBatches)
        {
            // Force release/dispose
            kvp.Value.Release(); 
        }
        _unackedBatches.Clear();
        return ValueTask.CompletedTask;
    }
}
