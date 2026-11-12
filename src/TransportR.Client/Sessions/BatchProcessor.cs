using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using TransportR.Abstractions;
using TransportR.Client.Batching;
using TransportR.Client.Internal;
using TransportR.Protocol;

namespace TransportR.Client.Sessions;

/// <summary>
/// Handles the protocol messages of a transfer, over the batches its source gives and the sink that
/// takes the ones it receives. Manages sender/receiver state, unacked batches, and flow control.
/// </summary>
internal class BatchProcessor : IAsyncDisposable
{
    private readonly ISessionContext _context;
    private readonly string _transferId;

    // Channels
    private readonly Channel<IControlMessage> _sendChannel;
    private readonly Func<IChunkSink> _openSink;
    private IChunkSink? _sink;

    // Sender State: the source the producer fills and the send worker reads. Held once the sender
    // starts, and dropped when it completes.
    private readonly IBatchSource _batches;
    private IBatchSource? _source;
    private ChunkType _batchType = ChunkType.Batch;

    private readonly ConcurrentDictionary<int, BatchEntry> _unackedBatches = new();

    private Task? _sendWorker;

    // In-flight window (ClientOptions.MaxInFlightBatches). Slots are taken by the source, before the
    // body of the batch exists, and released here when the batch leaves _unackedBatches. Null =
    // unbounded.
    private SemaphoreSlim? _inFlightSlots;

    private int _pendingBatches = 0;

    // Receiver State
    private ReceiverState? _receiverState;

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

    /// <param name="batches">Where the batches to send come from; opened when the sender starts.</param>
    /// <param name="openSink">
    /// Makes the sink the received batches go to, when the receiver first starts. The sink is kept for
    /// the life of the processor.
    /// </param>
    public BatchProcessor(ISessionContext context, string transferId, IBatchSource batches, Func<IChunkSink> openSink)
    {
        _context = context;
        _transferId = transferId;
        _batches = batches;
        _openSink = openSink;
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

        var source = _batches;
        source.Open(_inFlightSlots, ReleaseInFlightSlot);

        // Kept apart from the source: a resend after the sender has completed still writes batches.
        _batchType = source.BatchType;
        _source = source;
        source.StartFlushInterval();

        _sendWorker = Task.Run(() => SendWorkerAsync(source, token));
        return Task.CompletedTask;
    }

    public Task StartReceiverAsync()
    {
        // The sink is kept across reconnections: the consumer's enumeration is over it, and a new
        // one would leave it waiting on a sink nothing writes to.
        _sink ??= _openSink();

        int startBatchId = _receiverState?.NextExpectedBatchId ?? 0;

        // A reconnection replaces the state; the chunks it held out of order go back to the pool
        // rather than away with the instance. The sender resends them from startBatchId.
        _receiverState?.DiscardReordered();

        _receiverState = new ReceiverState(_sink, startBatchId);
        return Task.CompletedTask;
    }

    public async Task CompleteSenderAsync(CancellationToken token)
    {
        if (_source is not { } source) return;

        await source.CompleteAsync(token);

        if (_sendWorker != null)
        {
            await _sendWorker;
            _sendWorker = null;
        }
        source.Dispose();
        source.Detach();
        _source = null;
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
        _source?.Abandon();
    }

    public Task WaitForCompleteAckAsync(CancellationToken token)
    {
        return _completeAckReceived?.Task.WaitAsync(token) ?? Task.CompletedTask;
    }

    /// <summary>
    /// Handles one chunk read off the data stream.
    /// </summary>
    /// <param name="payload">
    /// The memory the payload was read into, exactly its length; <c>null</c> when it has none. Owned
    /// by this call from its start, whatever happens: a batch's goes on to the receiver state unless
    /// the batch is refused, anything else is returned here, and either way once.
    /// </param>
    /// <param name="ct">Cancels a batch's wait for room in the receive buffer.</param>
    public async Task ProcessChunkAsync(ChunkHeader header, IMemoryOwner<byte>? payload, CancellationToken ct = default)
    {
        Interlocked.Add(ref _payloadBytes, header.PayloadLength);

        if (header.Type is ChunkType.Batch or ChunkType.StreamBatch)
        {
            await HandleBatchChunkAsync(header, payload, ct);
            return;
        }

        using (payload)
        {
            switch (header.Type)
            {
                case ChunkType.BatchAck:
                    HandleBatchAck(header.BatchId);
                    break;
                case ChunkType.Ping:
                    await HandlePingAsync();
                    break;
                case ChunkType.Pong:
                    HandlePong(header.BatchId);
                    break;
                case ChunkType.Complete:
                    await HandleCompleteAsync(header);
                    break;
                case ChunkType.CompleteAck:
                    HandleCompleteAck();
                    break;
                case ChunkType.Abort:
                    HandleAbort((AbortReason)header.BatchId);
                    break;
                default:
                    throw new WireProtocolViolationException($"Unknown chunk type: {(byte)header.Type}");
            }
        }
    }
    /// <summary>
    /// The one send worker: reads each batch off the source, which serializes it in the item mode,
    /// and registers it. What it leaves in the source when it stops is the source's to give back.
    /// </summary>
    private async Task SendWorkerAsync(IBatchSource source, CancellationToken ct)
    {
        // Read as ChannelReader.ReadAllAsync reads: a cancellation while waiting for a batch ends
        // the worker cancelled, one while a batch is read or registered ends it quietly.
        while (await source.WaitToReadAsync(ct))
        {
            while (true)
            {
                OutgoingBatch? read = null;
                try
                {
                    read = await source.TryReadAsync(ct);
                    if (read is not { } batch) break;

                    await SendBatchAsync(batch, ct);
                }
                catch (OperationCanceledException) { return; }
                catch (Exception ex)
                {
                    // A batch that is never sent is a gap the receiver's Complete check cannot see when
                    // it is the last one: carrying on reported such a transfer as completed, short of
                    // that batch. The worker stops and the session fails.
                    if (read is { } failed)
                        _context.Logger.LogError(ex, "Batch {BatchId} could not be registered (Transfer={TransferId})", failed.BatchId, _transferId);
                    else
                        _context.Logger.LogError(ex, "A batch could not be serialized (Transfer={TransferId})", _transferId);
                    OnSerializationFailed?.Invoke(ex);
                    return;
                }
            }
        }
    }

    private async Task SendBatchAsync(OutgoingBatch batch, CancellationToken ct)
    {
        // The batch came with its slot and its body. The slot is the ack path's to give back from the
        // moment the batch lands in _unackedBatches, the body its entry's to dispose from the moment
        // one holds it; short of either, this method gives back what it still has.
        BatchEntry? entry = null;
        var registered = false;
        try
        {
            // Refused here rather than by a receiver at the same bound, after the whole batch has crossed.
            // The payload on the wire is the body less its 4-byte count.
            var payloadLength = batch.Body.Length - sizeof(int);
            if (payloadLength > _context.Options.MaxBatchBytes)
                throw new InvalidOperationException(
                    $"A batch of {payloadLength} bytes is over WithMaxBatchBytes ({_context.Options.MaxBatchBytes} bytes).");

            entry = new BatchEntry(batch.Body, batch.Body.GetReadOnlySequence());
            await RegisterBatchAsync(entry, batch.BatchId, batch.Count, () => registered = true, ct);
        }
        catch
        {
            if (!registered)
            {
                if (entry is null) batch.Body.Dispose();
                else entry.Release();
                ReleaseInFlightSlot();
            }
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

    /// <summary>
    /// Registers a batch - its 4-byte count, then its body, held by <paramref name="entry"/> - as
    /// unacknowledged and queues it for the write loop. <paramref name="onRegistered"/> runs once the
    /// entry is in <see cref="_unackedBatches"/>: from then on, the ack path owns the batch's slot and
    /// the entry's first reference.
    /// </summary>
    private async Task RegisterBatchAsync(
        BatchEntry entry, int batchId, int count, Action onRegistered, CancellationToken ct)
    {
        // 1 reference for _unackedBatches (initial ref = 1)
        _unackedBatches[batchId] = entry;
        onRegistered();

        // 1 reference for BatchControlMessage. TryRetain increments the entry's ref count and
        // hands back the body; the message carries the entry itself, so the return is unused.
        entry.TryRetain();

        Interlocked.Increment(ref _pendingBatches);
        Interlocked.Add(ref _itemsSent, count);

        UpdateMaxBatchIdSent(batchId);

        try
        {
            await _sendChannel.Writer.WriteAsync(new BatchControlMessage(batchId, entry), ct);
        }
        catch
        {
            // A message the channel refused reaches no write loop, the one that gives its reference back.
            entry.Release();
            throw;
        }
    }

    /// <param name="payload">
    /// Returned by this method until the receiver state takes it, which it does on every path that
    /// reaches it.
    /// </param>
    private async Task HandleBatchChunkAsync(ChunkHeader header, IMemoryOwner<byte>? payload, CancellationToken ct)
    {
        if (_receiverState == null || _sink == null)
        {
            payload?.Dispose();
            return;
        }

        // Checked before anything is decoded. An UndecodableBatchException, because it is the one
        // that makes the session tell the sender; a plain protocol violation would only end this side.
        if (header.Type != _sink.BatchType)
        {
            payload?.Dispose();
            throw new UndecodableBatchException("A batch of a kind this transfer does not carry was received.");
        }

        // A batch without a payload is handed on all the same, over memory with nothing to return.
        var owner = payload ?? new PooledPayload(0);
        var rxResult = await _receiverState.ProcessBatchAsync(header.BatchId, owner, owner.Memory, ct);

        // The count of a batch of bytes is its length, not items.
        if (header.Type == ChunkType.Batch) Interlocked.Add(ref _itemsReceived, header.Count);

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
    /// gets there, and from the <see cref="IBatchSource"/> for a batch it cannot produce or never
    /// hands off. Missing one of those closes the window permanently.
    /// </summary>
    /// <remarks>
    /// This runs on the ACK-reading loop (the Control stream read task), which is distinct from
    /// both the send worker and the data write loop. It only ever releases, never waits,
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
    /// Raised once, on the send worker, when a batch cannot be serialized or registered. The worker
    /// has stopped: nothing after that batch will be sent.
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

    private async Task HandleCompleteAsync(ChunkHeader header)
    {
        int lastBatchIdFromSender = header.BatchId;
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
    /// carried on the wire, then tells <see cref="DataSession"/> so it can fail the state
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
        _sink?.Fail(new TransferFailedException(_transferId, reason));
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
        if (entry.TryRetain() is not { } body) return false;

        try
        {
            await ChunkIO.WriteBatchAsync(outputStream, transferGuid, _batchType, batchId, ReadCount(body), body.Slice(sizeof(int)), token);
            Interlocked.Add(ref _payloadBytes, body.Length);
            return true;
        }
        finally
        {
            entry.Release();
        }
    }

    private static int ReadCount(ReadOnlySequence<byte> body)
    {
        Span<byte> count = stackalloc byte[sizeof(int)];
        body.Slice(0, sizeof(int)).CopyTo(count);
        return BinaryPrimitives.ReadInt32LittleEndian(count);
    }


    public ValueTask DisposeAsync()
    {
        _sendChannel.Writer.TryComplete();
        _sendWorker = null;

        // Dispose the source first: its accumulator cancels its own token, which releases a
        // producer parked on the in-flight window before that window is disposed underneath it.
        _source?.Dispose();

        // Not disposed on purpose: a producer may still be unwinding out of a cancelled wait on
        // it, and SemaphoreSlim only needs disposing once AvailableWaitHandle has been used.
        _inFlightSlots = null;

        // Same reason, other end: a read loop parked on the byte gate has to be let go. The
        // session token reaches it too, but only one of the two is an explicit release, and the
        // parked loop cannot reach the finally that would otherwise complete the buffer.
        _sink?.Complete();
        _sink?.DiscardUnread();
        _receiverState?.DiscardReordered();

        // A queued batch holds a reference of its own, which only the write loop gives back once it
        // has read the message. The channel hands each message out once, to that loop or here.
        while (_sendChannel.Reader.TryRead(out var queued))
        {
            if (queued is BatchControlMessage batch) batch.Entry.Release();
        }

        // The reference the dictionary holds is released by whoever takes the entry out of it, here or
        // an acknowledgement crossing this release, as in HandleBatchAck. Released here without
        // the removal, it is released twice, and the second can dispose a body a resend still writes.
        foreach (var (batchId, _) in _unackedBatches)
        {
            if (_unackedBatches.TryRemove(batchId, out var entry)) entry.Release();
        }
        return ValueTask.CompletedTask;
    }
}
