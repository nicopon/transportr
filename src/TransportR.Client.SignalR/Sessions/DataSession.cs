using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Threading.Channels;
using TransportR.Abstractions;
using TransportR.Client.SignalR.Batching;
using TransportR.Interfaces;
using TransportR.Protocol;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Logging;

using TransportR.Client.SignalR.Internal;
using TransportR.Client.SignalR.Sessions;
using TransportR.Models;
using TransportR.Services;

namespace TransportR.Client.SignalR.Sessions;

/// <summary>
/// Encapsulates the state and logic for a single data transfer session.
/// Handles bidirectional HTTP streams, batch serialization, acknowledgements, and reconnection.
/// </summary>
internal class DataSession<T> : IDataSession<T>
{
    // Interval between two Complete signals. Short enough that a lost one costs little, long
    // enough that a draining receiver is not asked again while it is still catching up.
    private const int CompleteRetryMs = 5000;

    // Reopening a stream in the completing phase can fail as fast as it is attempted; without a
    // pause between tries the session loop turns a dead hub into a spin.
    private const int CompletingPhaseRetryMs = 500;

    private const string ReleasedBeforeCompletion =
        "The transfer handle was released before the transfer completed; the transfer was abandoned.";

    public string TransferId { get; }
    public int BatchSize { get; }
    public string DataUrl { get; }
    
    private readonly ISignalRInteraction _signalR;
    private readonly ISessionConfig _config;
    
    private readonly CancellationTokenSource _cts;
    private readonly HttpStreamManager _httpStreamManager;
    private readonly BatchProcessor<T> _batchProcessor;
    private readonly ReconnectionHandler _reconnectionHandler;
    private readonly DataSessionStateMachine _fsm;
    private bool _isSender;

    private readonly Timer _heartbeatTimer;
    private int _disposed;

    // Session Statistics
    public DateTime StartedAt { get; } = DateTime.UtcNow;
    
    // Public properties for GetTransfers()
    public bool IsSender => _isSender;
    public SessionState State => _fsm.CurrentState;

    /// <summary>
    /// True once the session has stopped, whether it succeeded or failed.
    /// </summary>
    private bool IsTerminal => _fsm.CurrentState is SessionState.Completed or SessionState.Failed;

    /// <summary>
    /// True once nothing more will happen here, which includes a session released before it could
    /// reach an outcome. Wider than <see cref="IsTerminal"/>, which answers what became of the
    /// transfer: whoever waits on an outcome waits on this and reports on that.
    /// </summary>
    private bool IsStopped => IsTerminal || _fsm.CurrentState == SessionState.Disposed;

    private bool IsInCompletingPhase => _fsm.IsCompleting;

    public long ItemsProcessed => IsSender ? _batchProcessor.ItemsSent : _batchProcessor.ItemsReceived;
    public long BytesTransferred => _batchProcessor.PayloadBytes;
    public long AcksReceived => _batchProcessor.AcksReceived;
    public long AcksSent => _batchProcessor.AcksSent;
    
    // Ping/Pong connection probing delegated to ReconnectionHandler
    
    public DataSession(ISessionContext<T> context, string transferId, int batchSize, string dataUrl)
    {
        _signalR = context;
        _config = context; // ISessionContext implements ISessionConfig
        
        TransferId = transferId;
        BatchSize = batchSize;
        DataUrl = dataUrl.TrimEnd('/');
        
        _cts = new CancellationTokenSource();
        _batchProcessor = new BatchProcessor<T>(context, transferId, batchSize);
        _fsm = new DataSessionStateMachine(transferId, context.Logger);
        
        // Pass segregated interfaces to HttpStreamManager
        _httpStreamManager = new HttpStreamManager(context, context, transferId, dataUrl); // IStreamTransport, ISessionConfig
        
        _reconnectionHandler = new ReconnectionHandler(context, transferId);
            
        // Hook up events
        _batchProcessor.OnPongReceived += (id) => {
            _reconnectionHandler.HandleStreamPong(id);
        };
        _batchProcessor.OnCompleteReceived += () => {
            // Receiver received Complete from Sender (Domino Step 2)
            _fsm.TransitionTo(SessionState.ReceiverCompleteReceived);
            // Do NOT transition to Completed yet. We must wait for EOF signals from hub.
            // This ensures we stay alive long enough to flush the CompleteAck and for Sender to receive it.
            return Task.CompletedTask;
        };
        
        _heartbeatTimer = new Timer(HeartbeatLoop, null, (int)_config.Options.HeartbeatInterval.TotalMilliseconds, (int)_config.Options.HeartbeatInterval.TotalMilliseconds);
    }

    #region Public API

    public Task StartSenderAsync()
    {
        _config.Logger.LogInformation("Starting Sender Session {TransferId}...", TransferId);
        _isSender = true;
        
        _batchProcessor.StartSenderAsync(_cts.Token);
        _config.Logger.LogInformation("Started serialization worker for {TransferId}", TransferId);
        
        _fsm.TransitionTo(SessionState.Starting);
        _ = RunSessionLoop(_cts.Token);
        return Task.CompletedTask;
    }

    public async Task StartReceiverAsync()
    {
        _config.Logger.LogInformation("Starting Receiver Session {TransferId}...", TransferId);
        _isSender = false;
        
        await _batchProcessor.StartReceiverAsync();
        
        _fsm.TransitionTo(SessionState.Starting);
        _ = RunSessionLoop(_cts.Token);
    }

    public async Task SendAsync(T item, CancellationToken cancellationToken)
    {
        await _batchProcessor.SendAsync(item, cancellationToken);
    }

    public async Task SendRangeAsync(IEnumerable<T> items, CancellationToken cancellationToken)
    {
        await _batchProcessor.SendRangeAsync(items, cancellationToken);
    }

    public async Task CompleteAsync(CancellationToken cancellationToken = default)
    {
        // A released session has nothing left to complete, and draining an accumulator that is
        // already gone raises a disposal error the caller can do nothing with.
        if (_fsm.CurrentState == SessionState.Disposed)
        {
            throw new TransferFailedException(TransferId, ReleasedBeforeCompletion);
        }

        await _batchProcessor.CompleteSenderAsync(cancellationToken);
        
        // We used to wait for PendingBatches > 0 here, but that causes deadlocks if ACKs are buffered.
        // We now rely on the "Complete" signal to flush the pipe.
        // The Receiver checks for missing batches before accepting Complete.
        // If it rejects it (missing data), the Sender loops below ensuring retry.
        
        // Just log the status for debugging
        if (_batchProcessor.PendingBatches > 0)
        {
             _config.Logger.LogInformation("CompleteAsync entered with {Pending} pending batches. Proceeding to SendComplete to flush...", _batchProcessor.PendingBatches);
        }
        
        // Send Complete and wait for CompleteAck (with retry)
        _config.Logger.LogInformation("Sending Complete message (Transfer={TransferId})", TransferId);

        var stallTimeout = _config.Options.CompleteStallTimeout;
        var retryInterval = TimeSpan.FromMilliseconds(Math.Min(CompleteRetryMs, stallTimeout.TotalMilliseconds));

        // The receiver refuses Complete while batches are still missing, so re-sending it is how the
        // tail of a transfer drains and a retry on its own says nothing is wrong. What bounds the
        // loop is therefore the acknowledgement count, not the number of attempts: a peer still
        // acknowledging is still working, one that has gone away stops moving it.
        var lastAcks = _batchProcessor.AcksReceived;
        var lastProgressUtc = DateTime.UtcNow;

        while (!IsStopped)
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            await _batchProcessor.SendCompleteSignalAsync(cancellationToken);
            
            // Wait for CompleteAck
            var ackTask = _batchProcessor.WaitForCompleteAckAsync(cancellationToken);
            var delayTask = Task.Delay(retryInterval, cancellationToken);
            
            var finished = await Task.WhenAny(ackTask, delayTask);

            // Both tasks carry the caller's token, so cancelling completes them rather than leaving
            // them pending: without this, a cancelled wait reads as an acknowledgement and reports
            // a transfer that never finished as Completed.
            cancellationToken.ThrowIfCancellationRequested();

            if (finished == ackTask && ackTask.IsCompletedSuccessfully)
            {
                _config.Logger.LogInformation("CompleteAck received - transfer complete (Transfer={TransferId})", TransferId);
                _fsm.TransitionTo(SessionState.Completed);
                
                // Allow WriteLoop to exit
                _batchProcessor.CompleteSenderChannel();

                // No need to call TransferComplete - hub detects completion via dataplane EOF
                return;
            }

            var acks = _batchProcessor.AcksReceived;
            if (acks != lastAcks)
            {
                lastAcks = acks;
                lastProgressUtc = DateTime.UtcNow;
                _config.Logger.LogInformation(
                    "Complete not acknowledged yet, {Unacked} batches still in flight - retrying (Transfer={TransferId})",
                    _batchProcessor.UnackedCount, TransferId);
                continue;
            }

            var stalledFor = DateTime.UtcNow - lastProgressUtc;
            if (stalledFor >= stallTimeout)
            {
                _fsm.TransitionTo(SessionState.Failed);
                _batchProcessor.CompleteSenderChannel();
                throw new TransferFailedException(
                    TransferId,
                    $"Transfer did not complete: nothing was acknowledged for {stalledFor.TotalSeconds:N0}s.");
            }

            _config.Logger.LogWarning(
                "Complete not acknowledged and nothing acknowledged for {Stalled:N0}s (Transfer={TransferId})",
                stalledFor.TotalSeconds, TransferId);
        }

        // Only reachable when something else stopped the session under us.
        throw new TransferFailedException(
            TransferId,
            _fsm.CurrentState == SessionState.Disposed
                ? ReleasedBeforeCompletion
                : "Transfer failed before it could complete.");
    }
    
    public IAsyncEnumerable<T> ReceiveAsync(CancellationToken cancellationToken)
    {
        return _batchProcessor.ReceiveAsync(cancellationToken);
    }

    public void TriggerReconnect()
    {
        _config.Logger.LogInformation("TriggerReconnect called externally.");
        
        if (_fsm.CurrentState == SessionState.Streaming)
        {
            _fsm.TransitionTo(SessionState.Reconnecting);
            // Cancel streams to break the loop
            _httpStreamManager.CancelStreams();
        }
        else if (IsInCompletingPhase)
        {
             _config.Logger.LogInformation("SignalR disconnected while in completing phase. Cancelling streams to force verification.");
             // Do NOT transition to Reconnecting (preserve completing state logic)
             _httpStreamManager.CancelStreams();
        }
    }

    /// <summary>
    /// Called when Pong is received from hub, confirming connectivity.
    /// </summary>
    public void OnPongReceived()
    {
        _reconnectionHandler.HandleSignalRPong();
    }


    public async Task WaitForCompletionAsync(CancellationToken cancellationToken)
    {
        // Both roles wait for the domino to close the session. Having accepted Complete is not
        // enough for a receiver: its acknowledgement is queued at that point, and a queued chunk is
        // not a delivered one — the stream carrying it can fail, and the resend that follows needs
        // the session the caller is about to release. What says the acknowledgement arrived is the
        // hub ending this session's streams, which is the terminal state itself.
        DateTime? completingSince = null;

        while (!IsStopped)
        {
            if (IsInCompletingPhase)
            {
                completingSince ??= DateTime.UtcNow;

                // Only the ending is bounded, never the transfer: before the completing phase the
                // peer is still working and the caller's token is the only clock that applies.
                if (DateTime.UtcNow - completingSince >= _config.Options.CompleteStallTimeout)
                {
                    throw new TransferFailedException(
                        TransferId,
                        "All data was received, but the transfer never finished closing; the sender cannot know it completed.");
                }
            }

            await Task.Delay(100, cancellationToken);
        }

        if (_fsm.CurrentState == SessionState.Disposed)
        {
            throw new TransferFailedException(TransferId, ReleasedBeforeCompletion);
        }

        if (_fsm.CurrentState == SessionState.Failed)
        {
            throw new TransferFailedException(TransferId, "Transfer failed before it could complete.");
        }
    }

    public async ValueTask DisposeAsync()
    {
        // Releasing twice is the documented pattern meeting a defensive call, not a caller error,
        // and the second Cancel() on a disposed source would throw.
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

        // Releasing a session that has not stopped abandons its transfer. CompleteAsync and
        // WaitForCompletionAsync say so to whoever waits; nobody has to be waiting, so it is said
        // here too.
        if (_fsm.OnReleased())
        {
            _config.Logger.LogWarning(
                "Session released before the transfer ended - transfer abandoned (Transfer={TransferId})",
                TransferId);
        }

        _reconnectionHandler.Dispose();
        await _batchProcessor.DisposeAsync();
        await _httpStreamManager.DisposeAsync();
        _cts.Cancel(); // Stop all loops immediately, do not wait for 5s of retries!


        _heartbeatTimer.Dispose();
        _cts.Dispose();
        _signalR.RemoveSession(TransferId);
    }

    #endregion

    #region Streams Management

    private async Task RunSessionLoop(CancellationToken token)
    {
        _config.Logger.LogInformation("Session Loop {TransferId} Starting. IsSender={IsSender}", TransferId, IsSender);

        while (!token.IsCancellationRequested && _fsm.CurrentState != SessionState.Completed && _fsm.CurrentState != SessionState.Failed)
        {
            try
            {
                switch (_fsm.CurrentState)
                {
                    case SessionState.Starting:
                    case SessionState.Streaming:
                        _fsm.TransitionTo(SessionState.Streaming);
                        await RunStreamsUntilOneEndsAsync(token);
                        break;

                    case SessionState.Reconnecting:
                         // Execute Handshake
                         var online = await _reconnectionHandler.ProbeConnectivityAsync(token);
                         if (online)
                         {
                             // SignalR is back. The handshake rides the new data stream rather
                             // than the control plane, so there is nothing to do here but arm it
                             // and let Streaming open that stream.
                             _reconnectionHandler.PrepareForHandshake();
                             _fsm.TransitionTo(SessionState.Streaming);
                         }
                         else
                         {
                             // Wait before retrying probe
                             await Task.Delay(2000, token);
                         }
                         break;

                    default:
                         // The domino states are not a resting place: what ends a transfer is the
                         // hub closing these streams, so they have to stay open until it does.
                         // Sleeping here instead left a receiver whose control stream failed just
                         // after it accepted Complete sitting on an acknowledgement it would never
                         // re-send, and a sender retrying a transfer that was in fact delivered.
                         if (IsInCompletingPhase)
                         {
                             await RunStreamsUntilOneEndsAsync(token);
                         }
                         else
                         {
                             await Task.Delay(100, token);
                         }
                         break;
                }
            }
            catch (Exception ex)
            {
                 _config.Logger.LogError(ex, "Session Loop Error");
                 await Task.Delay(1000, token);
            }
        }
        
        _config.Logger.LogInformation("Session Loop ENDED. FinalState={State}", _fsm.CurrentState);
    }

    /// <summary>
    /// Opens the session's streams and waits for the first of them to end, translating that
    /// outcome into the next state. Returns as soon as one stream is done; the caller's loop
    /// decides whether to open them again.
    /// </summary>
    private async Task RunStreamsUntilOneEndsAsync(CancellationToken token)
    {
        _httpStreamManager.StartStreams(IsSender, WriteLoop, ReadLoop, token);
        
        // Wait for ANY stream to finish (returns on failure or completion)
        // If one fails, HttpStreamManager returns result of failing task.
        var senderTask = _httpStreamManager.SenderTask;
        var receiverTask = _httpStreamManager.ReceiverTask;
        
        var activeTasks = new List<Task<HttpStreamManager.StreamResult>>();
        if(senderTask != null) activeTasks.Add(senderTask);
        if(receiverTask != null) activeTasks.Add(receiverTask);
        
        if (activeTasks.Count > 0)
        {
            var finished = await Task.WhenAny(activeTasks);
            var result = await finished;
            
            if (!result.IsSuccess)
            {
                if (result.IsRetryable)
                {
                    _config.Logger.LogWarning("Stream failed (Retryable): {Error}. Transitioning to Reconnecting.", result.ErrorMessage);
                    if (!IsInCompletingPhase)
                    {
                        _fsm.TransitionTo(SessionState.Reconnecting);
                    }
                    else
                    {
                        _config.Logger.LogInformation("Retrying stream connection while in completing phase.");
                        await Task.Delay(CompletingPhaseRetryMs, token);
                    }
                }
                else
                {
                    _config.Logger.LogError("Stream failed (Fatal): {Error}. Sessions FAILED.", result.ErrorMessage);
                    // If we are in completing phase and the server says "Transfer Gone" (404),
                    // it means the transfer finished successfully and was cleaned up before we could reconnect.
                    // This is a valid "Success" signal in the completing phase.
                    if (IsInCompletingPhase && 
                        result.ErrorMessage != null && 
                        result.ErrorMessage.Contains("Transfer Gone"))
                    {
                        _config.Logger.LogInformation("Transfer Gone (404) received while in completing phase. Transfer confirmed Completed.");
                        _fsm.TransitionTo(SessionState.Completed);
                    }
                    else
                    {
                        _fsm.TransitionTo(SessionState.Failed);
                    }
                }
            }
            else
            {
                // A graceful close means only that the stream ended, not that
                // the transfer did: HttpStreamManager reports success whenever a
                // read or write loop finishes without throwing, and a dropped
                // connection can end one that way. What the two cases are told
                // apart by is the phase, below — in the completing phase the
                // close is the expected end, outside it the peer went away and
                // we reconnect. A 404 does not arrive here at all; it is fatal
                // and handled in the branch above.
                _config.Logger.LogInformation("Stream finished successfully. Re-evaluating state.");
                
                // Already Completed means the domino sequence finished before
                // this stream unwound; there is nothing left to decide.
                if (_fsm.CurrentState != SessionState.Completed)
                {
                    if (IsInCompletingPhase)
                    {
                        _config.Logger.LogInformation("Stream finished while in completing phase (Domino effect). Transfer confirmed Completed.");
                        _fsm.TransitionTo(SessionState.Completed);
                    }
                    else
                    {
                        _fsm.TransitionTo(SessionState.Reconnecting);
                    }
                }
            }
        }
    }


    private async Task WriteLoop(Stream outputStream, CancellationToken token)
    {
        var transferGuid = Guid.Parse(TransferId);
        
        if (IsSender)
        {
            // Every stream starts with the handshake, new or resumed alike. Doing it only on a
            // flagged reconnection would make the sender's idea of where the receiver stands
            // depend on the client having noticed the cut, which is exactly what it cannot be
            // trusted to know. Returns -1 on timeout, otherwise the receiver's NextExpectedId.
            int nextExpectedFromPong = await _reconnectionHandler.ExecuteHandshakeAsync(outputStream, transferGuid, token);
            bool didPingPong = nextExpectedFromPong >= 0;
            
            // Step 2: Resend unacked batches (targeted if ping/pong succeeded)
            await _batchProcessor.ResendUnackedBatchesAsync(outputStream, transferGuid, didPingPong, nextExpectedFromPong, token);
        }
        else
        {
            // Receiver Logic: If we are reconnecting in completing phase, we MUST re-send the CompleteAck.
            // The Sender is likely waiting for it. The previous attempt might have been lost.
            if (IsInCompletingPhase)
            {
                _config.Logger.LogInformation("Reconnected receiver stream while in completing phase. Resending CompleteAck to unblock Sender.");
                // We fire and forget this into the channel, ensuring it's the first thing sent.
                await _batchProcessor.ResendCompleteAckAsync();
            }
        }
        
        // Step 3: Process send channel (normal operation)
        await ProcessSendChannelAsync(outputStream, transferGuid, token);
    }






    /// <summary>
    /// Processes the send channel, writing batches and control messages to the output stream.
    /// Uses robust batching: drains all available messages, then flushes once.
    /// </summary>
    private async Task ProcessSendChannelAsync(Stream outputStream, Guid transferGuid, CancellationToken token)
    {
        while (await _batchProcessor.OutgoingMessages.WaitToReadAsync(token))
        {
            while (_batchProcessor.OutgoingMessages.TryRead(out var message))
            {
                if (message is BatchControlMessage batchMsg)
                {
                    // Attempt to retain the payload. 
                    // If it returns null, it means the batch was already ACKed and disposed (Race condition avoided).
                    var memoryOwner = batchMsg.Entry.TryRetain();
                    
                    try 
                    {
                        // If retained successfully for THIS operation
                        if (memoryOwner != null)
                        {
                             var payloadMem = memoryOwner.Memory;
                             var count = BinaryPrimitives.ReadInt32LittleEndian(payloadMem.Span[..4]);
                             var body = payloadMem[4..];
                             await ChunkIO.WriteBatchAsync(outputStream, transferGuid, batchMsg.BatchId, count, body, token);
                             _batchProcessor.CountPayloadBytes(payloadMem.Length);
                        }
                    }
                    finally
                    {
                        // Release the reference we took with TryRetain()
                        if (memoryOwner != null)
                        {
                            batchMsg.Entry.Release();
                        }

                        // Release the reference held by the Message itself (passed from BatchProcessor)
                        batchMsg.Entry.Release();
                    }
                }
                else if (message is ControlChunkMessage ctrlMsg)
                {
                    await ChunkIO.WriteControlAsync(outputStream, transferGuid, ctrlMsg.BatchId, ctrlMsg.Type, token);

                    // Ending the control POST is the receiver's step in the domino, and it belongs
                    // here rather than at the moment Complete was accepted: EOF after the
                    // acknowledgement means the hub read it, EOF before it was ever written means
                    // the sender waits forever. A stream that fails instead is reopened, and the
                    // acknowledgement goes out again on the new one.
                    if (!IsSender && ctrlMsg.Type == ChunkType.CompleteAck)
                    {
                        await outputStream.FlushAsync(token);
                        return;
                    }
                }
            }
            
            await outputStream.FlushAsync(token);
        }
    }

    private async Task ReadLoop(Stream inputStream, CancellationToken token)
    {
        // If we already received Complete, do not restart reading (prevents hanging on retry)
        if (_fsm.CurrentState == SessionState.Completed)
        {
            _config.Logger.LogInformation("ReadLoop started but Transfer={TransferId} is already completed. Exiting.", TransferId);
            return;
        }

        try
        {
            while (!token.IsCancellationRequested)
            {
                using var chunkData = await ChunkIO.ReadChunkAsync(inputStream, token);
                if (chunkData == null)
                {
                    // If stream ends but we haven't received Complete, it's an error (premature disconnect)
                    // If we ARE in completing phase, receiving EOF is the expected confirmation (Ack of Ack).
                    if (_fsm.CurrentState != SessionState.Completed && !IsInCompletingPhase && !token.IsCancellationRequested)
                    {
                         throw new IOException("Stream ended unexpectedly (Premature EOF)");
                    }
                    break;
                }

                await _batchProcessor.ProcessChunkAsync(chunkData.Value, token);
                
                // Exit when entering completing phase (receiver got Complete chunk) or fully completed
                if (IsInCompletingPhase || IsTerminal)
                {
                    _config.Logger.LogInformation("Entering completing phase - exiting ReadLoop for Transfer={TransferId}", TransferId);
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            if (!_cts.IsCancellationRequested)
            {
                _config.Logger.LogDebug(ex, "ReadLoop interrupted for Transfer={TransferId}. Rethrowing for retry.", TransferId);
                throw; // Propagate to HttpStreamManager to trigger retry
            }
        }
        finally
        {
            if (IsInCompletingPhase || IsTerminal || _cts.IsCancellationRequested)
            {
                _batchProcessor.CompleteReceiver();
            }
        }
    }





    #endregion

    #region Heartbeat

    private async void HeartbeatLoop(object? state)
    {
        if (_cts.IsCancellationRequested) return;

        // A per-transfer heartbeat recreates the hub-side TransferState it reports against, so one
        // that outlives its transfer resurrects a finished transfer every tick — and, behind a
        // shared state store, renews its expiry forever. Once this session has nothing left to say
        // about the transfer, it says nothing.
        if (IsStopped || IsInCompletingPhase)
        {
            _heartbeatTimer.Change(Timeout.Infinite, Timeout.Infinite);
            return;
        }

        try 
        {
            // Only send Heartbeat if Hub Connection is active
            if (_signalR.HubConnection.State == HubConnectionState.Connected)
            {
                // If Sender
                if (IsSender) 
                {
                    var items = _batchProcessor.ItemsSent;
                    var acks = _batchProcessor.AcksReceived;
                    await _signalR.HubConnection.InvokeAsync(nameof(ICommandHub.SenderHeartbeat), TransferId, items, acks);
                }
                
                // If Receiver
                if (!IsSender)
                {
                    var items = _batchProcessor.ItemsReceived;
                    var acks = _batchProcessor.AcksSent;
                    await _signalR.HubConnection.InvokeAsync(nameof(ICommandHub.ReceiverHeartbeat), TransferId, items, acks);
                }
            }
        }
        catch(Exception ex)
        {
            _config.Logger.LogError(ex, "Failed to send Heartbeat for {TransferId}", TransferId);
        }
    }

    #endregion
}




