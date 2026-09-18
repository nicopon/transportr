using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Threading.Channels;
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
/// Handles bidirectional HTTP streams, batch serialization, ACK/NACK, and reconnection.
/// </summary>
internal class DataSession<T> : IDataSession<T>
{
    public string TransferId { get; }
    public int BatchSize { get; }
    public string DataUrl { get; }
    
    private readonly ISignalRInteraction _signalR;
    private readonly ISessionConfig _config;
    private readonly BatchProtocol<T> _protocol;
    
    private readonly CancellationTokenSource _cts;
    private readonly HttpStreamManager _httpStreamManager;
    private readonly BatchProcessor<T> _batchProcessor;
    private readonly ReconnectionHandler<T> _reconnectionHandler;
    private readonly DataSessionStateMachine _fsm;
    private bool _isSender;

    private readonly Timer _heartbeatTimer;
    
    // Session Statistics
    public DateTime StartedAt { get; } = DateTime.UtcNow;
    
    // Public properties for GetTransfers()
    public bool IsSender => _isSender;
    public bool IsCompleted => _fsm.CurrentState == SessionState.Completed || _fsm.CurrentState == SessionState.Failed;
    
    /// <summary>
    /// Returns true if the session is in any of the completing/domino states (sender or receiver).
    /// Used for logic that should apply during the completion sequence.
    /// </summary>
    private bool IsInCompletingPhase => _fsm.CurrentState switch
    {
        SessionState.SenderCompleteSent => true,
        SessionState.SenderAckReceived => true,
        SessionState.SenderTerminatingDataPost => true,
        SessionState.SenderAwaitingControlGetEOF => true,
        SessionState.ReceiverCompleteReceived => true,
        SessionState.ReceiverAckSent => true,
        SessionState.ReceiverAwaitingDataGetEOF => true,
        SessionState.ReceiverTerminatingControlPost => true,
        _ => false
    };
    
    public long ItemsProcessed => IsSender ? _batchProcessor.ItemsSent : _batchProcessor.ItemsReceived;
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
        _protocol = new BatchProtocol<T>(context.Serializer);
        
        _cts = new CancellationTokenSource();
        _batchProcessor = new BatchProcessor<T>(context, transferId, batchSize, dataUrl);
        _fsm = new DataSessionStateMachine(transferId, context.Logger);
        
        // Pass segregated interfaces to HttpStreamManager
        _httpStreamManager = new HttpStreamManager(context, context, transferId, dataUrl); // IStreamTransport, ISessionConfig
        
        _reconnectionHandler = new ReconnectionHandler<T>(context, transferId);
            
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
        _batchProcessor.ResetCompleteAck();
        
        const int completeRetryMs = 5000;
        
        // Retry indefinitely until Ack or Shutdown
        while (_fsm.CurrentState != SessionState.Completed && _fsm.CurrentState != SessionState.Failed)
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            await _batchProcessor.SendCompleteSignalAsync(cancellationToken);
            
            // Wait for CompleteAck
            var ackTask = _batchProcessor.WaitForCompleteAckAsync(cancellationToken);
            var delayTask = Task.Delay(completeRetryMs, cancellationToken);
            
            if (await Task.WhenAny(ackTask, delayTask) == ackTask)
            {
                _config.Logger.LogInformation("CompleteAck received - transfer complete (Transfer={TransferId})", TransferId);
                _fsm.TransitionTo(SessionState.Completed);
                
                // Allow WriteLoop to exit
                _batchProcessor.CompleteSenderChannel();

                // No need to call TransferComplete - hub detects completion via dataplane EOF
                return;
            }
            
            _config.Logger.LogWarning("Complete not acknowledged, retrying... (Transfer={TransferId})", TransferId);
        }
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
        if (IsCompleted) return;
        
        // For receivers: completing phase means all data received + CompleteAck sent
        // We don't need to wait for the full domino sequence
        // For senders: we wait for actual Completed state (got CompleteAck)
        while (!IsCompleted)
        {
            // Receiver can exit when in completing phase (all data received)
            if (!_isSender && IsInCompletingPhase)
            {
                _config.Logger.LogInformation("Receiver in completing phase with all data received. Exiting wait.");
                break;
            }
            await Task.Delay(100, cancellationToken);
        }
    }

    public async ValueTask DisposeAsync()
    {
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
                        // Execute Streams (Active Mode)
                        _fsm.TransitionTo(SessionState.Streaming);
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
                                // Success completion (stream ended normally)
                                // If receiver, and 204/Success -> Completed?
                                // Let's check logic:
                                // ReadLoop returns on 404/204 in HttpStreamManager.
                                // If 404 -> HttpStreamManager returns Fatal "Transfer Gone".
                                // If 204 -> Success? 
                                // Actually HttpStreamManager returns Success on normal finish.
                                // If stream ends naturally, it's success.
                                
                                // For now, if stream ends and we are not Completed, it might be a disconnect?
                                // HttpStreamManager returns IsRetryable=true on exception.
                                // IsSuccess=true means graceful close.
                                // Graceful close during Streaming means... connection drop? Or finish?
                                
                                // If we are not in Completing state, graceful close is weird unless it's EOF.
                                // We treat as Reconnecting if not Fatal.
                                
                                // Wait, HttpStreamManager returns Success only if "isRead" loop finishes or "isWrite" finish.
                                _config.Logger.LogInformation("Stream finished successfully. Re-evaluating state.");
                                
                                // If we are just starting, maybe verify?
                                if (_fsm.CurrentState != SessionState.Completed)
                                {
                                    if (IsInCompletingPhase)
                                    {
                                        _config.Logger.LogInformation("Stream finished while in completing phase (Domino effect). Transfer confirmed Completed.");
                                        _fsm.TransitionTo(SessionState.Completed);
                                    }
                                    else
                                    {
                                        if (!IsInCompletingPhase)
                                        {
                                            _fsm.TransitionTo(SessionState.Reconnecting);
                                        }
                                        else
                                        {
                                            _config.Logger.LogInformation("Stream closed cleanly while in completing phase (Retrying handshake).");
                                        }
                                    }
                                }
                            }
                        }
                        break;

                    case SessionState.Reconnecting:
                         // Execute Handshake
                         var online = await _reconnectionHandler.ProbeConnectivityAsync(token);
                         if (online)
                         {
                             // SignalR is back. Is Handshake needed?
                             // Handshake is done via the NEW stream.
                             // So we just transition to Streaming, but with "PrepareForHandshake" flag?
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
                         await Task.Delay(100, token);
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


    private async Task WriteLoop(Stream outputStream, CancellationToken token)
    {
        var transferGuid = Guid.Parse(TransferId);
        
        if (IsSender)
        {
            // Step 1: Execute Handshake IF we are reconnecting (or new stream)
            // Since we always start a new stream in "Streaming", we should always ping/pong?
            // Or only if we flagged it?
            // _reconnectionHandler tracks the "PongReceived" TCS.
            // It returns -1 if timeout/fail, or NextExpectedID.
            
            // We always try handshake to be safe and sync with receiver
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
                    // If stream ends but we haven't received Complete, it's an error (premature disconnect)
                    // If we ARE in completing phase, receiving EOF is the expected confirmation (Ack of Ack).
                    if (_fsm.CurrentState != SessionState.Completed && !IsInCompletingPhase && !token.IsCancellationRequested)
                    {
                         throw new IOException("Stream ended unexpectedly (Premature EOF)");
                    }
                    break;
                }

                await _batchProcessor.ProcessChunkAsync(chunkData.Value);
                
                // Exit when entering completing phase (receiver got Complete chunk) or fully completed
                if (IsInCompletingPhase || IsCompleted)
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
            if (IsInCompletingPhase || IsCompleted || _cts.IsCancellationRequested)
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
                    // Note: BatchProcessor doesn't expose _lastAckedBatchId in public API, only via stats?
                    // We can track it or expose it.
                    // Let's assume NextExpectedBatchId - 1 is last acked for simplicity or expose it.
                    // ReceiverState tracks NextExpected. LastAcked is close.
                    // BatchProcessor doesn't expose it.
                    // Let's assume items count is rough proxy?
                    // Or add LastAckedBatchId to BatchProcessor public API.
                    await _signalR.HubConnection.InvokeAsync(nameof(ICommandHub.ReceiverHeartbeat), TransferId, items, acks, -1);
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




