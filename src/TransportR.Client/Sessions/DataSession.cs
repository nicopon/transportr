using System.Net;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using TransportR.Abstractions;
using TransportR.Interfaces;
using TransportR.Protocol;
using Microsoft.Extensions.Logging;

using TransportR.Client.Batching;
using TransportR.Client.Internal;
using TransportR.Models;
using TransportR.Services;

namespace TransportR.Client.Sessions;

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

    // Bounds how long DisposeAsync waits for Abort to reach the wire before tearing the streams
    // down anyway. A stream stuck on backpressure must not hang disposal.
    private static readonly TimeSpan AbortWriteTimeout = TimeSpan.FromSeconds(2);

    // Bounds the request that tells the hub when the Abort could not be written.
    private static readonly TimeSpan AbortRequestTimeout = TimeSpan.FromSeconds(2);

    // Bounds a heartbeat, so that one sent to a hub out of reach gives way to the next.
    private static readonly TimeSpan HeartbeatTimeout = TimeSpan.FromSeconds(5);

    private const string ReleasedBeforeCompletion =
        "The transfer handle was released before the transfer completed; the transfer was abandoned.";

    private const string EndedByHub = "The hub ended the transfer before it completed.";

    public string TransferId { get; }
    public int BatchSize { get; }
    public string DataUrl { get; }
    
    private readonly IControlPlane _control;
    private readonly string? _affinity;
    private readonly ISessionConfig _config;
    
    private readonly CancellationTokenSource _cts;
    private readonly HttpStreamManager _httpStreamManager;
    private readonly BatchProcessor<T> _batchProcessor;
    private readonly ReconnectionHandler _reconnectionHandler;
    private readonly DataSessionStateMachine _fsm;
    private bool _isSender;

    /// <summary>
    /// Set when the transfer is failed from outside any call - a peer's Abort, a stream the hub
    /// refuses for good - and read from another thread by whatever call that unblocks (SendAsync,
    /// CompleteAsync, WaitForCompletionAsync).
    /// </summary>
    private volatile string? _failureReason;

    /// <summary>
    /// Set once this side has put an Abort on its outgoing channel, so that disposal knows to give
    /// it time to reach the wire.
    /// </summary>
    private volatile bool _abortQueued;

    private readonly Timer _heartbeatTimer;
    private int _heartbeating;
    private int _disposed;
    private int _writeSideEndDelivered;

    // Session Statistics
    public DateTime StartedAt { get; } = DateTime.UtcNow;
    
    // Public properties for GetTransfers()
    public bool IsSender => _isSender;
    public SessionState State => _fsm.CurrentState;
    public WriteTransport? WriteTransport => _httpStreamManager.WriteTransport;
    public string? WriteTransportFallback => _httpStreamManager.WriteTransportFallback;

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
    
    /// <param name="affinity">
    /// The cookies that reach the hub instance holding the transfer's bridge, which every request of
    /// the session carries: its streams, its heartbeats, its probe and its abort.
    /// </param>
    public DataSession(ISessionContext<T> context, string transferId, int batchSize, string dataUrl, string? affinity)
    {
        _control = context;
        _affinity = affinity;
        _config = context; // ISessionContext implements ISessionConfig
        
        TransferId = transferId;
        BatchSize = batchSize;
        DataUrl = dataUrl.TrimEnd('/');
        
        _cts = new CancellationTokenSource();
        _batchProcessor = new BatchProcessor<T>(context, transferId, batchSize);
        _fsm = new DataSessionStateMachine(transferId, context.Logger);
        
        // Pass segregated interfaces to HttpStreamManager
        _httpStreamManager = new HttpStreamManager(context, context, transferId, dataUrl, affinity); // IStreamTransport, ISessionConfig
        
        _reconnectionHandler = new ReconnectionHandler(context, transferId, affinity);
            
        // Hook up events
        _batchProcessor.OnPongReceived += (id) => {
            _reconnectionHandler.HandleStreamPong(id);
        };
        _batchProcessor.OnCompleteReceived += () => {
            // Not Completed yet: the acknowledgement is only queued, and the session has to stay up
            // until the hub has read it.
            _fsm.TransitionTo(SessionState.ReceiverCompleteReceived);
            return Task.CompletedTask;
        };
        _batchProcessor.OnAbortReceived += (reason) => {
            _failureReason = reason.Describe();
            if (_fsm.OnAborted())
            {
                _config.Logger.LogWarning(
                    "Received Abort ({Reason}) - transfer failed (Transfer={TransferId})", reason, TransferId);
            }
        };
        
        _batchProcessor.OnSerializationFailed += (ex) => {
            if (IsStopped) return;

            FailTransfer($"A batch could not be serialized: {ex.Message}");

            // The receiver holds every batch up to this one and nothing would tell it the rest is not
            // coming. Abort precedes completing the channel, as on the stall bound.
            AbortPeer(AbortReason.SerializationFailed);
            _batchProcessor.CompleteSenderChannel();
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

    public Task SendAsync(T item, CancellationToken cancellationToken)
    {
        var send = _batchProcessor.SendAsync(item, cancellationToken);
        // IsCompletedSuccessfully, not IsCompleted: a call that lost the race against an Abort or a
        // dispose can fault before this is even checked, and that path must still go through
        // WhileAcknowledgingAsync to be translated into TransferFailedException.
        return send.IsCompletedSuccessfully ? send : WhileAcknowledgingAsync(send, cancellationToken);
    }

    public Task SendRangeAsync(IEnumerable<T> items, CancellationToken cancellationToken)
    {
        var send = _batchProcessor.SendRangeAsync(items, cancellationToken);
        return send.IsCompletedSuccessfully ? send : WhileAcknowledgingAsync(send, cancellationToken);
    }

    /// <summary>
    /// Waits out a producer parked on a full send window, for as long as the receiver keeps
    /// acknowledging.
    /// </summary>
    /// <remarks>
    /// Only an acknowledgement reopens the window, and a receiver that has gone away sends none: a
    /// consumer that stopped early leaves its producer parked forever. The bound is the one
    /// <see cref="CompleteAsync"/> applies — a peer still acknowledging is still working, however
    /// slowly; one that acknowledged nothing for <c>CompleteStallTimeout</c> is gone.
    /// </remarks>
    private async Task WhileAcknowledgingAsync(Task send, CancellationToken cancellationToken)
    {
        var stallTimeout = _config.Options.CompleteStallTimeout;
        var parkedAtMs = Environment.TickCount64;

        while (!send.IsCompleted)
        {
            var silence = AcknowledgementSilence(parkedAtMs);
            if (silence >= stallTimeout)
            {
                FailOnAcknowledgementStall();

                // The parked producer unwinds with a cancellation; the failure is reported below.
                try { await send; } catch (OperationCanceledException) { }

                throw new TransferFailedException(
                    TransferId,
                    $"The send window stayed full: nothing was acknowledged for {silence.TotalSeconds:N0}s.");
            }

            // Wakes when an acknowledgement reopens the window, or when the receiver will have been
            // silent for the whole bound: no moment in between can change the verdict.
            await send.WaitAsync(stallTimeout - silence, cancellationToken)
                .ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

            if (!send.IsCompleted) cancellationToken.ThrowIfCancellationRequested();
        }

        await AwaitOrTranslateAsync(send, cancellationToken);
    }

    /// <summary>
    /// How long the receiver has acknowledged nothing, counted from <paramref name="sinceMs"/>
    /// (<see cref="Environment.TickCount64"/>) at the earliest. The one clock for both waits that only
    /// an acknowledgement ends, a producer on a full window and <see cref="CompleteAsync"/>.
    /// </summary>
    private TimeSpan AcknowledgementSilence(long sinceMs) =>
        TimeSpan.FromMilliseconds(Environment.TickCount64 - Math.Max(sinceMs, _batchProcessor.LastAckAtMs));

    /// <summary>
    /// Fails a transfer whose receiver stopped acknowledging. The Abort is queued before the channel
    /// completes, as <see cref="AbortPeer"/> requires, and a producer still parked on the window is let go.
    /// </summary>
    private void FailOnAcknowledgementStall()
    {
        _fsm.TransitionTo(SessionState.Failed);
        AbortPeer(AbortReason.AcknowledgementTimeout);
        _batchProcessor.CompleteSenderChannel();
        _batchProcessor.AbandonSender();
    }

    /// <summary>
    /// Awaits a producer task, translating what a transfer that has ended does to it - an Abort or
    /// DisposeAsync abandoning the accumulator, which cancels a parked call and closes the queue under
    /// a later one - into <see cref="TransferFailedException"/>. A cancellation the caller's own token
    /// carries is left alone: that one was asked for.
    /// </summary>
    private async Task AwaitOrTranslateAsync(Task task, CancellationToken callerToken)
    {
        try
        {
            await task;
        }
        catch (OperationCanceledException) when (!callerToken.IsCancellationRequested)
        {
            throw new TransferFailedException(TransferId, FailureMessage);
        }
        catch (Exception ex) when (ex is ChannelClosedException or ObjectDisposedException
            && _fsm.CurrentState is SessionState.Failed or SessionState.Disposed)
        {
            throw new TransferFailedException(TransferId, FailureMessage);
        }
    }

    private string FailureMessage =>
        _failureReason
        ?? (_fsm.CurrentState == SessionState.Disposed ? ReleasedBeforeCompletion : "Transfer failed before it could complete.");

    public async Task CompleteAsync(CancellationToken cancellationToken = default)
    {
        // A released session has nothing left to complete, and draining an accumulator that is
        // already gone raises a disposal error the caller can do nothing with.
        if (_fsm.CurrentState == SessionState.Disposed)
        {
            throw new TransferFailedException(TransferId, ReleasedBeforeCompletion);
        }

        // The partial batch handed off here takes a slot like any other, so a full window parks this
        // call as it parks SendAsync, and under the same bound.
        await WhileAcknowledgingAsync(_batchProcessor.CompleteSenderAsync(cancellationToken), cancellationToken);

        // Complete goes out with batches still in flight rather than after them: the receiver refuses
        // it while one is missing, and the loop below sends it again until the tail is in.
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
        // loop is therefore the acknowledgements, not the number of attempts: a peer still
        // acknowledging is still working, one that has gone away stops sending them.
        var startedAtMs = Environment.TickCount64;

        while (!IsStopped)
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            await _batchProcessor.SendCompleteSignalAsync();
            
            // Wait for CompleteAck
            var ackTask = _batchProcessor.WaitForCompleteAckAsync(cancellationToken);
            var delayTask = Task.Delay(retryInterval, cancellationToken);
            
            var finished = await Task.WhenAny(ackTask, delayTask, _fsm.Stopped);

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

                // The hub learns the transfer is over from the end of the data stream, which the
                // completed channel above lets the write loop produce.
                return;
            }

            // The session stopped under the wait (the hub ended the transfer, the peer aborted): what
            // follows the loop says why, and a retry of Complete would be sent to nobody.
            if (IsStopped) break;

            var silence = AcknowledgementSilence(startedAtMs);
            if (silence >= stallTimeout)
            {
                FailOnAcknowledgementStall();
                throw new TransferFailedException(
                    TransferId,
                    $"Transfer did not complete: nothing was acknowledged for {silence.TotalSeconds:N0}s.");
            }

            if (silence < retryInterval)
            {
                _config.Logger.LogInformation(
                    "Complete not acknowledged yet, {Unacked} batches still in flight - retrying (Transfer={TransferId})",
                    _batchProcessor.UnackedCount, TransferId);
            }
            else
            {
                _config.Logger.LogWarning(
                    "Complete not acknowledged and nothing acknowledged for {Stalled:N0}s (Transfer={TransferId})",
                    silence.TotalSeconds, TransferId);
            }
        }

        // Only reachable when something else stopped the session under us.
        throw new TransferFailedException(TransferId, FailureMessage);
    }
    
    public async IAsyncEnumerable<T> ReceiveAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var item in _batchProcessor.ReceiveAsync(cancellationToken))
        {
            yield return item;
        }

        // The last item is not the last word: the acknowledgement is still queued behind it, and a
        // caller that stops here as soon as items stop arriving can abandon a transfer it in fact
        // received in full.
        await WaitForCompletionAsync(cancellationToken);
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
             _config.Logger.LogInformation("The control stream was lost while in completing phase. Cancelling streams to force verification.");
             // Do NOT transition to Reconnecting (preserve completing state logic)
             _httpStreamManager.CancelStreams();
        }
    }


    public async Task WaitForCompletionAsync(CancellationToken cancellationToken)
    {
        // Both roles wait for the domino to close the session. Having accepted Complete is not
        // enough for a receiver: its acknowledgement is queued at that point, and a queued chunk is
        // not a delivered one — the stream carrying it can fail, and the resend that follows needs
        // the session the caller is about to release. What says the acknowledgement arrived is the
        // hub ending this session's streams, which is the terminal state itself.
        await Task.WhenAny(_fsm.Completing, _fsm.Stopped).WaitAsync(cancellationToken);

        if (!IsStopped)
        {
            // Only the ending is bounded, never the transfer: before the completing phase the
            // peer is still working and the caller's token is the only clock that applies.
            try
            {
                await _fsm.Stopped.WaitAsync(_config.Options.CompleteStallTimeout, cancellationToken);
            }
            catch (TimeoutException)
            {
                throw new TransferFailedException(
                    TransferId,
                    "All data was received, but the transfer never finished closing; the sender cannot know it completed.");
            }
        }

        if (_fsm.CurrentState is SessionState.Disposed or SessionState.Failed)
        {
            throw new TransferFailedException(TransferId, FailureMessage);
        }
    }

    /// <summary>
    /// Fails a transfer this side cannot finish and releases whoever is parked on it. Failing the
    /// state alone leaves them waiting: nothing else wakes a consumer on an empty buffer or a producer
    /// on a full window, and the contract has the consumer ask for the outcome only after the
    /// enumeration ends.
    /// </summary>
    private void FailTransfer(string reason)
    {
        // A session that already stopped keeps the reason it stopped for. The reason is set before the
        // state moves: whoever polls the state, or is woken by it, reads the reason next.
        if (IsStopped) return;

        _failureReason ??= reason;
        if (_fsm.TransitionTo(SessionState.Failed))
        {
            _batchProcessor.Fail(reason);
        }
    }

    /// <summary>
    /// Tells the peer this side is giving up. Must precede completing the send channel: the write
    /// loop ends the data stream cleanly when the channel completes, and a clean end with nothing
    /// before it is what the hub relays as a finished transfer.
    /// </summary>
    private void AbortPeer(AbortReason reason)
    {
        _batchProcessor.EnqueueAbort(reason);
        _abortQueued = true;
    }

    public async Task EndByHubAsync()
    {
        // A session that already stopped keeps the reason it stopped for. The reason is set before the
        // state moves: whoever polls the state reads the reason next.
        if (!IsStopped)
        {
            _failureReason ??= EndedByHub;
            if (_fsm.OnEndedByHub())
            {
                _batchProcessor.Fail(EndedByHub);
            }
        }

        // Failed, the session is past OnReleased: nothing is queued to the peer, which the hub told itself.
        await DisposeAsync();
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

            AbortPeer(AbortReason.ReleasedBeforeCompletion);
        }

        // Best-effort: give the peer a reason to fail on instead of a silence it can only read as a
        // stall. Bounded because a stream stuck on backpressure must not hang disposal, and
        // DisposeAsync never throws. Waits on the end of the write direction, which the hub
        // answers once it has relayed what came before and closed the stream: cancelling the
        // streams sooner can leave the Abort unsent, and turns the clean close into a rupture the
        // hub waits out.
        //
        // A session that completed waits too. CompleteAsync returns on the acknowledgement, and the
        // clean end of the data stream, which is how the hub learns the transfer is over and forgets
        // it, only starts then; a release straight after loses that race most of the time on a
        // loaded machine.
        if (_abortQueued || _fsm.CurrentState == SessionState.Completed)
        {
            await DeliverWriteSideEndAsync();
        }

        // A receiver completes when the hub has read its acknowledgement, which the sender may not have
        // read yet. The hub ends the receiver's data GET once the sender's data stream has ended cleanly,
        // and a GET cut before that takes that stream down with it: the sender then loses the
        // acknowledgement with its streams, and nobody sends it again.
        if (!IsSender && _fsm.CurrentState == SessionState.Completed && _httpStreamManager.ReadTask is { } readTask)
        {
            try
            {
                await readTask.WaitAsync(HttpStreamManager.ReadSideEndTimeout);
            }
            catch (TimeoutException)
            {
                _config.Logger.LogDebug("The hub had not ended the data stream of Transfer={TransferId} when it was released", TransferId);
            }
        }

        _reconnectionHandler.Dispose();
        await _batchProcessor.DisposeAsync();
        await _httpStreamManager.DisposeAsync();
        _cts.Cancel(); // Stop all loops immediately, do not wait for 5s of retries!


        _heartbeatTimer.Dispose();
        _cts.Dispose();
        _control.RemoveSession(TransferId);
    }

    /// <summary>
    /// Waits, bounded, for the end of the write direction to be answered, and falls back on the hub's abort
    /// route when an Abort is queued and that end does not confirm it. Runs once: the read loop calls it
    /// before failing on a batch it cannot decode, and the release that follows has nothing to add.
    /// </summary>
    private async Task DeliverWriteSideEndAsync()
    {
        if (Interlocked.Exchange(ref _writeSideEndDelivered, 1) != 0) return;

        var abortUnconfirmed = false;
        try
        {
            if (_httpStreamManager.WriteTask is { } writeTask)
            {
                abortUnconfirmed = !AbortConfirmed(await writeTask.WaitAsync(AbortWriteTimeout));
            }
        }
        catch (Exception ex)
        {
            abortUnconfirmed = true;
            _config.Logger.LogDebug(ex, "Could not confirm Abort was written for Transfer={TransferId}", TransferId);
        }

        // An Abort queued behind data nobody reads cannot overtake it, and cancelling the stream
        // afterwards then looks to the hub like a cut it should wait out. Only a request of its own
        // gets through a stream that is full, so the hub is told that way.
        if (abortUnconfirmed && _abortQueued)
        {
            await _httpStreamManager.RequestAbortAsync(AbortRequestTimeout);
        }
    }

    /// <summary>
    /// Whether the write direction's end says the Abort is no longer this side's to deliver: the
    /// end was answered, which means the hub relayed the stream to its end, or the hub refused
    /// the stream for good, which means it has no such transfer to tell. A cut or a cancelled
    /// write says neither.
    /// </summary>
    private static bool AbortConfirmed(HttpStreamManager.StreamResult written) =>
        written.Outcome is StreamOutcome.Ended or StreamOutcome.Gone or StreamOutcome.Fatal;

    #endregion

    #region Streams Management

    private async Task RunSessionLoop(CancellationToken token)
    {
        _config.Logger.LogInformation("Session Loop {TransferId} Starting. IsSender={IsSender}", TransferId, IsSender);

        while (!token.IsCancellationRequested && !IsStopped)
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
                             // The hub answers. The handshake rides the new data stream rather
                             // than the control plane, so there is nothing to do here but arm it
                             // and let Streaming open that stream. A receiver that accepted Complete
                             // in the meantime stays closing, and reopens its streams from there.
                             _reconnectionHandler.PrepareForHandshake();
                             _fsm.TransitionTo(SessionState.Streaming);
                         }
                         else
                         {
                             // Wait before retrying probe
                             await Task.Delay(2000, token);
                         }
                         break;

                    case SessionState.ReceiverCompleteReceived:
                         // The closing phase is not a resting place: what ends a transfer is the
                         // hub reading the acknowledgement, so the streams have to stay open until
                         // it does. A receiver whose control stream failed just after it accepted
                         // Complete would otherwise sit on an acknowledgement it never re-sends, and
                         // its sender retry a transfer that was in fact delivered.
                         await RunStreamsUntilOneEndsAsync(token);
                         break;

                    default:
                         // Idle, which the session leaves before this loop starts: there is nothing
                         // to run, and waiting beats spinning.
                         await _fsm.Stopped.WaitAsync(token);
                         break;
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                // DisposeAsync cancels the loop's own token last: that is the loop ending, not failing.
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
            // A receiver's closing phase ends with its control stream, answered once the hub has read the
            // acknowledgement. Its data GET is read on until the hub ends it, after the sender's data
            // stream, so the attempt as a whole ends a step later than the transfer does.
            if (receiverTask != null && _httpStreamManager.WriteTask is { } writeTask
                && await Task.WhenAny(receiverTask, writeTask) == writeTask
                && writeTask.Result.Outcome == StreamOutcome.Ended
                && IsInCompletingPhase)
            {
                _config.Logger.LogInformation("Control stream ended while in completing phase: the hub read the acknowledgement. Transfer confirmed Completed.");
                _fsm.TransitionTo(SessionState.Completed);
                return;
            }

            var finished = await Task.WhenAny(activeTasks);
            var result = await finished;

            if (result.Outcome == StreamOutcome.Retryable)
            {
                _config.Logger.LogWarning("Stream failed (Retryable): {Error}. Transitioning to Reconnecting.", result.ErrorMessage);
                if (!IsInCompletingPhase)
                {
                    _fsm.OnStreamsLost();
                }
                else
                {
                    _config.Logger.LogInformation("Retrying stream connection while in completing phase.");
                    await Task.Delay(CompletingPhaseRetryMs, token);
                }
            }
            else if (result.Outcome == StreamOutcome.Gone && IsInCompletingPhase)
            {
                // The hub forgets a transfer when the sender's data stream ends cleanly, which comes after
                // the acknowledgement: a stream reopened in the closing phase that finds the transfer
                // gone has found it finished.
                _config.Logger.LogInformation("Transfer Gone (404) received while in completing phase. Transfer confirmed Completed.");
                _fsm.TransitionTo(SessionState.Completed);
            }
            else if (!result.IsSuccess)
            {
                _config.Logger.LogError("Stream failed (Fatal): {Error}. Sessions FAILED.", result.ErrorMessage);
                FailTransfer($"The data stream failed permanently: {result.ErrorMessage}");
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
                        _fsm.OnStreamsLost();
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
        while (await WaitForOutgoingAsync(outputStream, transferGuid, token))
        {
            while (_batchProcessor.OutgoingMessages.TryRead(out var message))
            {
                if (message is BatchControlMessage batchMsg)
                {
                    try
                    {
                        await _batchProcessor.WriteBatchAsync(outputStream, transferGuid, batchMsg.BatchId, batchMsg.Entry, token);
                    }
                    finally
                    {
                        // The message holds a reference of its own, taken when it was queued.
                        batchMsg.Entry.Release();
                    }
                }
                else if (message is ControlChunkMessage ctrlMsg)
                {
                    await ChunkIO.WriteControlAsync(outputStream, transferGuid, ctrlMsg.BatchId, ctrlMsg.Type, token);

                    if (ctrlMsg.Type == ChunkType.Abort)
                    {
                        // Ends this side's outgoing stream cleanly, the same way CompleteAck does
                        // below: the hub sees a normal EOF and releases the bridge through its
                        // ordinary path instead of waiting on a reconnection that will never come.
                        // Not flushed here: the end of the stream carries it.
                        return;
                    }

                    // Ending the control stream is the receiver's step in the domino, and it belongs
                    // here rather than at the moment Complete was accepted: EOF after the
                    // acknowledgement means the hub read it, EOF before it was ever written means
                    // the sender waits forever. A stream that fails instead is reopened, and the
                    // acknowledgement goes out again on the new one. It is not flushed apart from the
                    // end: a sender that has it closes its control stream, and an end sent after that
                    // finds no stream left to end cleanly.
                    if (!IsSender && ctrlMsg.Type == ChunkType.CompleteAck)
                    {
                        return;
                    }
                }
            }
            
            await outputStream.FlushAsync(token);
        }
    }

    /// <summary>
    /// Waits for something to send. A sender that has had nothing to send for a heartbeat interval
    /// writes a Ping, and the receiver's Pong comes back on the control stream: the hub takes a data
    /// stream it has heard nothing from for two intervals as cut, and a front end cuts a response
    /// that carries no byte for long enough.
    /// </summary>
    private async Task<bool> WaitForOutgoingAsync(Stream outputStream, Guid transferGuid, CancellationToken token)
    {
        if (!IsSender)
        {
            return await _batchProcessor.OutgoingMessages.WaitToReadAsync(token);
        }

        while (true)
        {
            using var idle = CancellationTokenSource.CreateLinkedTokenSource(token);
            idle.CancelAfter(_config.Options.PresenceHeartbeatInterval);
            try
            {
                return await _batchProcessor.OutgoingMessages.WaitToReadAsync(idle.Token);
            }
            catch (OperationCanceledException) when (!token.IsCancellationRequested)
            {
                await ChunkIO.WriteControlAsync(outputStream, transferGuid, 0, ChunkType.Ping, token);
                await outputStream.FlushAsync(token);
            }
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
                if (IsInCompletingPhase)
                {
                    // The consumer has everything, and what still comes is the sender asking again for
                    // an acknowledgement already queued. The stream is read to its end rather than
                    // closed: the hub ends it once the sender's data stream has ended cleanly, and closed
                    // sooner it takes that stream down with it, possibly before the sender has read the
                    // acknowledgement.
                    _batchProcessor.CompleteReceiver();
                    await inputStream.CopyToAsync(Stream.Null, token);
                    break;
                }

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

                try
                {
                    await _batchProcessor.ProcessChunkAsync(chunkData.Value, token);
                }
                catch (UndecodableBatchException ex) when (!_cts.IsCancellationRequested && !IsStopped)
                {
                    // Failing cancels this attempt's write stream along with this one, so the sender is
                    // told first; otherwise it waits out its stall bound on a transfer that cannot complete.
                    _config.Logger.LogError(ex, "A batch could not be decoded - aborting Transfer={TransferId}", TransferId);
                    AbortPeer(AbortReason.UndecodableBatch);
                    await DeliverWriteSideEndAsync();
                    throw;
                }
                
                if (IsTerminal)
                {
                    _config.Logger.LogInformation("Transfer ended - exiting ReadLoop for Transfer={TransferId}", TransferId);
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            // A session that has stopped is being torn down, and what its loop raises then belongs to the
            // teardown: failing it releases a loop parked on the receive buffer before the disposal has
            // cancelled the streams, and a stream of a transfer that is over has nothing to retry.
            if (!_cts.IsCancellationRequested && !IsStopped)
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

        // A heartbeat counts as activity at the hub and renews the transfer's shared session. A session
        // that has stopped, or a receiver that only waits for its acknowledgement to be read, has no
        // activity left to claim, and claiming it would keep alive a transfer whose peer is gone.
        if (IsStopped || IsInCompletingPhase)
        {
            _heartbeatTimer.Change(Timeout.Infinite, Timeout.Infinite);
            return;
        }

        // A beat still waiting on the hub is not joined by the next: they would only pile up on a hub
        // out of reach.
        if (Interlocked.Exchange(ref _heartbeating, 1) == 1) return;
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
            timeout.CancelAfter(HeartbeatTimeout);

            var (items, acks) = IsSender
                ? (_batchProcessor.ItemsSent, _batchProcessor.AcksReceived)
                : (_batchProcessor.ItemsReceived, _batchProcessor.AcksSent);
            await _control.ReportTransferAsync(TransferId, _affinity, items, acks, timeout.Token);
        }
        catch (Exception ex)
        {
            // The next beat is a second away, and an outage is the streams' to report.
            _config.Logger.LogDebug("Heartbeat for {TransferId} failed: {Message}", TransferId, ex.Message);
        }
        finally
        {
            Volatile.Write(ref _heartbeating, 0);
        }
    }

    #endregion
}




