using TransportR.Services;
using Microsoft.Extensions.Logging;

namespace TransportR.Models;

/// <summary>
/// Represents a bridge between sender and receiver streams.
/// When either side disconnects, the bridge cancels both streams to ensure synchronized reconnection.
/// </summary>
public class ConnectionBridge
{
    private readonly object _lock = new();
    private TaskCompletionSource<Stream> _receiverStreamTcs = new();
    private TaskCompletionSource<Stream> _senderStreamTcs = new();
    private CancellationTokenSource _senderCts = new();
    private CancellationTokenSource _receiverCts = new();
    private TaskCompletionSource _completionTcs = new();
    
    private readonly BridgeStateMachine _fsm;

    public string BridgeKey { get; }
    public BridgeState State => _fsm.CurrentState;
    public DateTime LastActivity { get; private set; } = DateTime.UtcNow;
    public bool SenderConnected { get; private set; }
    public bool ReceiverConnected { get; private set; }  // Kept for internal logic check, but state comes from FSM

    public CancellationToken SenderToken => _senderCts.Token;
    public CancellationToken ReceiverToken => _receiverCts.Token;
    public Task CompletionTask => _completionTcs.Task;
    
    public ConnectionBridge(string bridgeKey, ILogger logger)
    {
        BridgeKey = bridgeKey;
        _fsm = new BridgeStateMachine(bridgeKey, logger);
    }

    private Guid _currentReceiverId;
    private Guid _currentSenderId;

    /// <summary>
    /// Register a receiver stream. Returns false if bridge is cancelled.
    ///Provider a unique connection ID to prevent stale disconnects from resetting the bridge.
    /// </summary>
    public async Task<bool> RegisterReceiverAsync(Stream receiverOutput, Guid connectionId)
    {
        lock (_lock)
        {
            if (State == BridgeState.Cancelled) return false;
            
            if (ReceiverConnected)
            {
                // Reset ONLY Receiver-side state
                // This will transition FSM: Active -> WaitingForReceiver OR WaitingForSender -> Idle
                ResetReceiver(); 
            }
            
            ReceiverConnected = true;
            _currentReceiverId = connectionId;
            LastActivity = DateTime.UtcNow;
            _receiverStreamTcs.TrySetResult(receiverOutput);
            
            // FSM Transition
            if (_fsm.CurrentState == BridgeState.Idle)
                _fsm.TransitionTo(BridgeState.WaitingForSender);
            else if (_fsm.CurrentState == BridgeState.WaitingForReceiver)
                _fsm.TransitionTo(BridgeState.Active);
        }
        
        // Wait for sender input to arrive (or cancellation)
        try
        {
            await _senderStreamTcs.Task;
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    /// <summary>
    /// Register a sender stream. Returns false if bridge is cancelled.
    /// Provider a unique connection ID to prevent stale disconnects from resetting the bridge.
    /// </summary>
    public async Task<bool> RegisterSenderAsync(Stream senderInput, Guid connectionId)
    {
        lock (_lock)
        {
            if (State == BridgeState.Cancelled) return false;
            
            if (SenderConnected)
            {
               // Reset ONLY Sender-side state
               // FSM: Active -> WaitingForSender OR WaitingForReceiver -> Idle
               ResetSender();
            }
            
            SenderConnected = true;
            _currentSenderId = connectionId;
            LastActivity = DateTime.UtcNow;
            _senderStreamTcs.TrySetResult(senderInput);
            
            // FSM Transition
            if (_fsm.CurrentState == BridgeState.Idle)
                _fsm.TransitionTo(BridgeState.WaitingForReceiver);
            else if (_fsm.CurrentState == BridgeState.WaitingForSender)
                _fsm.TransitionTo(BridgeState.Active);
        }
        
        // Wait for receiver output to arrive (or cancellation)
        try
        {
            await _receiverStreamTcs.Task;
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    /// <summary>
    /// Called when either side disconnects. Cancels both streams and resets for reconnection.
    /// This ensures both parties reconnect together.
    /// Only resets if the disconnecting party is the CURRENTLY active one.
    /// </summary>
    public void OnDisconnected(Guid connectionId)
    {
        lock (_lock)
        {
            if (State == BridgeState.Cancelled) return;
            
            if (_currentSenderId == connectionId)
            {
                ResetSender();
            }
            else if (_currentReceiverId == connectionId)
            {
                ResetReceiver();
            }
        }
    }

    private void ResetSender()
    {
        if (SenderConnected)
        {
            SenderConnected = false;
            LastActivity = DateTime.UtcNow;
            
            // FSM Update
            if (_fsm.CurrentState == BridgeState.Active)
                _fsm.TransitionTo(BridgeState.WaitingForSender);
            else if (_fsm.CurrentState == BridgeState.WaitingForReceiver)
                _fsm.TransitionTo(BridgeState.Idle);
        }
        
        if (!_senderCts.IsCancellationRequested)
        {
            _senderCts.Cancel();
            _senderCts.Dispose();
        }
        _senderCts = new CancellationTokenSource();
        
        _senderStreamTcs.TrySetCanceled();
        _senderStreamTcs = new TaskCompletionSource<Stream>();
    }

    private void ResetReceiver()
    {
        if (ReceiverConnected)
        {
            ReceiverConnected = false;
            LastActivity = DateTime.UtcNow;
            
            // FSM Update
            if (_fsm.CurrentState == BridgeState.Active)
                _fsm.TransitionTo(BridgeState.WaitingForReceiver);
            else if (_fsm.CurrentState == BridgeState.WaitingForSender)
                _fsm.TransitionTo(BridgeState.Idle);
        }
        
        if (!_receiverCts.IsCancellationRequested)
        {
            _receiverCts.Cancel();
            _receiverCts.Dispose();
        }
        _receiverCts = new CancellationTokenSource();
        
        _receiverStreamTcs.TrySetCanceled();
        _receiverStreamTcs = new TaskCompletionSource<Stream>();
    }

    /// <summary>
    /// Cancel the bridge, releasing any waiting tasks.
    /// </summary>
    public void Cancel()
    {
        lock (_lock)
        {
            _fsm.TransitionTo(BridgeState.Cancelled);
            _senderCts.Cancel();
            _receiverCts.Cancel();
            _senderStreamTcs.TrySetCanceled();
            _receiverStreamTcs.TrySetCanceled();
            _completionTcs.TrySetCanceled();
        }
    }

    /// <summary>
    /// Signals that the sender has finished transmitting data cleanly.
    /// This allows the receiver stream to exit its wait loop.
    /// </summary>
    public void Complete()
    {
         // Sender finished Data stream - Receiver should close cleanly.
         // Completing the TCS allows HandleReceiveAsync to exit its wait loop.
         _completionTcs.TrySetResult();
    }

    public Task<Stream> WaitForSenderStream() => _senderStreamTcs.Task;
    public Task<Stream> WaitForReceiverStream() => _receiverStreamTcs.Task;
}
