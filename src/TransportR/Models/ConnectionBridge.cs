using TransportR.Services;
using Microsoft.Extensions.Logging;

namespace TransportR.Models;

/// <summary>
/// Represents a bridge between sender and receiver streams.
/// A disconnection ends only the stream generation of the side that left; its peer stays registered.
/// The bridge does not reach across: the copy loop of the source has to notice that its destination
/// is gone, by observing <see cref="ReceiverToken"/>, or it keeps writing to a stream nobody reads.
/// </summary>
public class ConnectionBridge
{
    private readonly object _lock = new();
    private TaskCompletionSource<Stream> _receiverStreamTcs = NewStreamTcs();
    private TaskCompletionSource<Stream> _senderStreamTcs = NewStreamTcs();
    private CancellationTokenSource _senderCts = new();
    private CancellationTokenSource _receiverCts = new();
    private readonly TaskCompletionSource _completionTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private readonly BridgeStateMachine _fsm;

    public string BridgeKey { get; }
    public BridgeState State => _fsm.CurrentState;
    public DateTime LastActivity { get; private set; } = DateTime.UtcNow;

    /// <summary>
    /// The token of the sender's current stream generation. Taken under the bridge lock: a reset
    /// cancels and disposes the running source before installing its replacement, and reading
    /// <c>.Token</c> off a disposed source throws.
    /// </summary>
    public CancellationToken SenderToken
    {
        get { lock (_lock) return _senderCts.Token; }
    }

    /// <summary>
    /// The token of the receiver's current stream generation. See <see cref="SenderToken"/>.
    /// </summary>
    public CancellationToken ReceiverToken
    {
        get { lock (_lock) return _receiverCts.Token; }
    }

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

            if (_fsm.ReceiverConnected)
            {
                ResetReceiver();
            }

            _fsm.OnReceiverRegistered();
            _currentReceiverId = connectionId;
            LastActivity = DateTime.UtcNow;
            _receiverStreamTcs.TrySetResult(receiverOutput);
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

            if (_fsm.SenderConnected)
            {
                ResetSender();
            }

            _fsm.OnSenderRegistered();
            _currentSenderId = connectionId;
            LastActivity = DateTime.UtcNow;
            _senderStreamTcs.TrySetResult(senderInput);
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
    /// Called when either side disconnects. Cancels the stream generation of that side only and resets
    /// it for reconnection; the peer stays registered on its own generation.
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
        if (_fsm.OnSenderLost())
        {
            LastActivity = DateTime.UtcNow;
        }

        if (!_senderCts.IsCancellationRequested)
        {
            _senderCts.Cancel();
            _senderCts.Dispose();
        }
        _senderCts = new CancellationTokenSource();

        _senderStreamTcs.TrySetCanceled();
        _senderStreamTcs = NewStreamTcs();
    }

    private void ResetReceiver()
    {
        if (_fsm.OnReceiverLost())
        {
            LastActivity = DateTime.UtcNow;
        }

        if (!_receiverCts.IsCancellationRequested)
        {
            _receiverCts.Cancel();
            _receiverCts.Dispose();
        }
        _receiverCts = new CancellationTokenSource();

        _receiverStreamTcs.TrySetCanceled();
        _receiverStreamTcs = NewStreamTcs();
    }

    /// <summary>
    /// Cancel the bridge, releasing any waiting tasks.
    /// </summary>
    public void Cancel()
    {
        lock (_lock)
        {
            _fsm.OnCancelled();
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

    // Every completion below is signalled with the bridge lock held. Without this flag the waiter's
    // continuation runs inline on the signalling thread, inside that lock, and whatever it does next
    // to the bridge executes in the middle of a registration or a reset.
    private static TaskCompletionSource<Stream> NewStreamTcs() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);
}
