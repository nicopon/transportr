using Microsoft.Extensions.Logging;
using TransportR.Models;

namespace TransportR.Services;

/// <summary>
/// Owns the bridge lifecycle. Callers report what happened to a participant and the machine picks
/// the resulting state; there is no way to ask it for a state directly, so no caller can drive it
/// into one the table does not allow.
/// </summary>
/// <remarks>
/// Every member takes the machine's own lock and may be called from any thread.
/// </remarks>
public class BridgeStateMachine
{
    private volatile BridgeState _currentState = BridgeState.Idle;
    private readonly ILogger _logger;
    private readonly string _bridgeKey;
    private readonly object _lock = new();

    public BridgeState CurrentState => _currentState;

    /// <summary>True while a sender stream is registered on this bridge.</summary>
    public bool SenderConnected =>
        _currentState is BridgeState.WaitingForReceiver or BridgeState.Active;

    /// <summary>True while a receiver stream is registered on this bridge.</summary>
    public bool ReceiverConnected =>
        _currentState is BridgeState.WaitingForSender or BridgeState.Active;

    public BridgeStateMachine(string bridgeKey, ILogger logger)
    {
        _bridgeKey = bridgeKey;
        _logger = logger;
    }

    /// <returns>True if the state changed.</returns>
    public bool OnSenderRegistered()
    {
        lock (_lock)
        {
            switch (_currentState)
            {
                case BridgeState.Idle:
                    return MoveTo(BridgeState.WaitingForReceiver);
                case BridgeState.WaitingForSender:
                    return MoveTo(BridgeState.Active);
                case BridgeState.Cancelled:
                    return false;
                default:
                    // The caller is expected to drop the previous sender first, so arriving with one
                    // already registered means a reset was skipped and the old stream is orphaned.
                    _logger.LogError(
                        "Sender registered on {BridgeKey} while state is {State}; previous sender was not released",
                        _bridgeKey, _currentState);
                    return false;
            }
        }
    }

    /// <returns>True if the state changed.</returns>
    public bool OnReceiverRegistered()
    {
        lock (_lock)
        {
            switch (_currentState)
            {
                case BridgeState.Idle:
                    return MoveTo(BridgeState.WaitingForSender);
                case BridgeState.WaitingForReceiver:
                    return MoveTo(BridgeState.Active);
                case BridgeState.Cancelled:
                    return false;
                default:
                    _logger.LogError(
                        "Receiver registered on {BridgeKey} while state is {State}; previous receiver was not released",
                        _bridgeKey, _currentState);
                    return false;
            }
        }
    }

    /// <summary>
    /// Reports that the sender is gone. A sender that was already gone is not an anomaly: a reset
    /// runs on every disconnect, including ones that arrive twice.
    /// </summary>
    /// <returns>True if the state changed.</returns>
    public bool OnSenderLost()
    {
        lock (_lock)
        {
            return _currentState switch
            {
                BridgeState.WaitingForReceiver => MoveTo(BridgeState.Idle),
                BridgeState.Active => MoveTo(BridgeState.WaitingForSender),
                _ => false
            };
        }
    }

    /// <summary>
    /// Reports that the receiver is gone. See <see cref="OnSenderLost"/> for why a repeat is silent.
    /// </summary>
    /// <returns>True if the state changed.</returns>
    public bool OnReceiverLost()
    {
        lock (_lock)
        {
            return _currentState switch
            {
                BridgeState.WaitingForSender => MoveTo(BridgeState.Idle),
                BridgeState.Active => MoveTo(BridgeState.WaitingForReceiver),
                _ => false
            };
        }
    }

    /// <summary>
    /// Moves the bridge to its terminal state. Every later event is ignored.
    /// </summary>
    /// <returns>True if the state changed.</returns>
    public bool OnCancelled()
    {
        lock (_lock)
        {
            return MoveTo(BridgeState.Cancelled);
        }
    }

    private bool MoveTo(BridgeState newState)
    {
        if (_currentState == newState) return false;

        _logger.LogInformation("Bridge State: {Old} -> {New} ({BridgeKey})", _currentState, newState, _bridgeKey);
        _currentState = newState;
        return true;
    }
}
