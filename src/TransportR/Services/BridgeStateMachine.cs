using Microsoft.Extensions.Logging;
using TransportR.Models;

namespace TransportR.Services;

/// <summary>
/// Manages state transitions for ConnectionBridge with validation and thread safety.
/// </summary>
public class BridgeStateMachine
{
    private volatile BridgeState _currentState = BridgeState.Idle;
    private readonly ILogger _logger;
    private readonly string _bridgeKey;
    private readonly object _lock = new();

    public BridgeState CurrentState => _currentState;

    public BridgeStateMachine(string bridgeKey, ILogger logger)
    {
        _bridgeKey = bridgeKey;
        _logger = logger;
    }

    /// <summary>
    /// Attempts to transition to a new state.
    /// </summary>
    /// <returns>True if transition valid and executed, False if same state.</returns>
    /// <exception cref="InvalidOperationException">If transition is invalid.</exception>
    public bool TransitionTo(BridgeState newState)
    {
        lock (_lock)
        {
            if (_currentState == newState) return false;
            
            // Terminal state check
            if (_currentState == BridgeState.Cancelled)
            {
                _logger.LogWarning("Ignored transition from Cancelled to {New} for {BridgeKey}", newState, _bridgeKey);
                return false;
            }

            // Validation logic
            if (!IsValidTransition(_currentState, newState))
            {
                 _logger.LogError("Invalid transition from {Current} to {New} for {BridgeKey}", _currentState, newState, _bridgeKey);
                 throw new InvalidOperationException($"Invalid state transition: {_currentState} -> {newState}");
            }

            _logger.LogInformation("Bridge State: {Old} -> {New} ({BridgeKey})", _currentState, newState, _bridgeKey);
            _currentState = newState;
            return true;
        }
    }

    private static bool IsValidTransition(BridgeState from, BridgeState to)
    {
        if (to == BridgeState.Cancelled) return true; // Always allow cancellation

        return from switch
        {
            BridgeState.Idle => to == BridgeState.WaitingForReceiver || to == BridgeState.WaitingForSender,
            
            BridgeState.WaitingForReceiver => to == BridgeState.Active || to == BridgeState.Idle,
            
            BridgeState.WaitingForSender => to == BridgeState.Active || to == BridgeState.Idle,
            
            BridgeState.Active => to == BridgeState.WaitingForReceiver || to == BridgeState.WaitingForSender || to == BridgeState.Idle,
            
            _ => false
        };
    }
}
