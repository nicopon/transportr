using Microsoft.Extensions.Logging;
using TransportR.Interfaces;
using TransportR.Models;

namespace TransportR.Services;

/// <summary>
/// Manages state transitions for DataSession with validation and thread safety.
/// </summary>
public class DataSessionStateMachine
{
    private volatile SessionState _currentState = SessionState.Idle;
    private readonly ILogger _logger;
    private readonly string _transferId;
    private readonly object _lock = new();

    public SessionState CurrentState => _currentState;

    public DataSessionStateMachine(string transferId, ILogger logger)
    {
        _transferId = transferId;
        _logger = logger;
    }

    /// <summary>
    /// Attempts to transition to a new state.
    /// </summary>
    /// <returns>True if transition valid and executed, False if ignored.</returns>
    public bool TransitionTo(SessionState newState)
    {
        lock (_lock)
        {
            if (_currentState == newState) return false;

            // Simple validation logic (can be expanded)
            if (IsTerminal(_currentState))
            {
                _logger.LogWarning("Invalid transition from Terminal state {Current} to {New} for {TransferId}", _currentState, newState, _transferId);
                return false;
            }

            if (_currentState == SessionState.Disposed) return false;

            _logger.LogInformation("State Transition: {Old} -> {New} (Transfer={TransferId})", _currentState, newState, _transferId);
            _currentState = newState;
            return true;
        }
    }

    private static bool IsTerminal(SessionState state) => 
        state == SessionState.Completed || 
        state == SessionState.Failed || 
        state == SessionState.Disposed;
}
