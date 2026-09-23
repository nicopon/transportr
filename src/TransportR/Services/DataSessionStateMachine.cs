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

    /// <summary>
    /// True while the session is in one of the domino states, sender or receiver side: the data is
    /// through and what is left is the closing exchange. The streams have to stay open here, so
    /// callers use this to tell the closing sequence apart from a session that lost its peer.
    /// </summary>
    public bool IsCompleting => IsCompletingPhase(_currentState);

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

            if (IsFinal(_currentState))
            {
                _logger.LogWarning("Invalid transition from Terminal state {Current} to {New} for {TransferId}", _currentState, newState, _transferId);
                return false;
            }

            _logger.LogInformation("State Transition: {Old} -> {New} (Transfer={TransferId})", _currentState, newState, _transferId);
            _currentState = newState;
            return true;
        }
    }

    /// <summary>
    /// Records that the session was released. A session that already reached an outcome keeps it:
    /// releasing a finished transfer is how its handle ends, not a state change.
    /// </summary>
    /// <returns>True when the release stopped a session that had not, which abandons its transfer.</returns>
    public bool OnReleased()
    {
        lock (_lock)
        {
            if (IsFinal(_currentState)) return false;

            _logger.LogInformation("State Transition: {Old} -> {New} (Transfer={TransferId})", _currentState, SessionState.Disposed, _transferId);
            _currentState = SessionState.Disposed;
            return true;
        }
    }

    /// <summary>
    /// Records that the peer sent Abort, failing the transfer. A session that already reached an
    /// outcome keeps it: a stray Abort arriving after Completed changes nothing.
    /// </summary>
    /// <returns>True when the abort stopped a session that had not yet reached an outcome.</returns>
    public bool OnAborted()
    {
        lock (_lock)
        {
            if (IsFinal(_currentState)) return false;

            _logger.LogInformation("State Transition: {Old} -> {New} (Transfer={TransferId})", _currentState, SessionState.Failed, _transferId);
            _currentState = SessionState.Failed;
            return true;
        }
    }

    // "No transition may leave this state", which is not the same question as "did the transfer
    // stop": a disposed session never succeeded, so callers reporting an outcome ask their own.
    private static bool IsFinal(SessionState state) =>
        state == SessionState.Completed ||
        state == SessionState.Failed ||
        state == SessionState.Disposed;

    private static bool IsCompletingPhase(SessionState state) => state switch
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
}
