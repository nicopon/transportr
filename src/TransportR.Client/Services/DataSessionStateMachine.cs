using Microsoft.Extensions.Logging;
using TransportR.Models;

namespace TransportR.Services;

/// <summary>
/// Manages state transitions for DataSession with validation and thread safety.
/// </summary>
internal class DataSessionStateMachine
{
    private volatile SessionState _currentState = SessionState.Idle;
    private readonly ILogger _logger;
    private readonly string _transferId;
    private readonly object _lock = new();
    private readonly TaskCompletionSource _stopped = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _completing = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public SessionState CurrentState => _currentState;

    /// <summary>
    /// Completes when the session reaches a final state, whichever of them. A wait with no event of its
    /// own to be woken by ends on this instead of on its next tick.
    /// </summary>
    public Task Stopped => _stopped.Task;

    /// <summary>
    /// Completes when a receiver enters <see cref="SessionState.ReceiverCompleteReceived"/>, and never
    /// for a sender. It stays completed: what a wait on it starts is the bound on the closing phase.
    /// </summary>
    public Task Completing => _completing.Task;

    /// <summary>
    /// True while a receiver is closing: it accepted Complete, and what is left is the hub reading its
    /// acknowledgement. The streams have to stay open here, so callers use this to tell the closing
    /// sequence apart from a session that lost its peer. Always false on a sender.
    /// </summary>
    public bool IsCompleting => _currentState == SessionState.ReceiverCompleteReceived;

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

            Enter(newState);
            return true;
        }
    }

    /// <summary>
    /// Records that the streams ended without the transfer having, which a session that is still
    /// working answers by reconnecting. A session that has stopped keeps its state: its own release
    /// or failure is what cancelled the streams, and their ending says nothing new.
    /// </summary>
    /// <returns>True when the session moved to <see cref="SessionState.Reconnecting"/>.</returns>
    public bool OnStreamsLost()
    {
        lock (_lock)
        {
            if (IsFinal(_currentState) || _currentState == SessionState.Reconnecting) return false;

            Enter(SessionState.Reconnecting);
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

            Enter(SessionState.Disposed);
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

            Enter(SessionState.Failed);
            return true;
        }
    }

    /// <summary>
    /// Records that the hub ended the transfer, which fails it. Neither end released anything: the
    /// state says that the transfer failed, where a release leaves <see cref="SessionState.Disposed"/>.
    /// A session that already reached an outcome keeps it.
    /// </summary>
    /// <returns>True when the hub's ending stopped a session that had not yet reached an outcome.</returns>
    public bool OnEndedByHub()
    {
        lock (_lock)
        {
            if (IsFinal(_currentState)) return false;

            Enter(SessionState.Failed);
            return true;
        }
    }

    // Called under the lock: the state, the log line and the signal that the session stopped are one step.
    private void Enter(SessionState state)
    {
        _logger.LogInformation("State Transition: {Old} -> {New} (Transfer={TransferId})", _currentState, state, _transferId);
        _currentState = state;

        if (state == SessionState.ReceiverCompleteReceived)
        {
            _completing.TrySetResult();
        }

        if (IsFinal(state))
        {
            _stopped.TrySetResult();
        }
    }

    // "No transition may leave this state", which is not the same question as "did the transfer
    // stop": a disposed session never succeeded, so callers reporting an outcome ask their own.
    private static bool IsFinal(SessionState state) =>
        state == SessionState.Completed ||
        state == SessionState.Failed ||
        state == SessionState.Disposed;
}
