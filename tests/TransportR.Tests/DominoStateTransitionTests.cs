using TransportR.Services;
using TransportR.Models;
using Microsoft.Extensions.Logging;

namespace TransportR.Tests;

public class DominoStateTransitionTests
{
    private class StubLogger : ILogger
    {
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) { }
        public bool IsEnabled(LogLevel logLevel) => true;
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    }

    private readonly DataSessionStateMachine _fsm;

    public DominoStateTransitionTests()
    {
        _fsm = new DataSessionStateMachine("test-transfer", new StubLogger());
    }

    #region Sender Domino Sequence Tests

    [Fact]
    public void SenderDomino_Step1_StreamingToCompleteSent()
    {
        // Arrange: Sender is streaming
        _fsm.TransitionTo(SessionState.Starting);
        _fsm.TransitionTo(SessionState.Streaming);

        // Act: Send Complete
        var result = _fsm.TransitionTo(SessionState.SenderCompleteSent);

        // Assert
        Assert.True(result);
        Assert.Equal(SessionState.SenderCompleteSent, _fsm.CurrentState);
    }

    [Fact]
    public void SenderDomino_Step2_CompleteSentToAckReceived()
    {
        // Arrange: Sender sent Complete
        _fsm.TransitionTo(SessionState.Starting);
        _fsm.TransitionTo(SessionState.Streaming);
        _fsm.TransitionTo(SessionState.SenderCompleteSent);

        // Act: Receive CompleteAck
        var result = _fsm.TransitionTo(SessionState.SenderAckReceived);

        // Assert
        Assert.True(result);
        Assert.Equal(SessionState.SenderAckReceived, _fsm.CurrentState);
    }

    [Fact]
    public void SenderDomino_FullSequence_ToCompleted()
    {
        // Full sender domino sequence
        _fsm.TransitionTo(SessionState.Starting);
        _fsm.TransitionTo(SessionState.Streaming);
        _fsm.TransitionTo(SessionState.SenderCompleteSent);
        _fsm.TransitionTo(SessionState.SenderAckReceived);
        _fsm.TransitionTo(SessionState.SenderTerminatingDataPost);
        _fsm.TransitionTo(SessionState.SenderAwaitingControlGetEOF);
        var result = _fsm.TransitionTo(SessionState.Completed);

        Assert.True(result);
        Assert.Equal(SessionState.Completed, _fsm.CurrentState);
    }

    #endregion

    #region Receiver Domino Sequence Tests

    [Fact]
    public void ReceiverDomino_Step1_StreamingToCompleteReceived()
    {
        // Arrange: Receiver is streaming
        _fsm.TransitionTo(SessionState.Starting);
        _fsm.TransitionTo(SessionState.Streaming);

        // Act: Receive Complete
        var result = _fsm.TransitionTo(SessionState.ReceiverCompleteReceived);

        // Assert
        Assert.True(result);
        Assert.Equal(SessionState.ReceiverCompleteReceived, _fsm.CurrentState);
    }

    [Fact]
    public void ReceiverDomino_Step2_CompleteReceivedToAckSent()
    {
        // Arrange: Receiver got Complete
        _fsm.TransitionTo(SessionState.Starting);
        _fsm.TransitionTo(SessionState.Streaming);
        _fsm.TransitionTo(SessionState.ReceiverCompleteReceived);

        // Act: Send CompleteAck
        var result = _fsm.TransitionTo(SessionState.ReceiverAckSent);

        // Assert
        Assert.True(result);
        Assert.Equal(SessionState.ReceiverAckSent, _fsm.CurrentState);
    }

    [Fact]
    public void ReceiverDomino_FullSequence_ToCompleted()
    {
        // Full receiver domino sequence
        _fsm.TransitionTo(SessionState.Starting);
        _fsm.TransitionTo(SessionState.Streaming);
        _fsm.TransitionTo(SessionState.ReceiverCompleteReceived);
        _fsm.TransitionTo(SessionState.ReceiverAckSent);
        _fsm.TransitionTo(SessionState.ReceiverAwaitingDataGetEOF);
        _fsm.TransitionTo(SessionState.ReceiverTerminatingControlPost);
        var result = _fsm.TransitionTo(SessionState.Completed);

        Assert.True(result);
        Assert.Equal(SessionState.Completed, _fsm.CurrentState);
    }

    #endregion

    #region Reconnection During Domino Tests

    [Fact]
    public void Reconnection_DuringDomino_Sender_AllowsTransition()
    {
        // Arrange: Sender in completing phase
        _fsm.TransitionTo(SessionState.Starting);
        _fsm.TransitionTo(SessionState.Streaming);
        _fsm.TransitionTo(SessionState.SenderCompleteSent);

        // Act: Network failure triggers reconnect
        var result = _fsm.TransitionTo(SessionState.Reconnecting);

        // Assert: Should still allow reconnection (for network recovery)
        Assert.True(result);
        Assert.Equal(SessionState.Reconnecting, _fsm.CurrentState);
    }

    [Fact]
    public void Reconnection_DuringDomino_Receiver_AllowsTransition()
    {
        // Arrange: Receiver in completing phase
        _fsm.TransitionTo(SessionState.Starting);
        _fsm.TransitionTo(SessionState.Streaming);
        _fsm.TransitionTo(SessionState.ReceiverCompleteReceived);

        // Act: Network failure triggers reconnect
        var result = _fsm.TransitionTo(SessionState.Reconnecting);

        // Assert
        Assert.True(result);
        Assert.Equal(SessionState.Reconnecting, _fsm.CurrentState);
    }

    #endregion

    #region Terminal State Tests

    [Fact]
    public void Completed_IsTerminal_NoTransitions()
    {
        _fsm.TransitionTo(SessionState.Completed);

        var result1 = _fsm.TransitionTo(SessionState.Streaming);
        var result2 = _fsm.TransitionTo(SessionState.Reconnecting);

        Assert.False(result1);
        Assert.False(result2);
        Assert.Equal(SessionState.Completed, _fsm.CurrentState);
    }

    [Fact]
    public void Failed_IsTerminal_NoTransitions()
    {
        _fsm.TransitionTo(SessionState.Failed);

        var result1 = _fsm.TransitionTo(SessionState.Streaming);
        var result2 = _fsm.TransitionTo(SessionState.Reconnecting);

        Assert.False(result1);
        Assert.False(result2);
        Assert.Equal(SessionState.Failed, _fsm.CurrentState);
    }

    [Fact]
    public void Disposed_IsTerminal_NoTransitions()
    {
        _fsm.TransitionTo(SessionState.Disposed);

        var result1 = _fsm.TransitionTo(SessionState.Streaming);
        var result2 = _fsm.TransitionTo(SessionState.Idle);

        Assert.False(result1);
        Assert.False(result2);
        Assert.Equal(SessionState.Disposed, _fsm.CurrentState);
    }

    #endregion

    #region Edge Cases

    [Fact]
    public void CanTransitionToFailed_FromAnyNonTerminalState()
    {
        // From Streaming
        _fsm.TransitionTo(SessionState.Streaming);
        var result = _fsm.TransitionTo(SessionState.Failed);
        Assert.True(result);
    }

    [Fact]
    public void CanTransitionToFailed_FromDominoState()
    {
        // From sender completing phase
        _fsm.TransitionTo(SessionState.SenderCompleteSent);
        var result = _fsm.TransitionTo(SessionState.Failed);
        Assert.True(result);
    }

    #endregion
}
