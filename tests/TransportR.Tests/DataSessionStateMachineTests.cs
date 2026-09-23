using TransportR.Client.SignalR.Sessions;
using TransportR.Services;
using TransportR.Models;
using Xunit;
using Microsoft.Extensions.Logging;

namespace TransportR.Tests;

public class DataSessionStateMachineTests
{
    private class StubLogger : ILogger
    {
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) { }
        public bool IsEnabled(LogLevel logLevel) => true;
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    }

    private readonly DataSessionStateMachine _fsm;

    public DataSessionStateMachineTests()
    {
        _fsm = new DataSessionStateMachine("test-transfer", new StubLogger());
    }

    [Fact]
    public void InitialState_IsIdle()
    {
        Assert.Equal(SessionState.Idle, _fsm.CurrentState);
    }

    [Fact]
    public void TransitionTo_ValidState_UpdatesState()
    {
        var result = _fsm.TransitionTo(SessionState.Starting);
        Assert.True(result);
        Assert.Equal(SessionState.Starting, _fsm.CurrentState);
    }

    [Fact]
    public void TransitionTo_SameState_ReturnsFalse()
    {
        _fsm.TransitionTo(SessionState.Starting);
        var result = _fsm.TransitionTo(SessionState.Starting);
        
        Assert.False(result);
        Assert.Equal(SessionState.Starting, _fsm.CurrentState);
    }

    [Fact]
    public void TransitionFromTerminal_ReturnsFalse()
    {
        _fsm.TransitionTo(SessionState.Completed);
        Assert.Equal(SessionState.Completed, _fsm.CurrentState);

        var result = _fsm.TransitionTo(SessionState.Reconnecting);

        Assert.False(result);
        Assert.Equal(SessionState.Completed, _fsm.CurrentState);
    }

    [Fact]
    public void OnReleased_FromNonFinalState_ReturnsTrueAndMovesToDisposed()
    {
        _fsm.TransitionTo(SessionState.Streaming);

        var result = _fsm.OnReleased();

        Assert.True(result);
        Assert.Equal(SessionState.Disposed, _fsm.CurrentState);
    }

    [Fact]
    public void OnReleased_FromCompleted_ReturnsFalseAndKeepsState()
    {
        // A transfer that ended normally emits no Abort: OnReleased finds nothing left to abandon.
        _fsm.TransitionTo(SessionState.Completed);

        var result = _fsm.OnReleased();

        Assert.False(result);
        Assert.Equal(SessionState.Completed, _fsm.CurrentState);
    }

    [Fact]
    public void OnAborted_FromNonFinalState_ReturnsTrueAndMovesToFailed()
    {
        _fsm.TransitionTo(SessionState.Streaming);

        var result = _fsm.OnAborted();

        Assert.True(result);
        Assert.Equal(SessionState.Failed, _fsm.CurrentState);
    }

    [Fact]
    public void OnAborted_FromCompleted_ReturnsFalseAndKeepsState()
    {
        // A stray Abort arriving after Completed changes nothing.
        _fsm.TransitionTo(SessionState.Completed);

        var result = _fsm.OnAborted();

        Assert.False(result);
        Assert.Equal(SessionState.Completed, _fsm.CurrentState);
    }
}
