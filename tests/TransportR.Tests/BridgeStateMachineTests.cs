using TransportR.Services;
using TransportR.Models;
using Xunit;
using Microsoft.Extensions.Logging;

namespace TransportR.Tests;

/// <summary>
/// Tests for BridgeStateMachine - hub-side connection bridge lifecycle.
/// Note: Domino completion states (SenderPostCompleted, etc.) are defined in the enum
/// but not yet wired into the state machine transitions.
/// </summary>
public class BridgeStateMachineTests
{
    private class StubLogger : ILogger
    {
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) { }
        public bool IsEnabled(LogLevel logLevel) => true;
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    }

    private readonly BridgeStateMachine _fsm;

    public BridgeStateMachineTests()
    {
        _fsm = new BridgeStateMachine("test-bridge", new StubLogger());
    }

    #region Initial State Tests

    [Fact]
    public void InitialState_IsIdle()
    {
        Assert.Equal(BridgeState.Idle, _fsm.CurrentState);
    }

    #endregion

    #region Registration Sequence Tests

    [Fact]
    public void ReceiverFirst_TransitionsToWaitingForSender()
    {
        var result = _fsm.TransitionTo(BridgeState.WaitingForSender);

        Assert.True(result);
        Assert.Equal(BridgeState.WaitingForSender, _fsm.CurrentState);
    }

    [Fact]
    public void SenderFirst_TransitionsToWaitingForReceiver()
    {
        var result = _fsm.TransitionTo(BridgeState.WaitingForReceiver);

        Assert.True(result);
        Assert.Equal(BridgeState.WaitingForReceiver, _fsm.CurrentState);
    }

    [Fact]
    public void BothConnected_TransitionsToActive()
    {
        _fsm.TransitionTo(BridgeState.WaitingForSender);
        var result = _fsm.TransitionTo(BridgeState.Active);

        Assert.True(result);
        Assert.Equal(BridgeState.Active, _fsm.CurrentState);
    }

    #endregion

    #region Cancellation Tests

    [Fact]
    public void CanTransitionToCancelled_FromActive()
    {
        _fsm.TransitionTo(BridgeState.WaitingForSender);
        _fsm.TransitionTo(BridgeState.Active);
        var result = _fsm.TransitionTo(BridgeState.Cancelled);

        Assert.True(result);
        Assert.Equal(BridgeState.Cancelled, _fsm.CurrentState);
    }

    [Fact]
    public void CanTransitionToCancelled_FromIdle()
    {
        var result = _fsm.TransitionTo(BridgeState.Cancelled);

        Assert.True(result);
        Assert.Equal(BridgeState.Cancelled, _fsm.CurrentState);
    }

    [Fact]
    public void Cancelled_IsTerminal_NoTransitions()
    {
        _fsm.TransitionTo(BridgeState.Cancelled);

        var result = _fsm.TransitionTo(BridgeState.WaitingForSender);

        Assert.False(result);
        Assert.Equal(BridgeState.Cancelled, _fsm.CurrentState);
    }

    #endregion

    #region Reconnection Tests

    [Fact]
    public void Active_ToWaitingForSender_OnSenderDisconnect()
    {
        _fsm.TransitionTo(BridgeState.WaitingForSender);
        _fsm.TransitionTo(BridgeState.Active);

        var result = _fsm.TransitionTo(BridgeState.WaitingForSender);

        Assert.True(result);
        Assert.Equal(BridgeState.WaitingForSender, _fsm.CurrentState);
    }

    [Fact]
    public void Active_ToWaitingForReceiver_OnReceiverDisconnect()
    {
        _fsm.TransitionTo(BridgeState.WaitingForSender);
        _fsm.TransitionTo(BridgeState.Active);

        var result = _fsm.TransitionTo(BridgeState.WaitingForReceiver);

        Assert.True(result);
        Assert.Equal(BridgeState.WaitingForReceiver, _fsm.CurrentState);
    }

    [Fact]
    public void WaitingForSender_BackToActive_OnSenderReconnect()
    {
        _fsm.TransitionTo(BridgeState.WaitingForSender);
        _fsm.TransitionTo(BridgeState.Active);
        _fsm.TransitionTo(BridgeState.WaitingForSender);

        var result = _fsm.TransitionTo(BridgeState.Active);

        Assert.True(result);
        Assert.Equal(BridgeState.Active, _fsm.CurrentState);
    }

    [Fact]
    public void Active_CanReset_ToIdle()
    {
        _fsm.TransitionTo(BridgeState.WaitingForSender);
        _fsm.TransitionTo(BridgeState.Active);

        var result = _fsm.TransitionTo(BridgeState.Idle);

        Assert.True(result);
        Assert.Equal(BridgeState.Idle, _fsm.CurrentState);
    }

    #endregion

    #region Invalid Transition Tests

    [Fact]
    public void Idle_ToActive_IsInvalid()
    {
        Assert.Throws<InvalidOperationException>(() => 
            _fsm.TransitionTo(BridgeState.Active));
    }

    [Fact]
    public void WaitingForSender_ToWaitingForReceiver_IsInvalid()
    {
        _fsm.TransitionTo(BridgeState.WaitingForSender);

        Assert.Throws<InvalidOperationException>(() => 
            _fsm.TransitionTo(BridgeState.WaitingForReceiver));
    }

    #endregion
}
