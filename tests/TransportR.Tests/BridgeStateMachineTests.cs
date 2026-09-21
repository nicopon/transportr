using TransportR.Services;
using TransportR.Models;
using Xunit;
using Microsoft.Extensions.Logging;

namespace TransportR.Tests;

/// <summary>
/// Tests for BridgeStateMachine - hub-side connection bridge lifecycle.
/// The machine takes events, not states, so there is no illegal-transition case to assert: the
/// question these ask is whether each event lands on the right state from every state.
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
        Assert.False(_fsm.SenderConnected);
        Assert.False(_fsm.ReceiverConnected);
    }

    #endregion

    #region Registration Sequence Tests

    [Fact]
    public void ReceiverFirst_TransitionsToWaitingForSender()
    {
        var result = _fsm.OnReceiverRegistered();

        Assert.True(result);
        Assert.Equal(BridgeState.WaitingForSender, _fsm.CurrentState);
    }

    [Fact]
    public void SenderFirst_TransitionsToWaitingForReceiver()
    {
        var result = _fsm.OnSenderRegistered();

        Assert.True(result);
        Assert.Equal(BridgeState.WaitingForReceiver, _fsm.CurrentState);
    }

    [Fact]
    public void BothConnected_TransitionsToActive()
    {
        _fsm.OnReceiverRegistered();
        var result = _fsm.OnSenderRegistered();

        Assert.True(result);
        Assert.Equal(BridgeState.Active, _fsm.CurrentState);
    }

    [Fact]
    public void BothConnected_SenderFirst_TransitionsToActive()
    {
        _fsm.OnSenderRegistered();
        var result = _fsm.OnReceiverRegistered();

        Assert.True(result);
        Assert.Equal(BridgeState.Active, _fsm.CurrentState);
    }

    #endregion

    #region Cancellation Tests

    [Fact]
    public void CanCancel_FromActive()
    {
        _fsm.OnReceiverRegistered();
        _fsm.OnSenderRegistered();
        var result = _fsm.OnCancelled();

        Assert.True(result);
        Assert.Equal(BridgeState.Cancelled, _fsm.CurrentState);
    }

    [Fact]
    public void CanCancel_FromIdle()
    {
        var result = _fsm.OnCancelled();

        Assert.True(result);
        Assert.Equal(BridgeState.Cancelled, _fsm.CurrentState);
    }

    [Fact]
    public void Cancelled_IsTerminal_IgnoresEveryEvent()
    {
        _fsm.OnCancelled();

        Assert.False(_fsm.OnSenderRegistered());
        Assert.False(_fsm.OnReceiverRegistered());
        Assert.False(_fsm.OnSenderLost());
        Assert.False(_fsm.OnReceiverLost());
        Assert.Equal(BridgeState.Cancelled, _fsm.CurrentState);
    }

    #endregion

    #region Reconnection Tests

    [Fact]
    public void Active_ToWaitingForSender_OnSenderLost()
    {
        _fsm.OnReceiverRegistered();
        _fsm.OnSenderRegistered();

        var result = _fsm.OnSenderLost();

        Assert.True(result);
        Assert.Equal(BridgeState.WaitingForSender, _fsm.CurrentState);
    }

    [Fact]
    public void Active_ToWaitingForReceiver_OnReceiverLost()
    {
        _fsm.OnReceiverRegistered();
        _fsm.OnSenderRegistered();

        var result = _fsm.OnReceiverLost();

        Assert.True(result);
        Assert.Equal(BridgeState.WaitingForReceiver, _fsm.CurrentState);
    }

    [Fact]
    public void WaitingForSender_BackToActive_OnSenderReconnect()
    {
        _fsm.OnReceiverRegistered();
        _fsm.OnSenderRegistered();
        _fsm.OnSenderLost();

        var result = _fsm.OnSenderRegistered();

        Assert.True(result);
        Assert.Equal(BridgeState.Active, _fsm.CurrentState);
    }

    [Fact]
    public void BothLost_ReturnsToIdle()
    {
        _fsm.OnReceiverRegistered();
        _fsm.OnSenderRegistered();

        _fsm.OnSenderLost();
        var result = _fsm.OnReceiverLost();

        Assert.True(result);
        Assert.Equal(BridgeState.Idle, _fsm.CurrentState);
    }

    #endregion

    #region Idempotence Tests

    [Fact]
    public void LosingAnAbsentSender_IsIgnored()
    {
        _fsm.OnReceiverRegistered();

        var result = _fsm.OnSenderLost();

        Assert.False(result);
        Assert.Equal(BridgeState.WaitingForSender, _fsm.CurrentState);
    }

    [Fact]
    public void LosingAnAbsentReceiver_IsIgnored()
    {
        _fsm.OnSenderRegistered();

        var result = _fsm.OnReceiverLost();

        Assert.False(result);
        Assert.Equal(BridgeState.WaitingForReceiver, _fsm.CurrentState);
    }

    [Fact]
    public void RegisteringTwice_LeavesTheStateCorrect()
    {
        _fsm.OnSenderRegistered();

        var result = _fsm.OnSenderRegistered();

        Assert.False(result);
        Assert.Equal(BridgeState.WaitingForReceiver, _fsm.CurrentState);
    }

    #endregion

    #region Invariant Tests

    /// <summary>
    /// The booleans ConnectionBridge used to carry alongside the state are now derived from it.
    /// This walks every event from every reachable state and checks the pair never contradicts the
    /// state it came from — the property nothing guarded while the two were stored separately.
    /// </summary>
    [Fact]
    public void FlagsAgreeWithState_AfterEveryEventSequence()
    {
        var events = new (string Name, Func<BridgeStateMachine, bool> Apply)[]
        {
            ("SenderRegistered", m => m.OnSenderRegistered()),
            ("ReceiverRegistered", m => m.OnReceiverRegistered()),
            ("SenderLost", m => m.OnSenderLost()),
            ("ReceiverLost", m => m.OnReceiverLost())
        };

        // Four events deep covers every reachable state and every event out of it.
        foreach (var a in events)
        foreach (var b in events)
        foreach (var c in events)
        foreach (var d in events)
        {
            var fsm = new BridgeStateMachine("invariant", new StubLogger());
            foreach (var step in new[] { a, b, c, d })
            {
                step.Apply(fsm);

                var expectedSender = fsm.CurrentState is BridgeState.WaitingForReceiver or BridgeState.Active;
                var expectedReceiver = fsm.CurrentState is BridgeState.WaitingForSender or BridgeState.Active;

                Assert.Equal(expectedSender, fsm.SenderConnected);
                Assert.Equal(expectedReceiver, fsm.ReceiverConnected);
                Assert.True(
                    Enum.IsDefined(fsm.CurrentState),
                    $"{a.Name},{b.Name},{c.Name},{d.Name} reached an undefined state");
            }
        }
    }

    #endregion
}
