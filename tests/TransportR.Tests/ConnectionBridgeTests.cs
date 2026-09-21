using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Models;
using Xunit;

namespace TransportR.Tests;

/// <summary>
/// Tests for ConnectionBridge — the object that pairs a sender stream with a receiver stream on the
/// hub. BridgeStateMachine covers the table; these cover what the bridge does around it: which
/// participant a reset belongs to, which token it cancels, and what a crossed register/reset leaves
/// behind.
/// </summary>
public class ConnectionBridgeTests
{
    private static ConnectionBridge NewBridge() =>
        new("transfer-1:Data", NullLogger.Instance);

    [Fact]
    public async Task ReceiverThenSender_ReachesActive_AndBothRegistrationsSucceed()
    {
        var bridge = NewBridge();
        var receiverId = Guid.NewGuid();
        var senderId = Guid.NewGuid();

        var receiverTask = bridge.RegisterReceiverAsync(Stream.Null, receiverId);
        Assert.Equal(BridgeState.WaitingForSender, bridge.State);

        var senderTask = bridge.RegisterSenderAsync(Stream.Null, senderId);

        Assert.True(await receiverTask);
        Assert.True(await senderTask);
        Assert.Equal(BridgeState.Active, bridge.State);
    }

    [Fact]
    public async Task SenderThenReceiver_ReachesActive()
    {
        var bridge = NewBridge();

        var senderTask = bridge.RegisterSenderAsync(Stream.Null, Guid.NewGuid());
        Assert.Equal(BridgeState.WaitingForReceiver, bridge.State);

        var receiverTask = bridge.RegisterReceiverAsync(Stream.Null, Guid.NewGuid());

        Assert.True(await senderTask);
        Assert.True(await receiverTask);
        Assert.Equal(BridgeState.Active, bridge.State);
    }

    [Fact]
    public async Task SenderReconnect_ReplacesTheStream_AndReturnsToActive()
    {
        var bridge = NewBridge();
        var receiverId = Guid.NewGuid();

        var receiverTask = bridge.RegisterReceiverAsync(Stream.Null, receiverId);
        Assert.True(await bridge.RegisterSenderAsync(Stream.Null, Guid.NewGuid()));
        Assert.True(await receiverTask);

        var secondStream = new MemoryStream();
        Assert.True(await bridge.RegisterSenderAsync(secondStream, Guid.NewGuid()));

        Assert.Equal(BridgeState.Active, bridge.State);
        Assert.Same(secondStream, await bridge.WaitForSenderStream());
    }

    [Fact]
    public async Task DisconnectOfTheCurrentSender_CancelsOnlyTheSenderToken()
    {
        var bridge = NewBridge();
        var senderId = Guid.NewGuid();

        var receiverTask = bridge.RegisterReceiverAsync(Stream.Null, Guid.NewGuid());
        Assert.True(await bridge.RegisterSenderAsync(Stream.Null, senderId));
        Assert.True(await receiverTask);

        var senderTokenBefore = bridge.SenderToken;

        bridge.OnDisconnected(senderId);

        Assert.Equal(BridgeState.WaitingForSender, bridge.State);
        Assert.True(senderTokenBefore.IsCancellationRequested);
        Assert.False(bridge.ReceiverToken.IsCancellationRequested);
        Assert.False(bridge.SenderToken.IsCancellationRequested);
    }

    [Fact]
    public async Task DisconnectOfTheCurrentReceiver_CancelsOnlyTheReceiverToken()
    {
        var bridge = NewBridge();
        var receiverId = Guid.NewGuid();

        var receiverTask = bridge.RegisterReceiverAsync(Stream.Null, receiverId);
        Assert.True(await bridge.RegisterSenderAsync(Stream.Null, Guid.NewGuid()));
        Assert.True(await receiverTask);

        var receiverTokenBefore = bridge.ReceiverToken;

        bridge.OnDisconnected(receiverId);

        Assert.Equal(BridgeState.WaitingForReceiver, bridge.State);
        Assert.True(receiverTokenBefore.IsCancellationRequested);
        Assert.False(bridge.SenderToken.IsCancellationRequested);
    }

    [Fact]
    public async Task DisconnectOfAnEvictedSender_LeavesTheBridgeAlone()
    {
        var bridge = NewBridge();
        var firstSenderId = Guid.NewGuid();

        var receiverTask = bridge.RegisterReceiverAsync(Stream.Null, Guid.NewGuid());
        Assert.True(await bridge.RegisterSenderAsync(Stream.Null, firstSenderId));
        Assert.True(await receiverTask);

        // The first sender is replaced, then its disconnect arrives late.
        Assert.True(await bridge.RegisterSenderAsync(Stream.Null, Guid.NewGuid()));
        var liveToken = bridge.SenderToken;

        bridge.OnDisconnected(firstSenderId);

        Assert.Equal(BridgeState.Active, bridge.State);
        Assert.False(liveToken.IsCancellationRequested);
    }

    [Fact]
    public async Task CrossedRegisterAndReset_EndsActiveWithTheSurvivingStream()
    {
        var bridge = NewBridge();
        var receiverId = Guid.NewGuid();
        var senderId = Guid.NewGuid();

        var receiverTask = bridge.RegisterReceiverAsync(Stream.Null, receiverId);
        Assert.True(await bridge.RegisterSenderAsync(Stream.Null, senderId));
        Assert.True(await receiverTask);

        bridge.OnDisconnected(receiverId);
        Assert.Equal(BridgeState.WaitingForReceiver, bridge.State);

        var newReceiverStream = new MemoryStream();
        var reconnect = bridge.RegisterReceiverAsync(newReceiverStream, Guid.NewGuid());

        Assert.Equal(BridgeState.Active, bridge.State);
        Assert.True(await reconnect);
        Assert.Same(newReceiverStream, await bridge.WaitForReceiverStream());
    }

    [Fact]
    public async Task BothSidesLost_ReturnsToIdle()
    {
        var bridge = NewBridge();
        var receiverId = Guid.NewGuid();
        var senderId = Guid.NewGuid();

        var receiverTask = bridge.RegisterReceiverAsync(Stream.Null, receiverId);
        Assert.True(await bridge.RegisterSenderAsync(Stream.Null, senderId));
        Assert.True(await receiverTask);

        bridge.OnDisconnected(senderId);
        bridge.OnDisconnected(receiverId);

        Assert.Equal(BridgeState.Idle, bridge.State);
    }

    [Fact]
    public async Task CancelledBridge_RefusesRegistrationAndIgnoresDisconnects()
    {
        var bridge = NewBridge();
        var senderId = Guid.NewGuid();

        var receiverTask = bridge.RegisterReceiverAsync(Stream.Null, Guid.NewGuid());
        Assert.True(await bridge.RegisterSenderAsync(Stream.Null, senderId));
        Assert.True(await receiverTask);

        bridge.Cancel();

        Assert.Equal(BridgeState.Cancelled, bridge.State);
        Assert.False(await bridge.RegisterSenderAsync(Stream.Null, Guid.NewGuid()));

        bridge.OnDisconnected(senderId);
        Assert.Equal(BridgeState.Cancelled, bridge.State);
    }

    /// <summary>
    /// The hub reads SenderToken off the data path while another thread may be resetting the
    /// sender. A reset disposes the running source before replacing it, so an unsynchronised read
    /// could land on the disposed one and throw ObjectDisposedException into HandleSendAsync.
    /// </summary>
    [Fact]
    public async Task ReadingTokensWhileAParticipantResets_NeverThrows()
    {
        var bridge = NewBridge();
        var receiverId = Guid.NewGuid();

        var receiverTask = bridge.RegisterReceiverAsync(Stream.Null, receiverId);
        var senderId = Guid.NewGuid();
        Assert.True(await bridge.RegisterSenderAsync(Stream.Null, senderId));
        Assert.True(await receiverTask);

        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        var readers = Enumerable.Range(0, 4).Select(i => Task.Run(() =>
        {
            while (!stop.IsCancellationRequested)
            {
                _ = bridge.SenderToken;
                _ = bridge.ReceiverToken;
            }
        })).ToArray();

        var resetter = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                bridge.OnDisconnected(senderId);
                await bridge.RegisterSenderAsync(Stream.Null, senderId);
            }
        });

        // Task.WhenAll surfaces the first exception any of them threw.
        await Task.WhenAll(readers.Append(resetter));
    }
}
