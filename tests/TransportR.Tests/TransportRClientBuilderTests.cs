using System.Diagnostics;
using Microsoft.AspNetCore.TestHost;
using TransportR.Client;
using TransportR.Client.Sessions;
using TransportR.Serialization.MessagePack;
using TransportR.Tests.Infrastructure;
using TransportR.Tests.TestHelpers;

namespace TransportR.Tests;

/// <summary>
/// What <c>Build</c> of the builder of items refuses.
/// </summary>
public class TransportRClientBuilderTests
{
    private static TransportRClientBuilder<byte[]> Builder() =>
        new TransportRClientBuilder<byte[]>().WithUrl("http://localhost:1").WithMaxInFlightBatches(4);

    [Fact]
    public async Task WithBatchTimeout_CarriesTheIntervalToTheClient()
    {
        await using var client = new TransportRClientBuilder<string>()
            .WithUrl("http://localhost:1")
            .WithSerializer(new StubBatchSerializer())
            .WithMaxInFlightBatches(4)
            .WithBatchTimeout(TimeSpan.FromMilliseconds(250))
            .Build();

        Assert.Equal(250, client.Connection.Options.BatchFlushIntervalMs);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void WithBatchTimeout_RefusesAtTheCall_ADurationThatIsNotPositive(int milliseconds)
    {
        var builder = Builder();

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithBatchTimeout(TimeSpan.FromMilliseconds(milliseconds)));
    }

    // The longest wait the runtime's timers take, which is how the session waits on the delay.
    private static readonly TimeSpan LongestAcknowledgementTimeout = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    public static TheoryData<TimeSpan> RefusedAcknowledgementTimeouts => new()
    {
        TimeSpan.Zero,
        TimeSpan.FromMilliseconds(-5),
        LongestAcknowledgementTimeout + TimeSpan.FromMilliseconds(1),
    };

    [Theory]
    [MemberData(nameof(RefusedAcknowledgementTimeouts))]
    public void WithAcknowledgementTimeout_RefusesAtTheCall_WhatIsNotPositiveOrIsPastTheLongestWait(TimeSpan timeout)
    {
        var builder = Builder();

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithAcknowledgementTimeout(timeout));
    }

    [Fact]
    public void WithAcknowledgementTimeout_AcceptsTheLongestWait()
    {
        var builder = Builder().WithAcknowledgementTimeout(LongestAcknowledgementTimeout);

        Assert.NotNull(builder);
    }

    [Fact]
    public async Task WithAcknowledgementTimeout_EndsAParkedProducerAtTheDelayItGave_NotAtTheDefault()
    {
        // On the network, as a builder's client takes no handler: a delay that did not reach the session
        // leaves the third send parked for the default 60 s, which the ceiling below turns into a failure.
        await using var hub = await TestHubServerFactory.StartLoopbackAsync();

        await using var sender = new TransportRClientBuilder<string>()
            .WithUrl(hub.BaseUrl)
            .WithMessagePackSerialization()
            .WithMaxInFlightBatches(1)
            .WithAcknowledgementTimeout(TimeSpan.FromSeconds(2))
            .Build();

        // A batch is acknowledged only once it lands in the receive buffer. With room for one item and
        // nobody consuming, the second batch is delivered and never acknowledged.
        await using var receiver = new TransportRClientBuilder<string>()
            .WithUrl(hub.BaseUrl)
            .WithMessagePackSerialization()
            .WithReceiveCapacity(maxItems: 1)
            .Build();

        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted(receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; });

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 1);
        await using var receive = await receiveHandle.Task.WaitAsync(TimeSpan.FromSeconds(20));

        await send.SendAsync("item-1");
        await send.SendAsync("item-2");

        var stopwatch = Stopwatch.StartNew();
        await Assert.ThrowsAsync<TransferFailedException>(
            () => send.SendAsync("item-3").WaitAsync(TimeSpan.FromSeconds(20)));
        stopwatch.Stop();

        Assert.InRange(stopwatch.Elapsed, TimeSpan.FromSeconds(1.5), TimeSpan.FromSeconds(20));
    }

    [Fact]
    public void Build_WithoutASerializer_KeepsItsRefusal()
    {
        var refused = Assert.Throws<InvalidOperationException>(() => Builder().Build());

        Assert.Equal("Serializer must be configured using WithSerializer().", refused.Message);
    }

    [Fact]
    public void WithClientId_EmptyGuid_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => Builder().WithClientId(Guid.Empty));
    }

    [Fact]
    public async Task WithClientId_SetsClientId_AndSupportsPerPairPreSharedKeyOverTestServer()
    {
        using var host = TestHubServerFactory.CreateHost();
        var server = host.GetTestServer();
        var senderId = Guid.NewGuid();
        var receiverId = Guid.NewGuid();
        var pairKey = Enumerable.Range(0, 32).Select(i => (byte)(i * 3 + 5)).ToArray();

        var senderKeys = new Dictionary<Guid, byte[]> { [receiverId] = pairKey };
        var receiverKeys = new Dictionary<Guid, byte[]> { [senderId] = pairKey };

        await using var sender = new TransportRClientBuilder<string>()
            .WithUrl("http://localhost")
            .WithClientId(senderId)
            .WithHttpMessageHandler(server.CreateHandler())
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithMessagePackSerialization()
            .WithMaxInFlightBatches(4)
            .WithEndToEndEncryption(peerId => senderKeys.GetValueOrDefault(peerId))
            .Build();

        await using var receiver = new TransportRClientBuilder<string>()
            .WithUrl("http://localhost")
            .WithClientId(receiverId)
            .WithHttpMessageHandler(server.CreateHandler())
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithMessagePackSerialization()
            .WithReceiveCapacity(maxItems: 100)
            .WithEndToEndEncryption(peerId => receiverKeys.GetValueOrDefault(peerId))
            .Build();

        Assert.Equal(senderId, sender.ClientId);
        Assert.Equal(receiverId, receiver.ClientId);

        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted(receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; });

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        await using var send = await sender.InitTransferAsync(receiverId, batchSize: 5);
        await using var receive = await receiveHandle.Task.WaitAsync(TimeSpan.FromSeconds(10));

        var received = new List<string>();
        var consumer = Task.Run(async () =>
        {
            await foreach (var item in receive.ReceiveAsync()) received.Add(item);
        });

        await send.SendAsync(["a", "b", "c"]);
        await send.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(10));
        await consumer.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(["a", "b", "c"], received);
        Assert.Equal(TransferProtection.EncryptedWithPreSharedKey, Assert.Single(sender.GetTransfers()).Protection);
    }

    [Fact]
    public async Task OnTransferStarted_And_OnSendRequested_RefuseNullOrSecondRegistration()
    {
        await using var client = Builder().WithMessagePackSerialization().Build();

        Assert.Throws<ArgumentNullException>(() => client.OnTransferStarted(null!));
        Assert.Throws<ArgumentNullException>(() => client.OnSendRequested(null!));

        client.OnTransferStarted(_ => Task.CompletedTask);
        client.OnSendRequested(_ => Task.CompletedTask);

        Assert.Throws<InvalidOperationException>(() => client.OnTransferStarted(_ => Task.CompletedTask));
        Assert.Throws<InvalidOperationException>(() => client.OnSendRequested(_ => Task.CompletedTask));
    }
}
