using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Client;
using TransportR.Client.Sessions;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// What <see cref="ByteStreamClientBuilder"/> accepts: the batch size is the constructor's, the other
/// settings are those of the typed builder's, and what only an item client could be given is not there.
/// </summary>
public class ByteStreamClientBuilderTests
{
    private static ByteStreamClientBuilder Builder(int batchBytes = 4096) =>
        new ByteStreamClientBuilder(batchBytes).WithUrl("http://localhost:1").WithMaxInFlightBatches(4);

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void TheConstructor_RefusesABudgetOfZeroOrLess(int batchBytes)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ByteStreamClientBuilder(batchBytes));
    }

    [Theory]
    [InlineData(int.MaxValue - 3)]
    [InlineData(int.MaxValue)]
    public void TheConstructor_RefusesABudgetThatCannotHoldItsLengthPrefix(int batchBytes)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ByteStreamClientBuilder(batchBytes));
    }

    [Fact]
    public void TheConstructor_AcceptsTheLargestBudgetAnIntCanHold()
    {
        _ = new ByteStreamClientBuilder(int.MaxValue - 4);
    }

    [Fact]
    public async Task Build_NeedsOnlyTheUrl_AndCarriesTheSettingsTheBuilderWasGiven()
    {
        await using var client = new ByteStreamClientBuilder(4096)
            .WithUrl("http://localhost:1")
            .WithBatchTimeout(TimeSpan.FromMilliseconds(250))
            .WithAcknowledgementTimeout(TimeSpan.FromSeconds(30))
            .WithMaxInFlightBatches(4)
            .WithReceiveCapacity(maxBytes: 1 << 20)
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithTokenRefreshThreshold(TimeSpan.FromMinutes(1))
            .WithLoggerFactory(NullLoggerFactory.Instance)
            .Build();

        var options = client.Connection.Options;
        Assert.Equal(250, options.BatchFlushIntervalMs);
        Assert.Equal(TimeSpan.FromSeconds(30), options.CompleteStallTimeout);
        Assert.Equal(4, options.MaxInFlightBatches);
        Assert.Equal(BufferCapacity.Bytes(1 << 20), options.ReceiveBufferCapacity);
        Assert.Equal(WriteTransportPolicy.SegmentsOnly, options.WriteTransportPolicy);
        Assert.Equal(TimeSpan.FromMinutes(1), options.TokenRefreshThreshold);
    }

    [Fact]
    public void Build_WithoutAUrl_Refuses()
    {
        var refused = Assert.Throws<InvalidOperationException>(() => new ByteStreamClientBuilder(1024).Build());

        Assert.Equal("Hub URL must be configured using WithUrl().", refused.Message);
    }

    public static TheoryData<TimeSpan> RefusedBatchTimeouts => new()
    {
        TimeSpan.Zero,
        TimeSpan.FromMilliseconds(-5),
        TimeSpan.FromTicks(TimeSpan.TicksPerMillisecond / 2),
        TimeSpan.FromTicks(TimeSpan.TicksPerMillisecond + 1),
        TimeSpan.FromMilliseconds((double)int.MaxValue + 1),
    };

    [Theory]
    [MemberData(nameof(RefusedBatchTimeouts))]
    public void WithBatchTimeout_RefusesAtTheCall_WhatIsNotAWholePositiveNumberOfMilliseconds(TimeSpan timeout)
    {
        var builder = Builder();

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithBatchTimeout(timeout));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(int.MaxValue)]
    public void WithBatchTimeout_AcceptsTheShortestAndTheLongestInterval(int milliseconds)
    {
        var builder = Builder().WithBatchTimeout(TimeSpan.FromMilliseconds(milliseconds));

        Assert.NotNull(builder);
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
    public void TheSharedSettings_KeepTheChecksOfTheTypedBuilder()
    {
        var builder = Builder();

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithMaxInFlightBatches(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithTokenRefreshThreshold(TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithWriteTransport((WriteTransportPolicy)99));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithReceiveCapacity(maxBytes: 0));
    }

    [Fact]
    public async Task WithClientCredentials_GivesTheBuilderATokenProvider_AndReturnsTheBuilder()
    {
        var builder = Builder();

        var returned = builder.WithClientCredentials("http://idp.invalid/token", "id", "secret", scope: null);

        Assert.Same(builder, returned);
        await using var client = returned.Build();
        Assert.True(client.Channel.HasTokenProvider);
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
        var pairKey = Enumerable.Range(0, 32).Select(i => (byte)(i * 5 + 2)).ToArray();

        var senderKeys = new Dictionary<Guid, byte[]> { [receiverId] = pairKey };
        var receiverKeys = new Dictionary<Guid, byte[]> { [senderId] = pairKey };

        await using var sender = new ByteStreamClientBuilder(1024)
            .WithUrl("http://localhost")
            .WithClientId(senderId)
            .WithHttpMessageHandler(server.CreateHandler())
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithMaxInFlightBatches(4)
            .WithEndToEndEncryption(peerId => senderKeys.GetValueOrDefault(peerId))
            .Build();

        await using var receiver = new ByteStreamClientBuilder(1024)
            .WithUrl("http://localhost")
            .WithClientId(receiverId)
            .WithHttpMessageHandler(server.CreateHandler())
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithReceiveCapacity(1 << 16)
            .WithEndToEndEncryption(peerId => receiverKeys.GetValueOrDefault(peerId))
            .Build();

        Assert.Equal(senderId, sender.ClientId);
        Assert.Equal(receiverId, receiver.ClientId);

        var receiveHandle = new TaskCompletionSource<ByteReceive>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted(receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; });

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        await using var send = await sender.InitTransferAsync(receiverId);
        await using var receive = await receiveHandle.Task.WaitAsync(TimeSpan.FromSeconds(10));

        var received = new MemoryStream();
        var copy = receive.CopyToAsync(received);

        var payload = Enumerable.Range(0, 1500).Select(i => (byte)(i % 251)).ToArray();
        await send.WriteAsync(payload).WaitAsync(TimeSpan.FromSeconds(10));
        await send.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(10));
        await copy.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(payload, received.ToArray());
        Assert.Equal(TransferProtection.EncryptedWithPreSharedKey, Assert.Single(sender.GetTransfers()).Protection);
    }

    [Fact]
    public async Task OnTransferStarted_And_OnSendRequested_RefuseNullOrSecondRegistration()
    {
        await using var client = Builder().Build();

        Assert.Throws<ArgumentNullException>(() => client.OnTransferStarted(null!));
        Assert.Throws<ArgumentNullException>(() => client.OnSendRequested(null!));

        client.OnTransferStarted(_ => Task.CompletedTask);
        client.OnSendRequested(_ => Task.CompletedTask);

        Assert.Throws<InvalidOperationException>(() => client.OnTransferStarted(_ => Task.CompletedTask));
        Assert.Throws<InvalidOperationException>(() => client.OnSendRequested(_ => Task.CompletedTask));
    }
}
