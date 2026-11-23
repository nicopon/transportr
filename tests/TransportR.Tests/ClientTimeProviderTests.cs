using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Time.Testing;
using TransportR.Client;
using TransportR.Serialization.MessagePack;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

public class ClientTimeProviderTests
{
    [Fact]
    public void WithTimeProvider_Null_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new TransportRClientBuilder<string>().WithTimeProvider(null!));
        Assert.Throws<ArgumentNullException>(() => new ByteStreamClientBuilder(1024).WithTimeProvider(null!));
    }

    [Fact]
    public async Task WithTimeProvider_DrivesAcknowledgementTimeoutDeterministically()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);

        await using var sender = new TransportRClientBuilder<string>()
            .WithUrl("http://localhost")
            .WithHttpMessageHandler(handler)
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithTimeProvider(clock)
            .WithAcknowledgementTimeout(TimeSpan.FromSeconds(30))
            .WithMaxInFlightBatches(1)
            .WithMessagePackSerialization()
            .Build();

        await using var receiver = new TransportRClientBuilder<string>()
            .WithUrl("http://localhost")
            .WithHttpMessageHandler(handler)
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithReceiveCapacity(maxItems: 1)
            .WithMessagePackSerialization()
            .Build();

        var allowRead = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted(async receive =>
        {
            await using (receive)
            {
                await allowRead.Task;
                await foreach (var _ in receive.ReceiveAsync()) { }
            }
        });

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 1);

        // Batch 0 fills the 1-item receive buffer; batch 1 stalls ACKed at the receiver; CompleteAsync parks.
        await send.SendAsync("item-0");
        await send.SendAsync("item-1");

        var completeTask = send.CompleteAsync();
        await Task.Delay(150);
        Assert.False(completeTask.IsCompleted);

        clock.Advance(TimeSpan.FromSeconds(31));

        var ex = await Assert.ThrowsAsync<TransferFailedException>(() => completeTask).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(TransferFailureCategory.AcknowledgementTimedOut, ex.Category);
        allowRead.TrySetResult();
    }

    [Fact]
    public async Task WithTimeProvider_DrivesTokenRenewerSchedule()
    {
        using var host = AuthenticatedHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();
        var start = DateTimeOffset.UtcNow;
        var clock = new FakeTimeProvider(start);

        var subject = $"client-{Guid.NewGuid()}";
        var issuedCount = 0;
        var provider = () =>
        {
            var count = Interlocked.Increment(ref issuedCount);
            var expiry = count == 1
                ? start.UtcDateTime.AddMinutes(10)
                : start.UtcDateTime.AddMinutes(40);
            return Task.FromResult(StubJwt.Create(subject, expiry, AuthenticatedHubServerFactory.DefaultGroup));
        };

        await using var client = new TransportRClientBuilder<string>()
            .WithUrl("http://localhost")
            .WithHttpMessageHandler(handler)
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithTimeProvider(clock)
            .WithTokenRefreshThreshold(TimeSpan.FromMinutes(2))
            .WithTokenProvider(provider)
            .WithMessagePackSerialization()
            .Build();

        await client.ConnectAsync();

        // Wait for the initial push to settle (provider called once for Connect + TokenRenewer's initial push).
        await Task.Delay(150);
        var baseline = Volatile.Read(ref issuedCount);

        // Advance past the 8-minute mark (10 min expiry - 2 min threshold): renewal must fire.
        clock.Advance(TimeSpan.FromMinutes(8) + TimeSpan.FromSeconds(2));

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (Volatile.Read(ref issuedCount) == baseline && DateTime.UtcNow < deadline)
        {
            await Task.Delay(20);
        }

        Assert.True(Volatile.Read(ref issuedCount) > baseline);
    }

    [Fact]
    public async Task WithTimeProvider_DrivesAccumulatorFlushInterval()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);

        await using var sender = new TransportRClientBuilder<string>()
            .WithUrl("http://localhost")
            .WithHttpMessageHandler(handler)
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithTimeProvider(clock)
            .WithBatchTimeout(TimeSpan.FromSeconds(30))
            .WithMessagePackSerialization()
            .Build();

        await using var receiver = new TransportRClientBuilder<string>()
            .WithUrl("http://localhost")
            .WithHttpMessageHandler(handler)
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithMessagePackSerialization()
            .Build();

        var firstItemArrived = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted(async receive =>
        {
            await using (receive)
            {
                await foreach (var item in receive.ReceiveAsync())
                {
                    firstItemArrived.TrySetResult(item);
                }
            }
        });

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 100);
        await send.SendAsync("partial-item");

        await Task.Delay(100);
        Assert.False(firstItemArrived.Task.IsCompleted);

        // Accumulator checks every flushInterval/2 (15 s): two advances past 30 s flush the partial batch.
        clock.Advance(TimeSpan.FromSeconds(16));
        await Task.Delay(50);
        clock.Advance(TimeSpan.FromSeconds(16));

        Assert.Equal("partial-item", await firstItemArrived.Task.WaitAsync(TimeSpan.FromSeconds(5)));

        await send.CompleteAsync();
    }
}
