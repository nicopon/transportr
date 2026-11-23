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
        var renewalPushDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var handler = new TokenPutTrackingHandler(
            host.GetTestServer().CreateHandler(),
            putCount =>
            {
                if (putCount >= 2) renewalPushDone.TrySetResult();
            });
        var start = DateTimeOffset.UtcNow;
        var fakeClock = new FakeTimeProvider(start);
        var renewalTimerArmed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var clock = new ObservingTimeProvider(fakeClock, due =>
        {
            if (due >= TimeSpan.FromMinutes(5)) renewalTimerArmed.TrySetResult();
        });

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

        // Wait deterministically until TokenRenewer finishes its initial push and arms the 8-minute timer.
        await renewalTimerArmed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1, Volatile.Read(ref issuedCount));

        // Advance past the 8-minute mark (10 min expiry - 2 min threshold): renewal must fire.
        fakeClock.Advance(TimeSpan.FromMinutes(8) + TimeSpan.FromSeconds(2));

        await renewalPushDone.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(Volatile.Read(ref issuedCount) >= 2);
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

        await Task.Yield();
        Assert.False(firstItemArrived.Task.IsCompleted);

        // Accumulator checks every flushInterval/2 (15 s): advance in bounded steps until the loop flushes.
        var advances = 0;
        while (!firstItemArrived.Task.IsCompleted && advances < 3)
        {
            clock.Advance(TimeSpan.FromSeconds(16));
            advances++;
            await Task.WhenAny(firstItemArrived.Task, Task.Delay(25));
        }

        Assert.Equal("partial-item", await firstItemArrived.Task.WaitAsync(TimeSpan.FromSeconds(5)));

        await send.CompleteAsync();
    }

    private sealed class ObservingTimeProvider(FakeTimeProvider inner, Action<TimeSpan> onTimerCreated) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => inner.GetUtcNow();
        public override long GetTimestamp() => inner.GetTimestamp();
        public override TimeZoneInfo LocalTimeZone => inner.LocalTimeZone;

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = inner.CreateTimer(callback, state, dueTime, period);
            onTimerCreated(dueTime);
            return timer;
        }
    }

    private sealed class TokenPutTrackingHandler(HttpMessageHandler inner, Action<int> onTokenPut) : DelegatingHandler(inner)
    {
        private int _putCount;

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var response = await base.SendAsync(request, cancellationToken);
            if (request.Method == HttpMethod.Put
                && request.RequestUri?.AbsolutePath.EndsWith("/" + WireRoutes.TokenSegment, StringComparison.Ordinal) == true)
            {
                onTokenPut(Interlocked.Increment(ref _putCount));
            }
            return response;
        }
    }
}
