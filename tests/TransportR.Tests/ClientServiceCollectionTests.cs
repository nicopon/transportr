using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using TransportR.Client;
using TransportR.Serialization.MessagePack;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

public class ClientServiceCollectionTests
{
    [Fact]
    public async Task AddTransportRClient_RegistersSingletonSenderReceiver_WiresDiServices_AndCompletesTransfer()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();
        var fakeClock = new FakeTimeProvider(DateTimeOffset.UtcNow);

        var senderServices = new ServiceCollection();
        senderServices.AddSingleton<ILoggerFactory>(NullLoggerFactory.Instance);
        senderServices.AddSingleton<TimeProvider>(fakeClock);
        senderServices.AddTransportRClient<string>(builder => builder
            .WithUrl("http://localhost")
            .WithHttpMessageHandler(handler)
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithMessagePackSerialization());

        var receiverServices = new ServiceCollection();
        receiverServices.AddSingleton<ILoggerFactory>(NullLoggerFactory.Instance);
        receiverServices.AddTransportRClient<string>((_, builder) => builder
            .WithUrl("http://localhost")
            .WithHttpMessageHandler(handler)
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithMessagePackSerialization());

        await using var senderSp = senderServices.BuildServiceProvider();
        await using var receiverSp = receiverServices.BuildServiceProvider();

        var sender = senderSp.GetRequiredService<IDataSender<string>>();
        var receiver = receiverSp.GetRequiredService<IDataReceiver<string>>();
        Assert.Same(senderSp.GetRequiredService<TransportRClient<string>>(), sender);
        Assert.Same(senderSp.GetRequiredService<IDataReceiver<string>>(), sender);
        Assert.Same(fakeClock, ((TransportRClient<string>)sender).Connection.Options.TimeProvider);

        var received = new List<string>();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted(async receive =>
        {
            await using (receive)
            {
                await foreach (var item in receive.ReceiveAsync())
                {
                    received.Add(item);
                }
            }
            done.TrySetResult();
        });

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 4);
        await send.SendAsync(["one", "two"]);
        await send.CompleteAsync();
        await done.Task.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(["one", "two"], received);
    }

    [Fact]
    public async Task AddTransportRClient_AllowsConfigureDelegateToOverrideDiServices()
    {
        var diClock = new FakeTimeProvider();
        var customClock = new FakeTimeProvider();

        var services = new ServiceCollection();
        services.AddSingleton<TimeProvider>(diClock);
        services.AddTransportRClient<string>(builder => builder
            .WithUrl("http://localhost")
            .WithTimeProvider(customClock)
            .WithMessagePackSerialization());

        await using var sp = services.BuildServiceProvider();
        var client = sp.GetRequiredService<TransportRClient<string>>();
        Assert.Same(customClock, client.Connection.Options.TimeProvider);
    }

    [Fact]
    public async Task AddByteStreamClient_ValidatesBatchBytesUpfront_AndCompletesByteTransfer()
    {
        var invalidServices = new ServiceCollection();
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            invalidServices.AddByteStreamClient(0, builder => builder.WithUrl("http://localhost")));

        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        var senderServices = new ServiceCollection();
        senderServices.AddByteStreamClient(1024, builder => builder
            .WithUrl("http://localhost")
            .WithHttpMessageHandler(handler)
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly));

        var receiverServices = new ServiceCollection();
        receiverServices.AddByteStreamClient(1024, builder => builder
            .WithUrl("http://localhost")
            .WithHttpMessageHandler(handler)
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly));

        await using var senderSp = senderServices.BuildServiceProvider();
        await using var receiverSp = receiverServices.BuildServiceProvider();

        var sender = senderSp.GetRequiredService<ByteStreamClient>();
        var receiver = receiverSp.GetRequiredService<ByteStreamClient>();

        using var received = new MemoryStream();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted(async receive =>
        {
            await using (receive)
            {
                await receive.CopyToAsync(received);
            }
            done.TrySetResult();
        });

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var payload = "hello-di-bytes"u8.ToArray();
        await using var send = await sender.InitTransferAsync(receiver.ClientId);
        await send.WriteAsync(payload);
        await send.CompleteAsync();
        await done.Task.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(payload, received.ToArray());
    }
}
