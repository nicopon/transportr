using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.SignalR;
using TransportR.Protocol;
using Microsoft.AspNetCore.SignalR.Client;
using TransportR.Models;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Abstractions;
using TransportR.Client.SignalR.Configuration;
using TransportR.Client.SignalR.Internal;
using TransportR.Client.SignalR.Services;
using TransportR.Client.SignalR.Sessions;
using TransportR.Hub.SignalR;
using TransportR.Interfaces;
using TransportR.Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;


using TransportR.Serialization.MessagePack; // Extension

namespace TransportR.Tests;

public class SignalRIntegrationTests
{
    private readonly ITestOutputHelper _output;

    public SignalRIntegrationTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public async Task InitTransfer_ShouldSucceed_WhenHandlersReturnValues()
    {
        // 1. Start Server
        using var host = TestHubServerFactory.CreateHost();
        var server = host.GetTestServer();
        var handler = server.CreateHandler();

        // 2. Create Clients
        await using var sender = CreateClient(handler, "http://localhost", "sender");
        await using var receiver = CreateClient(handler, "http://localhost", "receiver");

        // 3. Connect clients
        // Note: In dev mode (TestHubServerFactory), groups are auto-assigned to ["test"]
        await sender.ConnectAsync();
        await receiver.ConnectAsync();
        
        // Wait briefly for registration
        await Task.Delay(500);

        // 4. Setup Receiver to "Accept" the transfer
        bool getReadyCalled = false;
        receiver.OnTransferStarted += (transfer) => 
        {
            getReadyCalled = true;
            return Task.CompletedTask;
        };

        // 5. Init Transfer (This is where it failed with HubException)
        var sendSession = await sender.InitTransferAsync(receiver.ClientId, batchSize: 100);

        // Assert
        Assert.NotNull(sendSession);
        Assert.True(getReadyCalled, "Receiver OnTransferStarted should have been triggered");
    }

    [Fact]
    public async Task HostInitiatedTransfer_DeliversEveryItem()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        await using var sender = CreateClient(handler, "http://localhost", "sender");
        await using var receiver = CreateClient(handler, "http://localhost", "receiver");

        var sendHandle = new TaskCompletionSource<Send<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        sender.OnSendRequested += send => { sendHandle.TrySetResult(send); return Task.CompletedTask; };
        receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        // Neither client asks for anything: the host names both ends.
        var initiator = host.Services.GetRequiredService<ITransferInitiator>();
        var transferId = await initiator.InitTransferAsync(sender.ClientId, receiver.ClientId, batchSize: 10, timeoutMs: 20_000);

        // Both handles are handed out inside the handshake, before the host has the id to match
        // them against - a host that routes by transfer id has to hold them until it does.
        Assert.True(sendHandle.Task.IsCompleted, "the sender's handle must be handed out before InitTransferAsync returns");
        Assert.True(receiveHandle.Task.IsCompleted, "the receiver's handle must be handed out before InitTransferAsync returns");

        await using var send = await sendHandle.Task;
        await using var receive = await receiveHandle.Task;
        Assert.Equal(transferId, send.TransferId);
        Assert.Equal(transferId, receive.TransferId);

        var expected = Enumerable.Range(0, 250).Select(i => $"item-{i}").ToList();
        var consumer = Task.Run(async () =>
        {
            var items = new List<string>();
            await foreach (var item in receive.ReceiveAsync())
            {
                items.Add(item);
            }
            await receive.WaitForCompletionAsync();
            return items;
        });

        await send.SendAsync(expected);
        await send.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(expected, await consumer.WaitAsync(TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public async Task SenderInitiatedTransfer_DeliversEveryItem()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        await using var sender = CreateClient(handler, "http://localhost", "sender");
        await using var receiver = CreateClient(handler, "http://localhost", "receiver");

        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 10);
        await using var receive = await receiveHandle.Task;

        var expected = Enumerable.Range(0, 250).Select(i => $"item-{i}").ToList();
        var consumer = Task.Run(async () =>
        {
            var items = new List<string>();
            await foreach (var item in receive.ReceiveAsync())
            {
                items.Add(item);
            }
            await receive.WaitForCompletionAsync();
            return items;
        });

        await send.SendAsync(expected);
        await send.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(expected, await consumer.WaitAsync(TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public async Task Transfer_DeliversEveryItem_WhenTheHostRegisteredNoProgressMonitor()
    {
        using var host = CreateHostWithoutProgressMonitor();
        var handler = host.GetTestServer().CreateHandler();

        await using var sender = CreateClient(handler, "http://localhost", "sender");
        await using var receiver = CreateClient(handler, "http://localhost", "receiver");

        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

        await sender.ConnectAsync().WaitAsync(TimeSpan.FromSeconds(10));
        await receiver.ConnectAsync().WaitAsync(TimeSpan.FromSeconds(10));

        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 10).WaitAsync(TimeSpan.FromSeconds(10));
        await using var receive = await receiveHandle.Task.WaitAsync(TimeSpan.FromSeconds(10));

        var expected = Enumerable.Range(0, 50).Select(i => $"item-{i}").ToList();
        var consumer = Task.Run(async () =>
        {
            var items = new List<string>();
            await foreach (var item in receive.ReceiveAsync())
            {
                items.Add(item);
            }
            await receive.WaitForCompletionAsync();
            return items;
        });

        await send.SendAsync(expected).WaitAsync(TimeSpan.FromSeconds(10));
        await send.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(expected, await consumer.WaitAsync(TimeSpan.FromSeconds(10)));
    }

    private static IHost CreateHostWithoutProgressMonitor()
    {
        // Deliberately omits the IHubProgressMonitor registration every other factory in this
        // project adds, to exercise AddDataHub()'s own default.
        return new HostBuilder()
            .ConfigureWebHost(webBuilder =>
            {
                webBuilder
                    .UseTestServer()
                    .ConfigureServices(services =>
                    {
                        services.AddLogging(logging => logging.AddConsole().SetMinimumLevel(LogLevel.Debug));

                        services.AddDataHub()
                                .UseDevMode(options =>
                                {
                                    options.AllowAnonymous = true;
                                    options.DefaultGroups = ["test"];
                                })
                                .Build();
                    })
                    .Configure(app =>
                    {
                        app.UseRouting();
                        app.UseEndpoints(endpoints =>
                        {
                            endpoints.MapDataHub(requireAuth: false);
                        });
                    });
            })
            .Start();
    }

    [Fact]
    public async Task Abort_WhenReceiverReleasesEarly_MakesTheSenderFailFast()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        // A window of one batch: SendAsync has nowhere to go once the first batch goes
        // unacknowledged, which is what "parked on the window" means here.
        await using var sender = CreateClient(handler, "http://localhost", "sender", o => o.MaxInFlightBatches = 1);
        await using var receiver = CreateClient(handler, "http://localhost", "receiver");

        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 1);
        var receive = await receiveHandle.Task;

        // Fills the window's only slot. Never acknowledged: the receiver gives up next, without
        // ever reading it.
        await send.SendAsync("item-1");
        await receive.DisposeAsync();

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        await Assert.ThrowsAsync<TransferFailedException>(
            () => send.SendAsync("item-2").WaitAsync(TimeSpan.FromSeconds(5)));
        stopwatch.Stop();

        // Measured 60s on the code before Abort (CompleteStallTimeout). The 5s bound above only
        // keeps a regression from hanging the test; this is the actual claim.
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(1),
            $"Expected the Abort to fail the send in under a second; took {stopwatch.Elapsed}.");
    }

    [Fact]
    public async Task Abort_WhenSenderReleasesMidFlight_MakesTheReceiverFailFast()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        await using var sender = CreateClient(handler, "http://localhost", "sender");
        await using var receiver = CreateClient(handler, "http://localhost", "receiver");

        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 1);
        var receive = await receiveHandle.Task;

        var receiveTask = Task.Run(async () =>
        {
            var items = new List<string>();
            await foreach (var item in receive.ReceiveAsync())
            {
                items.Add(item);
            }
            return items;
        });

        // A couple of items make it through, then the sender walks away mid-stream without ever
        // calling CompleteAsync.
        await send.SendAsync("item-1");
        await send.SendAsync("item-2");
        await send.DisposeAsync();

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        await Assert.ThrowsAsync<TransferFailedException>(() => receiveTask.WaitAsync(TimeSpan.FromSeconds(10)));
        stopwatch.Stop();

        // Measured over 150s on the code before Abort (the sender kept heartbeating, so the hub's
        // cleanup never judged the transfer inactive). The 10s bound above only keeps a regression
        // from hanging the test; this is the actual claim.
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(1),
            $"Expected the Abort to fail the receive in under a second; took {stopwatch.Elapsed}.");

        await receive.DisposeAsync();
    }

    [Fact]
    public async Task HostTermination_MakesBothEndsFailFast_SendAsyncIncluded()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        // A window of one batch, so a SendAsync call after termination has nowhere to go and must
        // be unblocked rather than merely queued.
        await using var sender = CreateClient(handler, "http://localhost", "sender", o => o.MaxInFlightBatches = 1);
        await using var receiver = CreateClient(handler, "http://localhost", "receiver");

        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 1);
        var receive = await receiveHandle.Task;

        var receiveTask = Task.Run(async () =>
        {
            var items = new List<string>();
            await foreach (var item in receive.ReceiveAsync())
            {
                items.Add(item);
            }
            await receive.WaitForCompletionAsync();
            return items;
        });

        // Acknowledged normally: the receiver is alive and consuming. The window then has to be
        // refilled by a second call, which is the one the host termination has to unblock.
        await send.SendAsync("item-1");

        var terminator = host.Services.GetRequiredService<ITransferTerminator>();
        await terminator.TerminateAsync(send.TransferId);

        var sendStopwatch = System.Diagnostics.Stopwatch.StartNew();
        await Assert.ThrowsAsync<TransferFailedException>(
            () => send.SendAsync("item-2").WaitAsync(TimeSpan.FromSeconds(5)));
        sendStopwatch.Stop();
        Assert.True(sendStopwatch.Elapsed < TimeSpan.FromSeconds(1),
            $"Expected termination to fail SendAsync in under a second; took {sendStopwatch.Elapsed}.");

        var receiveStopwatch = System.Diagnostics.Stopwatch.StartNew();
        await Assert.ThrowsAsync<TransferFailedException>(() => receiveTask.WaitAsync(TimeSpan.FromSeconds(5)));
        receiveStopwatch.Stop();
        Assert.True(receiveStopwatch.Elapsed < TimeSpan.FromSeconds(1),
            $"Expected termination to fail the receive in under a second; took {receiveStopwatch.Elapsed}.");
    }

    [Fact]
    public async Task NormalCompletion_NeverReleasesTheSessionBeforeItsOutcome()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        await using var sender = CreateClient(handler, "http://localhost", "sender");
        await using var receiver = CreateClient(handler, "http://localhost", "receiver");

        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 10);
        await using var receive = await receiveHandle.Task;

        var consumer = Task.Run(async () =>
        {
            var items = new List<string>();
            await foreach (var item in receive.ReceiveAsync())
            {
                items.Add(item);
            }
            await receive.WaitForCompletionAsync();
            return items;
        });

        var expected = Enumerable.Range(0, 20).Select(i => $"item-{i}").ToList();
        await send.SendAsync(expected);
        await send.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(30));

        // A transfer that ends normally reaches Completed before either handle is disposed, so
        // DisposeAsync's OnReleased() finds nothing left to abandon - no Abort is ever queued.
        Assert.Equal(expected, await consumer.WaitAsync(TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public async Task HostInitiatedTransfer_IsRefused_ByASenderWithNoHandler()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        await using var sender = CreateClient(handler, "http://localhost", "sender");
        await using var receiver = CreateClient(handler, "http://localhost", "receiver");
        receiver.OnTransferStarted += _ => Task.CompletedTask;

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var initiator = host.Services.GetRequiredService<ITransferInitiator>();
        var ex = await Assert.ThrowsAsync<HubException>(
            () => initiator.InitTransferAsync(sender.ClientId, receiver.ClientId, batchSize: 10, timeoutMs: 20_000));

        Assert.Contains("refused", ex.Message, StringComparison.OrdinalIgnoreCase);

        // The sender refused before creating anything; the receiver was readied first and is told
        // to cancel, which releases its session.
        Assert.Empty(sender.GetTransfers());
        await AssertEventuallyEmptyAsync(receiver);
    }

    [Fact]
    public async Task HostInitiatedTransfer_ReleasesTheSendersSession_WhenItsHandlerFails()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        await using var sender = CreateClient(handler, "http://localhost", "sender");
        await using var receiver = CreateClient(handler, "http://localhost", "receiver");
        sender.OnSendRequested += _ => throw new InvalidOperationException("no room for another transfer");
        receiver.OnTransferStarted += _ => Task.CompletedTask;

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var initiator = host.Services.GetRequiredService<ITransferInitiator>();
        await Assert.ThrowsAsync<HubException>(
            () => initiator.InitTransferAsync(sender.ClientId, receiver.ClientId, batchSize: 10, timeoutMs: 20_000));

        Assert.Empty(sender.GetTransfers());
        await AssertEventuallyEmptyAsync(receiver);
    }

    private static async Task AssertEventuallyEmptyAsync(SignalRDataClient<string> client)
    {
        // CancelTransfer is sent without waiting for the client to act on it.
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (client.GetTransfers().Count > 0 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50);
        }

        Assert.Empty(client.GetTransfers());
    }

    [Fact]
    public async Task GetReceivers_ShouldReportDisconnection_WithoutHidingTheClient()
    {
        // 1. Start Server
        using var host = TestHubServerFactory.CreateHost();
        var server = host.GetTestServer();
        var handler = server.CreateHandler();

        await using var sender = CreateClient(handler, "http://localhost", "sender");
        var receiver = CreateClient(handler, "http://localhost", "receiver");

        await sender.ConnectAsync();
        await receiver.ConnectAsync();
        await Task.Delay(500);

        // 2. While connected, the receiver carries no disconnection marker.
        var whileUp = await sender.GetReceiversAsync();
        var liveEntry = Assert.Single(whileUp, c => c.ClientId == receiver.ClientId);
        Assert.Null(liveEntry.DisconnectedAtUtc);

        // 3. Drop the receiver's control connection.
        var receiverId = receiver.ClientId;
        await receiver.DisposeAsync();
        await Task.Delay(500);

        // 4. The registration is still returned - deleting it would break a transfer that was
        //    about to resume - but it now says when the connection went away.
        var afterDrop = await sender.GetReceiversAsync();
        var staleEntry = Assert.Single(afterDrop, c => c.ClientId == receiverId);
        Assert.NotNull(staleEntry.DisconnectedAtUtc);
    }

    [Fact]
    public async Task InitTransfer_AgainstStaleRegistration_ShouldFailWithAnActionableMessage()
    {
        // 1. Start Server
        using var host = TestHubServerFactory.CreateHost();
        var server = host.GetTestServer();
        var handler = server.CreateHandler();

        await using var sender = CreateClient(handler, "http://localhost", "sender");
        var receiver = CreateClient(handler, "http://localhost", "receiver");

        await sender.ConnectAsync();
        await receiver.ConnectAsync();
        await Task.Delay(500);

        var receiverId = receiver.ClientId;
        await receiver.DisposeAsync();
        await Task.Delay(500);

        // 2. The registration outlived the connection, so the handshake invokes a connection that
        //    no longer exists. SignalR always prefixes its generic sentence, but it appends a
        //    HubException's own message after it - so an untranslated IOException here leaves the
        //    caller with the prefix and nothing else.
        var ex = await Assert.ThrowsAsync<HubException>(
            () => sender.InitTransferAsync(receiverId, batchSize: 100));

        Assert.Contains("not reachable", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Heartbeat_ReportsWhetherTheHubStillKnowsTheConnection()
    {
        // 1. Start Server
        using var host = TestHubServerFactory.CreateHost();
        var server = host.GetTestServer();
        var handler = server.CreateHandler();

        // A raw connection: this exercises the hub method, not the client that drives it.
        await using var connection = new HubConnectionBuilder()
            .WithUrl("http://localhost" + WireRoutes.CommandHub, o => o.HttpMessageHandlerFactory = _ => handler)
            .Build();
        await connection.StartAsync();

        var clientId = Guid.NewGuid();
        await connection.InvokeAsync(nameof(ICommandHub.Connect), clientId, new[] { "senders" });

        // 2. Registered: the hub knows this connection.
        Assert.True(await connection.InvokeAsync<bool>(nameof(ICommandHub.Heartbeat)));

        // 3. Drop the registration the way an expiry would - the connection stays up.
        var store = host.Services.GetRequiredService<IStateStore>();
        await store.DeleteClientAsync(clientId);

        // 4. Connected but unknown. This is what discovery sees, and why the answer has to carry
        //    more than a keep-alive: the client must know to register again.
        Assert.False(await connection.InvokeAsync<bool>(nameof(ICommandHub.Heartbeat)));
    }

    [Fact]
    public async Task PresenceHeartbeat_ReRegistersAfterTheRegistrationIsLost()
    {
        // 1. Start Server. A short presence interval so the test does not wait a minute.
        using var host = TestHubServerFactory.CreateHost();
        var server = host.GetTestServer();
        var handler = server.CreateHandler();

        await using var sender = CreateClient(handler, "http://localhost", "sender");
        await using var receiver = CreateClient(
            handler, "http://localhost", "receiver",
            o => o.PresenceHeartbeatInterval = TimeSpan.FromMilliseconds(300));

        await sender.ConnectAsync();
        await receiver.ConnectAsync();
        await Task.Delay(500);

        Assert.Contains(await sender.GetReceiversAsync(), c => c.ClientId == receiver.ClientId);

        // 2. Lose the receiver's registration while it stays connected - a hub restart, or an
        //    eviction past the grace period.
        var store = host.Services.GetRequiredService<IStateStore>();
        await store.DeleteClientAsync(receiver.ClientId);
        Assert.DoesNotContain(await sender.GetReceiversAsync(), c => c.ClientId == receiver.ClientId);

        // 3. The presence heartbeat notices and registers again, with no transfer in flight and
        //    nothing else prompting it.
        var deadline = DateTime.UtcNow.AddSeconds(10);
        List<ClientInfo> receivers;
        do
        {
            await Task.Delay(200);
            receivers = await sender.GetReceiversAsync();
        }
        while (!receivers.Any(c => c.ClientId == receiver.ClientId) && DateTime.UtcNow < deadline);

        Assert.Contains(receivers, c => c.ClientId == receiver.ClientId);
    }

    private SignalRDataClient<string> CreateClient(
        HttpMessageHandler handler, string url, string logPrefix, Action<ClientOptions>? configure = null)
    {
        // Custom Connection Manager that uses TestServer's HttpMessageHandler
        var options = new ClientOptions();
        configure?.Invoke(options);
        var logger = NullLogger.Instance; // Can use XunitLogger if needed
        
        // We need to bypass the standard HubConnectionBuilder.WithUrl and inject the handler
        // But HubConnectionBuilderExtensions only supports standard HttpClient or options.
        // THE TRICK: Use WithUrl(url, options => options.HttpMessageHandlerFactory = _ => handler)
        
        var connectionManager = new HubConnectionManager(
            url + WireRoutes.CommandHub,
            options,
            logger,
            initialAccessToken: null,
            tokenProvider: null,
             configureHttpConnection: (httpOpts) => 
             {
                 httpOpts.HttpMessageHandlerFactory = _ => handler;
             }
        );
        
        var registry = new SessionRegistry<string>(logger);
        var serializer = new TransportR.Serialization.MessagePack.Services.MessagePackBatchSerializer<string>();
        var streamManager = new Microsoft.IO.RecyclableMemoryStreamManager();
        var httpClient = new HttpClient(handler) { BaseAddress = new Uri(url) };

        return new SignalRDataClient<string>(
            connectionManager,
            registry,
            serializer,
            logger,
            defaultBatchSize: 100,
            options,
            url + WireRoutes.StreamBase,
            httpClient,
            streamManager
        );
    }
}
