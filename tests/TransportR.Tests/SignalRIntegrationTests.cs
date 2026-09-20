using Microsoft.AspNetCore.SignalR;
using TransportR.Protocol;
using Microsoft.AspNetCore.SignalR.Client;
using TransportR.Models;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Client.SignalR.Configuration;
using TransportR.Client.SignalR.Internal;
using TransportR.Client.SignalR.Services;
using TransportR.Client.SignalR.Sessions;
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
            url,
            httpClient,
            streamManager
        );
    }
}
