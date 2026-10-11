using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using TransportR.Abstractions;
using TransportR.Client.SignalR.Configuration;
using TransportR.Client.SignalR.Internal;
using TransportR.Client.SignalR.Services;
using TransportR.Hub.SignalR;
using TransportR.Hub.SignalR.Access;
using TransportR.Hub.SignalR.Configuration;
using TransportR.Hub.SignalR.Hubs;
using TransportR.Hub.SignalR.Services;
using TransportR.Interfaces;
using TransportR.Protocol;
using TransportR.Tests.Infrastructure;
using Xunit;

namespace TransportR.Tests;

/// <summary>
/// Replays the §4bis probe (host-initiated and sender-initiated transfers, a client-added method
/// both ways, disconnection) through <c>UseHub&lt;THub&gt;()</c> and
/// <c>HubConnectionManager.AddRegistrationCallback</c> - the product mechanism, not the throwaway
/// registration line the probe used.
/// </summary>
public class DerivedHubTests
{
    private class ProbeHub : CommandHub
    {
        public static int DisconnectCount;

        public ProbeHub(
            IStateStore stateStore,
            ITransferManager transferManager,
            IClientIdentityProvider identityProvider,
            SignalRTransferInitiator initiator,
            AccessPolicy policy,
            ConnectionRegistry connections,
            IOptions<AffinityOptions> affinityOptions,
            ILogger<CommandHub> logger,
            TimeProvider timeProvider)
            : base(stateStore, transferManager, identityProvider, initiator, policy, connections, affinityOptions, logger, timeProvider)
        {
        }

        // Client -> hub, a method the host added.
        public Task<string> HostPing() => Task.FromResult("pong");

        public override Task OnDisconnectedAsync(Exception? exception)
        {
            Interlocked.Increment(ref DisconnectCount);
            return base.OnDisconnectedAsync(exception);
        }
    }

    private static IHost CreateHost()
    {
        return new HostBuilder()
            .ConfigureWebHost(webBuilder =>
            {
                webBuilder
                    .UseTestServer()
                    .ConfigureServices(services =>
                    {
                        services.AddLogging(logging => logging.SetMinimumLevel(LogLevel.Warning));
                        services.AddDataHub()
                                .UseHub<ProbeHub>()
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
                        app.UseEndpoints(endpoints => endpoints.MapDataHub(requireAuth: false));
                    });
            })
            .Start();
    }

    private static SignalRDataClient<string> CreateClient(
        HttpMessageHandler handler, string url, Action<HubConnection>? controlConnectionCallback = null)
    {
        var options = new ClientOptions();
        var logger = NullLogger.Instance;

        var connectionManager = new HubConnectionManager(
            url + WireRoutes.CommandHub,
            options,
            logger,
            initialAccessToken: null,
            tokenProvider: null,
            configureHttpConnection: httpOpts => httpOpts.HttpMessageHandlerFactory = _ => handler);

        // Mirrors what TransportRClientBuilder.WithControlConnection does: composed in before
        // SignalRDataClient's constructor adds TransportR's own (SignalRTransferSignaling).
        if (controlConnectionCallback != null)
        {
            connectionManager.AddRegistrationCallback(controlConnectionCallback);
        }

        var registry = new SessionRegistry<string>(logger);
        var serializer = new TransportR.Serialization.MessagePack.Services.MessagePackBatchSerializer<string>();
        var streamManager = new Microsoft.IO.RecyclableMemoryStreamManager();
        var httpClient = new HttpClient(handler) { BaseAddress = new Uri(url) };

        return new SignalRDataClient<string>(
            connectionManager, registry, serializer, logger, defaultBatchSize: 10, options,
            url + WireRoutes.StreamBase, url + WireRoutes.ClientBase, httpClient, streamManager);
    }

    [Fact]
    public async Task DerivedHub_CarriesHostInitiatedAndSenderInitiatedTransfers()
    {
        using var host = CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        // Host-initiated (C0's own path) through the derived hub.
        {
            await using var sender = CreateClient(handler, "http://localhost");
            await using var receiver = CreateClient(handler, "http://localhost");

            var sendHandle = new TaskCompletionSource<Send<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
            var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
            sender.OnSendRequested += send => { sendHandle.TrySetResult(send); return Task.CompletedTask; };
            receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

            await sender.ConnectAsync();
            await receiver.ConnectAsync();

            var initiator = host.Services.GetRequiredService<ITransferInitiator>();
            await initiator.InitTransferAsync(sender.ClientId, receiver.ClientId, batchSize: 10, timeoutMs: 20_000);

            await using var send = await sendHandle.Task;
            await using var receive = await receiveHandle.Task;

            var expected = Enumerable.Range(0, 100).Select(i => $"item-{i}").ToList();
            var consumer = Task.Run(async () =>
            {
                var items = new List<string>();
                await foreach (var item in receive.ReceiveAsync()) items.Add(item);
                await receive.WaitForCompletionAsync();
                return items;
            });

            await send.SendAsync(expected);
            await send.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(30));
            Assert.Equal(expected, await consumer.WaitAsync(TimeSpan.FromSeconds(30)));
        }

        // Sender-initiated (the inherited method), same derived hub.
        {
            await using var sender = CreateClient(handler, "http://localhost");
            await using var receiver = CreateClient(handler, "http://localhost");

            var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
            receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

            await sender.ConnectAsync();
            await receiver.ConnectAsync();

            await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 10);
            await using var receive = await receiveHandle.Task;

            var expected = Enumerable.Range(0, 100).Select(i => $"item-{i}").ToList();
            var consumer = Task.Run(async () =>
            {
                var items = new List<string>();
                await foreach (var item in receive.ReceiveAsync()) items.Add(item);
                await receive.WaitForCompletionAsync();
                return items;
            });

            await send.SendAsync(expected);
            await send.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(30));
            Assert.Equal(expected, await consumer.WaitAsync(TimeSpan.FromSeconds(30)));
        }
    }

    [Fact]
    public async Task DerivedHub_CarriesApplicationMethods_BothWays_AndSeesDisconnection()
    {
        using var host = CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        var disconnectsBefore = ProbeHub.DisconnectCount;

        // The handler below is posted through AddRegistrationCallback, composed with TransportR's
        // own registration rather than replacing it - if it were replaced, the transfer machinery
        // registered by SignalRDataClient's constructor would be the casualty, not this handler.
        var client = CreateClient(handler, "http://localhost",
            controlConnectionCallback: connection => connection.On("Acknowledge", () => true));

        await client.ConnectAsync();

        // Client -> hub, a method the host added to the derived hub.
        var pong = await client.ControlConnection.InvokeAsync<string>(nameof(ProbeHub.HostPing));
        Assert.Equal("pong", pong);

        // Hub -> client with a response, the shape a future Launch would take.
        var registration = await host.Services.GetRequiredService<IStateStore>().GetClientAsync(client.ClientId);
        var hubContext = host.Services.GetRequiredService<IHubContext<ProbeHub>>();
        var acknowledged = await hubContext.Clients.Client(registration!.ConnectionId).InvokeAsync<bool>("Acknowledge", CancellationToken.None);
        Assert.True(acknowledged);

        // Disconnection, seen by the override.
        await client.DisposeAsync();

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (ProbeHub.DisconnectCount < disconnectsBefore + 1 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50);
        }
        Assert.Equal(disconnectsBefore + 1, ProbeHub.DisconnectCount);
    }
}
