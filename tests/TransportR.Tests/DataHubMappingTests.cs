using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using TransportR.Client;
using TransportR.Hub;
using TransportR.Hub.Interfaces;
using TransportR.Serialization.MessagePack;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// What <c>MapDataHub()</c> arms when the host says nothing, and how <c>MapDataHub(routePrefix)</c>
/// exposes both planes under a path prefix with WebSockets self-contained.
/// </summary>
public class DataHubMappingTests
{
    private const string TransferId = "transfer-1";

    [Fact]
    public async Task AHubThatValidatesTokens_MappedWithoutArgument_ChallengesAnAnonymousStreamRequest()
    {
        using var host = AuthenticatedHubServerFactory.CreateHost(mapHub: endpoints => endpoints.MapDataHub());
        var store = host.Services.GetRequiredService<IStateStore>();
        var senderId = Guid.NewGuid();
        var receiverId = Guid.NewGuid();
        await store.SaveClientAsync(new ClientRegistration(senderId, "conn-sender", ["test"], null, null, DateTime.UtcNow, Subject: "sender-principal"));
        await store.SaveClientAsync(new ClientRegistration(receiverId, "conn-receiver", ["test"], null, null, DateTime.UtcNow, Subject: "receiver-principal"));
        await host.Services.GetRequiredService<ITransferManager>().RegisterParticipantsAsync(TransferId, senderId, receiverId);
        using var client = host.GetTestClient();

        var request = new HttpRequestMessage(HttpMethod.Get, $"http://localhost{WireRoutes.StreamBase}/{TransferId}/receive");
        request.Headers.Add("X-Connection-Type", nameof(ConnectionType.Data));
        using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task MapDataHub_WithRoutePrefix_ServesControlAndDataPlanesOverSegments()
    {
        using var host = new HostBuilder()
            .ConfigureWebHost(webBuilder => webBuilder
                .UseTestServer()
                .ConfigureServices(services =>
                {
                    services.AddDataHub().UseDevMode(o =>
                    {
                        o.AllowAnonymous = true;
                        o.DefaultGroups = ["test"];
                    });
                })
                .Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints => endpoints.MapDataHub("/transportr/", requireAuth: false));
                }))
            .Start();

        var handler = host.GetTestServer().CreateHandler();
        await using var sender = new TransportRClientBuilder<string>()
            .WithUrl("http://localhost/transportr")
            .WithHttpMessageHandler(handler)
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithMessagePackSerialization()
            .Build();
        await using var receiver = new TransportRClientBuilder<string>()
            .WithUrl("http://localhost/transportr")
            .WithHttpMessageHandler(handler)
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithMessagePackSerialization()
            .Build();

        var received = new List<string>();
        var receiverDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted(async receive =>
        {
            await using (receive)
            {
                await foreach (var item in receive.ReceiveAsync())
                {
                    received.Add(item);
                }
            }
            receiverDone.TrySetResult();
        });

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 2);
        await send.SendAsync("a");
        await send.SendAsync("b");
        await send.SendAsync("c");
        await send.CompleteAsync();
        await receiverDone.Task.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(["a", "b", "c"], received);
    }

    [Fact]
    public async Task MapDataHub_WithRoutePrefix_SupportsWebSocketsWithoutHostUseWebSockets()
    {
        var host = new HostBuilder()
            .ConfigureWebHost(webBuilder => webBuilder
                .UseKestrel()
                .UseUrls("http://127.0.0.1:0")
                .ConfigureServices(services =>
                {
                    services.AddDataHub().UseDevMode(o =>
                    {
                        o.AllowAnonymous = true;
                        o.DefaultGroups = ["test"];
                    });
                })
                .Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints => endpoints.MapDataHub("/edge/hub", requireAuth: false));
                }))
            .Build();

        await host.StartAsync();
        try
        {
            var baseUrl = host.Services.GetRequiredService<IServer>()
                .Features.Get<IServerAddressesFeature>()!.Addresses.First();
            var hubUrl = $"{baseUrl.TrimEnd('/')}/edge/hub";

            await using var sender = new TransportRClientBuilder<string>()
                .WithUrl(hubUrl)
                .WithWriteTransport(WriteTransportPolicy.RequireWebSocket)
                .WithMessagePackSerialization()
                .Build();
            await using var receiver = new TransportRClientBuilder<string>()
                .WithUrl(hubUrl)
                .WithWriteTransport(WriteTransportPolicy.RequireWebSocket)
                .WithMessagePackSerialization()
                .Build();

            var received = new List<string>();
            var receiverDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            receiver.OnTransferStarted(async receive =>
            {
                await using (receive)
                {
                    await foreach (var item in receive.ReceiveAsync())
                    {
                        received.Add(item);
                    }
                }
                receiverDone.TrySetResult();
            });

            await sender.ConnectAsync();
            await receiver.ConnectAsync();

            await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 2);
            await send.SendAsync("ws-1");
            await send.SendAsync("ws-2");
            await send.CompleteAsync();
            await receiverDone.Task.WaitAsync(TimeSpan.FromSeconds(10));

            Assert.Equal(["ws-1", "ws-2"], received);
            Assert.Equal(WriteTransport.WebSocket, sender.GetTransfers()[0].WriteTransport);
        }
        finally
        {
            await host.StopAsync();
            host.Dispose();
        }
    }

    [Fact]
    public async Task MapDataHub_WithRoutePrefixAndAuthArmed_ChallengesAnonymousRequests()
    {
        using var host = AuthenticatedHubServerFactory.CreateHost(mapHub: endpoints => endpoints.MapDataHub("/prefix"));
        using var client = host.GetTestClient();

        using var streamResp = await client.GetAsync($"http://localhost/prefix{WireRoutes.StreamBase}/t1/receive");
        using var clientResp = await client.GetAsync($"http://localhost/prefix{WireRoutes.ClientBase}/{Guid.NewGuid()}/receivers");

        Assert.Equal(HttpStatusCode.Unauthorized, streamResp.StatusCode);
        Assert.Equal(HttpStatusCode.Unauthorized, clientResp.StatusCode);
    }

    [Theory]
    [InlineData(" ")]
    [InlineData("/bad prefix")]
    [InlineData("/bad?prefix")]
    [InlineData("/bad#prefix")]
    [InlineData("/bad{id}")]
    [InlineData("/bad}id")]
    public void MapDataHub_WithInvalidCharactersInPrefix_ThrowsArgumentException(string prefix)
    {
        using var host = TestHubServerFactory.CreateHost();
        var builder = new Microsoft.AspNetCore.Builder.ApplicationBuilder(host.Services);
        builder.UseRouting();
        Assert.Throws<ArgumentException>(() =>
            builder.UseEndpoints(endpoints => endpoints.MapDataHub(prefix, requireAuth: false)));
    }

    [Fact]
    public async Task MapDataHub_MalformedQueryParameters_Return400EvenWhenThrowOnBadRequestIsEnabled()
    {
        using var host = new HostBuilder()
            .ConfigureWebHost(webBuilder => webBuilder
                .UseTestServer()
                .ConfigureServices(services =>
                {
                    services.Configure<Microsoft.AspNetCore.Routing.RouteHandlerOptions>(o => o.ThrowOnBadRequest = true);
                    services.AddDataHub().UseDevMode(o => o.AllowAnonymous = true);
                })
                .Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints => endpoints.MapDataHub(requireAuth: false));
                }))
            .Start();

        using var client = host.GetTestClient();

        using var eventsResp = await client.GetAsync($"{WireRoutes.ClientBase}/{Guid.NewGuid()}/{WireRoutes.EventsSegment}?heartbeatMs=abc");
        Assert.Equal(HttpStatusCode.BadRequest, eventsResp.StatusCode);

        using var sendOffsetResp = await client.PostAsync($"{WireRoutes.StreamBase}/t1/{WireRoutes.SendSegment}?offset=abc", content: null);
        Assert.Equal(HttpStatusCode.BadRequest, sendOffsetResp.StatusCode);

        using var sendFinalResp = await client.PostAsync($"{WireRoutes.StreamBase}/t1/{WireRoutes.SendSegment}?final=notbool", content: null);
        Assert.Equal(HttpStatusCode.BadRequest, sendFinalResp.StatusCode);
    }
}
