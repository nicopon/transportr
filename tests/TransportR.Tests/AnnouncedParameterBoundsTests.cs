using System.Net;
using System.Net.WebSockets;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using TransportR.Client.Configuration;
using TransportR.Configuration;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Protocol;
using TransportR.Services;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// A parameter a client announces to the hub is the client's to choose, so the hub holds it to a
/// range before it acts on it.
/// </summary>
public class AnnouncedParameterBoundsTests
{
    [Fact]
    public async Task AHandshakeTimeoutOutsideTheRange_IsABadRequest_AndWritesNothing()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();
        await using var sender = TestClients.Create(handler, "http://localhost");
        await using var receiver = TestClients.Create(handler, "http://localhost");
        receiver.OnTransferStarted += _ => Task.CompletedTask;
        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var transfers = host.Services.GetRequiredService<ITransferManager>();
        var ceiling = (int)TransferManager.SessionTtl.TotalMilliseconds;

        // -1 is an infinite timeout, which would hold the handshake open with no deadline.
        foreach (var timeoutMs in new[] { -2, -1, ceiling + 1 })
        {
            using var refused = await OpenTransferAsync(host, sender.ClientId, receiver.ClientId, timeoutMs);
            Assert.Equal(HttpStatusCode.BadRequest, refused.StatusCode);
        }

        Assert.Empty(transfers.GetAllParticipants());

        // The client's own default and the ceiling itself are inside the range.
        var clientDefault = (int)new ClientOptions().HandshakeTimeout.TotalMilliseconds;
        foreach (var timeoutMs in new[] { clientDefault, ceiling })
        {
            using var accepted = await OpenTransferAsync(host, sender.ClientId, receiver.ClientId, timeoutMs);
            Assert.Equal(HttpStatusCode.OK, accepted.StatusCode);
        }
    }

    [Fact]
    public async Task AHeartbeatIntervalOutsideTheRange_IsABadRequest_OnEveryStreamThatAnnouncesOne()
    {
        await using var hub = await TestHubServerFactory.StartLoopbackAsync();
        using var http = new HttpClient();
        var transferId = await RegisterTransferAsync(hub);

        // Two silent intervals have to fit in what the hub allows a transfer to be silent for, so an
        // interval of half of it is already too long.
        var inactivitySeconds = hub.Services.GetRequiredService<IOptions<DataPlaneConfiguration>>().Value.TransferInactivityTimeoutSeconds;
        foreach (var heartbeatMs in new[] { 1, 999, inactivitySeconds * 500, int.MaxValue })
        {
            using var events = await OpenEventsAsync(http, hub, heartbeatMs);
            Assert.Equal(HttpStatusCode.BadRequest, events.StatusCode);

            using var segment = await SendSegmentAsync(http, hub, transferId, heartbeatMs);
            Assert.Equal(HttpStatusCode.BadRequest, segment.StatusCode);

            using var socket = await OpenWebSocketAsync(hub, transferId, heartbeatMs);
            Assert.Equal(HttpStatusCode.BadRequest, socket.HttpStatusCode);
        }

        // The shortest interval itself, and what a client announces by default, are inside it, on all three.
        var presence = (int)new ClientOptions().PresenceHeartbeatInterval.TotalMilliseconds;
        foreach (var heartbeatMs in new[] { 1000, presence })
        {
            using var openedEvents = await OpenEventsAsync(http, hub, heartbeatMs);
            Assert.Equal(HttpStatusCode.OK, openedEvents.StatusCode);

            using var openedSegment = await SendSegmentAsync(http, hub, await RegisterTransferAsync(hub), heartbeatMs);
            Assert.Equal(HttpStatusCode.OK, openedSegment.StatusCode);

            using var openedSocket = await OpenWebSocketAsync(hub, await RegisterTransferAsync(hub), heartbeatMs);
            Assert.Equal(WebSocketState.Open, openedSocket.State);
        }
    }

    private static async Task<string> RegisterTransferAsync(LoopbackHost hub)
    {
        var store = hub.Services.GetRequiredService<IStateStore>();
        var sender = new ClientRegistration(Guid.NewGuid(), "conn-" + Guid.NewGuid(), ["test"], null, null, DateTime.UtcNow);
        var receiver = new ClientRegistration(Guid.NewGuid(), "conn-" + Guid.NewGuid(), ["test"], null, null, DateTime.UtcNow);
        await store.SaveClientAsync(sender);
        await store.SaveClientAsync(receiver);

        var transferId = Guid.NewGuid().ToString();
        await hub.Services.GetRequiredService<ITransferManager>().RegisterParticipantsAsync(transferId, sender.ClientId, receiver.ClientId);
        return transferId;
    }

    private static Task<HttpResponseMessage> OpenEventsAsync(HttpClient http, LoopbackHost hub, int heartbeatMs) =>
        http.GetAsync($"{hub.BaseUrl}{WireRoutes.ClientBase}/{Guid.NewGuid()}/{WireRoutes.EventsSegment}?{ControlPlane.HeartbeatParameter}={heartbeatMs}",
            HttpCompletionOption.ResponseHeadersRead).WaitAsync(Bound);

    private static Task<HttpResponseMessage> SendSegmentAsync(HttpClient http, LoopbackHost hub, string transferId, int heartbeatMs)
    {
        var query = $"{DataPlane.EpochParameter}={Guid.NewGuid()}&{DataPlane.OffsetParameter}=0&{DataPlane.HeartbeatParameter}={heartbeatMs}";
        var request = new HttpRequestMessage(HttpMethod.Post, $"{hub.BaseUrl}{WireRoutes.StreamBase}/{transferId}/send?{query}")
        {
            Content = new ByteArrayContent([])
        };
        request.Headers.Add("X-Connection-Type", nameof(ConnectionType.Data));
        return http.SendAsync(request).WaitAsync(Bound);
    }

    /// <summary>The socket, whether or not the upgrade was accepted; its status says which.</summary>
    private static async Task<ClientWebSocket> OpenWebSocketAsync(LoopbackHost hub, string transferId, int heartbeatMs)
    {
        var socket = new ClientWebSocket();
        socket.Options.CollectHttpResponseDetails = true;
        socket.Options.SetRequestHeader("X-Connection-Type", nameof(ConnectionType.Data));
        var uri = new Uri($"{hub.BaseUrl.Replace("http://", "ws://")}{WireRoutes.StreamBase}/{transferId}/send?{DataPlane.HeartbeatParameter}={heartbeatMs}");
        try
        {
            await socket.ConnectAsync(uri, CancellationToken.None).WaitAsync(Bound);
        }
        catch (WebSocketException)
        {
        }

        return socket;
    }

    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    private static Task<HttpResponseMessage> OpenTransferAsync(IHost host, Guid senderId, Guid receiverId, int timeoutMs) =>
        RawHub.SendAsync(host, HttpMethod.Post, senderId, WireRoutes.TransfersSegment, token: null,
            new InitTransferRequest(receiverId, timeoutMs)).WaitAsync(TimeSpan.FromSeconds(30));
}
