using System.Net;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Hosting;
using TransportR.Protocol;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// Every 404 the hub answers itself carries the same mark, whatever the reason, so a client can tell a
/// refusal from a path that no TransportR route serves: a proxy that does not forward it answers 404
/// too, and a client that took that for a refusal would report one the hub never made.
/// </summary>
public class RefusedHeaderTests
{
    [Theory]
    [InlineData(WireRoutes.PresenceSegment)]
    [InlineData(WireRoutes.ReceiversSegment)]
    [InlineData(WireRoutes.TransfersSegment)]
    [InlineData(WireRoutes.RepliesSegment)]
    [InlineData(WireRoutes.HeartbeatSegment)]
    [InlineData(WireRoutes.PingSegment)]
    [InlineData(WireRoutes.TokenSegment)]
    public async Task AControlRequestOfAClientTheHubDoesNotKnow_IsRefusedWithTheMark(string route)
    {
        using var host = TestHubServerFactory.CreateHost();
        var clientId = Guid.NewGuid();

        // The token route is refused here for want of an attested caller, the others for want of a
        // registration: two reasons, one mark.
        using var response = route switch
        {
            WireRoutes.PresenceSegment => await RawHub.SendAsync(host, HttpMethod.Post, clientId, $"{route}/conn-1", token: null),
            WireRoutes.ReceiversSegment => await RawHub.SendAsync(host, HttpMethod.Get, clientId, route, token: null),
            WireRoutes.TransfersSegment => await RawHub.SendAsync(host, HttpMethod.Post, clientId, route, token: null,
                new InitTransferRequest(Guid.NewGuid(), 1000)),
            WireRoutes.RepliesSegment => await RawHub.SendAsync(host, HttpMethod.Post, clientId, $"{route}/{Guid.NewGuid()}", token: null,
                new CallbackReply(CallbackOutcome.Accepted)),
            WireRoutes.HeartbeatSegment => await RawHub.SendAsync(host, HttpMethod.Post, clientId,
                $"{WireRoutes.TransfersSegment}/transfer-1/{route}", token: null, new TransferProgress(0, 0)),
            WireRoutes.PingSegment => await RawHub.SendAsync(host, HttpMethod.Post, clientId,
                $"{WireRoutes.TransfersSegment}/transfer-1/{route}", token: null),
            _ => await RawHub.RenewAsync(host, clientId, token: null)
        };

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        AssertMarked(response);
    }

    [Fact]
    public async Task AStreamOfEventsTheHubRefuses_CarriesTheMark()
    {
        using var host = AuthenticatedHubServerFactory.CreateHost();
        var token = StubJwt.Create("withdrawn-subject", DateTime.UtcNow.AddHours(1));

        using var response = await RawHub.SendAsync(host, HttpMethod.Get, Guid.NewGuid(),
            $"{WireRoutes.EventsSegment}?{ControlPlane.HeartbeatParameter}=15000", token);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        AssertMarked(response);
    }

    [Theory]
    [InlineData("send")]
    [InlineData("receive")]
    [InlineData(WireRoutes.AbortSegment)]
    public async Task ADataPlaneRequestForATransferTheHubDoesNotKnow_IsRefusedWithTheMark(string route)
    {
        using var host = TestHubServerFactory.CreateHost();
        var method = route == "receive" ? HttpMethod.Get : HttpMethod.Post;
        var request = new HttpRequestMessage(method, $"http://localhost{WireRoutes.StreamBase}/no-such-transfer/{route}");
        request.Headers.Add("X-Connection-Type", nameof(ConnectionType.Data));
        if (method == HttpMethod.Post) request.Content = new ByteArrayContent([]);

        using var response = await host.GetTestClient().SendAsync(request, HttpCompletionOption.ResponseHeadersRead);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        AssertMarked(response);
    }

    [Theory]
    [InlineData(WireRoutes.ClientBase + "/00000000-0000-0000-0000-000000000001/nothing-here")]
    [InlineData(WireRoutes.StreamBase + "/transfer-1/nothing-here")]
    public async Task APathNoRouteServes_IsANotFoundWithoutTheMark(string path)
    {
        using var host = TestHubServerFactory.CreateHost();

        using var response = await host.GetTestClient().GetAsync($"http://localhost{path}");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.False(response.Headers.Contains(WireRoutes.RefusedHeader));
    }

    [Fact]
    public async Task AStreamOfEventsThatAnnouncesNoHeartbeat_IsABadRequestWithoutTheMark()
    {
        using var host = TestHubServerFactory.CreateHost();

        using var response = await RawHub.SendAsync(host, HttpMethod.Get, Guid.NewGuid(), WireRoutes.EventsSegment, token: null);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.False(response.Headers.Contains(WireRoutes.RefusedHeader));
    }

    private static void AssertMarked(HttpResponseMessage response)
    {
        Assert.True(response.Headers.TryGetValues(WireRoutes.RefusedHeader, out var values), "The refusal does not carry the mark.");
        Assert.Equal(["1"], values);
    }
}
