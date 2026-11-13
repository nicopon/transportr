using System.Net;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using TransportR.Client.Configuration;
using TransportR.Interfaces;
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

        foreach (var timeoutMs in new[] { -2, ceiling + 1 })
        {
            using var refused = await OpenTransferAsync(host, sender.ClientId, receiver.ClientId, timeoutMs);
            Assert.Equal(HttpStatusCode.BadRequest, refused.StatusCode);
        }

        Assert.Empty(transfers.GetAllParticipants());

        // The client's own default, no delay at all, and the ceiling itself are inside the range.
        var clientDefault = (int)new ClientOptions().HandshakeTimeout.TotalMilliseconds;
        foreach (var timeoutMs in new[] { clientDefault, -1, ceiling })
        {
            using var accepted = await OpenTransferAsync(host, sender.ClientId, receiver.ClientId, timeoutMs);
            Assert.Equal(HttpStatusCode.OK, accepted.StatusCode);
        }
    }

    private static Task<HttpResponseMessage> OpenTransferAsync(IHost host, Guid senderId, Guid receiverId, int timeoutMs) =>
        RawHub.SendAsync(host, HttpMethod.Post, senderId, WireRoutes.TransfersSegment, token: null,
            new InitTransferRequest(receiverId, timeoutMs)).WaitAsync(TimeSpan.FromSeconds(30));
}
