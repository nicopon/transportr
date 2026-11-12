using System.Net;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using TransportR.Hub;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Protocol;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// What <c>MapDataHub()</c> arms when the host says nothing: the mode <c>Build()</c> chose, so a hub
/// that validates tokens does not serve its data plane to callers that present none.
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
}
