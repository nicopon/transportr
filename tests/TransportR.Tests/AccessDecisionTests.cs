using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using TransportR.Hub;
using TransportR.Hub.Interfaces;
using TransportR.Protocol;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// What the hub's surfaces do with the one decision: a token that grants nothing, a caller whose
/// token has run out, a target who has lost its rights.
/// </summary>
public class AccessDecisionTests
{
    private const string Subject = "decision-principal";

    private static string Token(params string[] groups) => RawHub.Token(Subject, groups: groups);

    private static async Task<List<ClientInfo>> ReceiversAsync(IHost host, Guid clientId, string token)
    {
        using var response = await RawHub.SendAsync(host, HttpMethod.Get, clientId, WireRoutes.ReceiversSegment, token);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        return (await response.Content.ReadFromJsonAsync<List<ClientInfo>>(ControlPlane.Json))!;
    }

    [Fact]
    public async Task Connect_WithATokenThatGrantsNothing_IsRefused()
    {
        // The token is valid and the subject is known: the one thing missing is a right to act.
        using var host = AuthenticatedHubServerFactory.CreateHost();
        var clientId = Guid.NewGuid();

        await using var stream = await RawHub.OpenAsync(host, clientId, Token());
        Assert.Equal(HttpStatusCode.NotFound, stream.Status);

        Assert.Null(await host.Services.GetRequiredService<IStateStore>().GetClientAsync(clientId));
    }

    [Fact]
    public async Task Stream_WithATokenThatGrantsNothing_IsRefusedAsNotFound()
    {
        // The same subject the transfer was opened for, with every group taken away since.
        using var host = AuthenticatedHubServerFactory.CreateHost();
        var senderId = Guid.NewGuid();
        var receiverId = Guid.NewGuid();
        var store = host.Services.GetRequiredService<IStateStore>();
        await store.SaveClientAsync(Registration(senderId, "conn-sender", "sender-principal"));
        await store.SaveClientAsync(Registration(receiverId, "conn-receiver", Subject));
        await host.Services.GetRequiredService<ITransferManager>().RegisterParticipantsAsync("t-1", senderId, receiverId);

        using var client = host.GetTestClient();
        var request = new HttpRequestMessage(HttpMethod.Get, $"http://localhost{WireRoutes.StreamBase}/t-1/receive");
        request.Headers.Add("X-Connection-Type", nameof(ConnectionType.Data));
        request.Headers.Add("Authorization", "Bearer " + Token());

        // A stream wrongly admitted waits for a peer that never comes, so the request is bounded: the
        // test then fails in seconds instead of waiting for the host to give up on it.
        using var abort = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        try
        {
            using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, abort.Token);
            Assert.Equal(System.Net.HttpStatusCode.NotFound, response.StatusCode);
        }
        catch (OperationCanceledException)
        {
            Assert.Fail("The stream was admitted: nothing answered the request.");
        }
    }

    [Fact]
    public async Task GetReceivers_ListsWhomTheCallerMayReach_AndNotAClientWithoutRights()
    {
        using var host = AuthenticatedHubServerFactory.CreateHost();
        var store = host.Services.GetRequiredService<IStateStore>();

        var withRights = Guid.NewGuid();
        var withoutRights = Guid.NewGuid();
        var expired = Guid.NewGuid();
        await store.SaveClientAsync(Registration(withRights, "c1", "a"));
        await store.SaveClientAsync(Registration(withoutRights, "c2", "b") with { Groups = [] });
        await store.SaveClientAsync(Registration(expired, "c3", "c") with { TokenExpiry = DateTime.UtcNow.AddMinutes(-1) });

        var callerId = Guid.NewGuid();
        var token = Token(AuthenticatedHubServerFactory.DefaultGroup);
        await using var stream = await RawHub.OpenAsync(host, callerId, token);

        var receivers = await ReceiversAsync(host, callerId, token);

        // A client with no group is out of discovery; one whose token has run out stays, because a
        // caller that finds nobody fails whereas one that tries a dead client retries.
        Assert.Equal(new[] { withRights, expired }.Order(), receivers.Select(r => r.ClientId).Order());
    }

    [Fact]
    public async Task GetReceivers_ForACallerWhoseTokenHasRunOut_ListsNobody()
    {
        using var host = AuthenticatedHubServerFactory.CreateHost();
        var store = host.Services.GetRequiredService<IStateStore>();
        await store.SaveClientAsync(Registration(Guid.NewGuid(), "other", "other-principal"));

        var callerId = Guid.NewGuid();
        var token = Token(AuthenticatedHubServerFactory.DefaultGroup);
        await using var stream = await RawHub.OpenAsync(host, callerId, token);
        Assert.Single(await ReceiversAsync(host, callerId, token));

        var registered = (await store.GetClientAsync(callerId))!;
        await store.SaveClientAsync(registered with { TokenExpiry = DateTime.UtcNow.AddMinutes(-1) });

        Assert.Empty(await ReceiversAsync(host, callerId, token));
    }

    private static ClientRegistration Registration(Guid clientId, string connectionId, string subject) =>
        new(clientId, connectionId, [AuthenticatedHubServerFactory.DefaultGroup], null, null, DateTime.UtcNow, Subject: subject);
}
