using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using TransportR.Interfaces;
using TransportR.Models;
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

    private static Task<HubConnection> StartAsync(IHost host, string token) => RawHub.StartAsync(host, token);

    [Fact]
    public async Task Connect_WithATokenThatGrantsNothing_IsRefused()
    {
        // The token is valid and the subject is known: the one thing missing is a right to act.
        using var host = AuthenticatedHubServerFactory.CreateHost();
        var clientId = Guid.NewGuid();

        await using var connection = await StartAsync(host, Token());
        await Assert.ThrowsAsync<HubException>(
            () => connection.InvokeAsync(nameof(ICommandHub.Connect), clientId, null));

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

        await using var connection = await StartAsync(host, Token(AuthenticatedHubServerFactory.DefaultGroup));
        await connection.InvokeAsync(nameof(ICommandHub.Connect), Guid.NewGuid(), null);

        var receivers = await connection.InvokeAsync<List<ClientInfo>>(nameof(ICommandHub.GetReceivers));

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
        await using var connection = await StartAsync(host, Token(AuthenticatedHubServerFactory.DefaultGroup));
        await connection.InvokeAsync(nameof(ICommandHub.Connect), callerId, null);
        Assert.Single(await connection.InvokeAsync<List<ClientInfo>>(nameof(ICommandHub.GetReceivers)));

        var registered = (await store.GetClientAsync(callerId))!;
        await store.SaveClientAsync(registered with { TokenExpiry = DateTime.UtcNow.AddMinutes(-1) });

        Assert.Empty(await connection.InvokeAsync<List<ClientInfo>>(nameof(ICommandHub.GetReceivers)));
    }

    private static ClientRegistration Registration(Guid clientId, string connectionId, string subject) =>
        new(clientId, connectionId, [AuthenticatedHubServerFactory.DefaultGroup], null, null, DateTime.UtcNow, Subject: subject);
}
