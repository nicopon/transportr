using System.Net;
using System.Net.Http.Json;
using System.Security.Claims;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using TransportR.Hub.SignalR;
using TransportR.Hub.SignalR.Access;
using TransportR.Hub.SignalR.Configuration;
using TransportR.Hub.SignalR.Controllers;
using TransportR.Identity;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Protocol;
using TransportR.Services;
using TransportR.Tests.Infrastructure;
using Xunit;

namespace TransportR.Tests;

/// <summary>
/// The route a client presents a renewed token on: what the hub records from it, and everything it
/// refuses. The token is the host's to validate; these tests start from one the host accepted.
/// </summary>
public class TokenRenewalRouteTests
{
    private const string Owner = "owner-principal";
    private const string Group = AuthenticatedHubServerFactory.DefaultGroup;

    private sealed class Rules(Func<IReadOnlySet<string>, IReadOnlySet<string>, bool> canSendTo) : IFlowControlService
    {
        public bool CanSendTo(IReadOnlySet<string> sourceGroups, IReadOnlySet<string> targetGroups) =>
            canSendTo(sourceGroups, targetGroups);
    }

    private static async Task<Guid> RegisterAsync(IHost host, string subject, DateTime? expiry = null, params string[] groups)
    {
        var clientId = Guid.NewGuid();
        await host.Services.GetRequiredService<IStateStore>().SaveClientAsync(new ClientRegistration(
            clientId, $"conn-{clientId}", (groups.Length == 0 ? [Group] : groups).ToHashSet(), null,
            expiry ?? DateTime.UtcNow.AddSeconds(30), DateTime.UtcNow, Subject: subject));
        return clientId;
    }

    [Fact]
    public async Task Renew_RecordsTheExpiryOfTheNewToken_AndAnswersWithIt()
    {
        using var host = AuthenticatedHubServerFactory.CreateHost();
        var clientId = await RegisterAsync(host, Owner);
        var expiry = DateTime.UtcNow.AddHours(1);

        using var response = await RawHub.RenewAsync(host, clientId, StubJwt.Create(Owner, expiry, Group));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = await response.Content.ReadFromJsonAsync<TokenRenewalResponse>();
        var stored = await host.Services.GetRequiredService<IStateStore>().GetClientAsync(clientId);
        Assert.Equal(stored!.TokenExpiry, body!.TokenExpiry);
        Assert.True(Math.Abs((body.TokenExpiry!.Value - expiry).TotalSeconds) < 1);
    }

    [Fact]
    public async Task Renew_FollowsTheGroupsOfTheNewToken_ThroughDiscoveryAndTransfers()
    {
        // senders may reach receivers; a token that says the caller is something else takes that away
        // from both surfaces, with no new connection.
        using var host = AuthenticatedHubServerFactory.CreateHost(hub => hub.Services.AddSingleton<IFlowControlService>(
            new Rules((source, target) => source.Contains("senders") && target.Contains("receivers"))));
        var receiverId = await RegisterAsync(host, "receiver-principal", groups: "receivers");

        await using var connection = await RawHub.StartAsync(host, RawHub.Token(Owner, groups: "senders"));
        var callerId = Guid.NewGuid();
        await connection.InvokeAsync(nameof(ICommandHub.Connect), callerId, null);

        Assert.Equal(receiverId, Assert.Single(await connection.InvokeAsync<List<ClientInfo>>(nameof(ICommandHub.GetReceivers))).ClientId);

        using var response = await RawHub.RenewAsync(host, callerId, RawHub.Token(Owner, groups: "bystanders"));
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        Assert.Empty(await connection.InvokeAsync<List<ClientInfo>>(nameof(ICommandHub.GetReceivers)));
        var refusal = await Assert.ThrowsAsync<HubException>(
            () => connection.InvokeAsync<string>(nameof(ICommandHub.InitTransfer), receiverId, 10, 1000));
        Assert.Contains("not authorized", refusal.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Renew_UnderAnotherSubject_IsRefusedAsNotFound_AndChangesNothing()
    {
        using var host = AuthenticatedHubServerFactory.CreateHost();
        var clientId = await RegisterAsync(host, Owner);
        var store = host.Services.GetRequiredService<IStateStore>();
        var before = await store.GetClientAsync(clientId);

        using var response = await RawHub.RenewAsync(host, clientId, RawHub.Token("intruder-principal", groups: Group));

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.Equal(before, await store.GetClientAsync(clientId));
    }

    [Fact]
    public async Task Renew_OfAnUnknownClient_IsNotFound()
    {
        using var host = AuthenticatedHubServerFactory.CreateHost();

        using var response = await RawHub.RenewAsync(host, Guid.NewGuid(), RawHub.Token(Owner, groups: Group));

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task Renew_WithoutAValidToken_IsTheHostsToRefuse()
    {
        using var host = AuthenticatedHubServerFactory.CreateHost();
        var clientId = await RegisterAsync(host, Owner);

        using var anonymous = await RawHub.RenewAsync(host, clientId, token: null);
        using var expired = await RawHub.RenewAsync(host, clientId,
            StubJwt.Create(Owner, DateTime.UtcNow.AddMinutes(-1), Group));

        Assert.Equal(HttpStatusCode.Unauthorized, anonymous.StatusCode);
        Assert.Equal(HttpStatusCode.Unauthorized, expired.StatusCode);
    }

    [Fact]
    public async Task Renew_WithAValidTokenThatGrantsNothing_WithdrawsEveryRight()
    {
        // The identity provider took every group away. That is an answer to apply, not an error to
        // refuse: refused, the old groups would stand until the old token ran out.
        using var host = AuthenticatedHubServerFactory.CreateHost();
        var clientId = await RegisterAsync(host, Owner);

        using var response = await RawHub.RenewAsync(host, clientId, RawHub.Token(Owner));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Empty((await host.Services.GetRequiredService<IStateStore>().GetClientAsync(clientId))!.Groups);
    }

    [Fact]
    public async Task Renew_InDevMode_IsNotFound()
    {
        // Nothing is attested in dev mode, so there is nothing to renew.
        using var host = new HostBuilder()
            .ConfigureWebHost(web => web
                .UseTestServer()
                .ConfigureServices(services =>
                {
                    services.AddDataHub().UseDevMode(options => options.DefaultGroups = ["test"]).Build();
                })
                .Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints => endpoints.MapDataHub(requireAuth: false));
                }))
            .Start();
        var clientId = Guid.NewGuid();
        await host.Services.GetRequiredService<IStateStore>().SaveClientAsync(
            new ClientRegistration(clientId, "conn", [Group], null, null, DateTime.UtcNow));

        using var response = await RawHub.RenewAsync(host, clientId, RawHub.Token(Owner, groups: Group));

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task Renew_OfATokenTheHostAcceptedButTheHubHoldsToItsOwnClock_IsRefused()
    {
        // The host's JwtBearer tolerates a skew (five minutes by default) the hub does not, unless it
        // is told to: a token already past its expiry renews nothing.
        var clock = new FakeTimeProvider(new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var store = new InMemoryStateStore(timeProvider: clock);
        var clientId = Guid.NewGuid();
        await store.SaveClientAsync(new ClientRegistration(
            clientId, "conn", [Group], null, clock.GetUtcNow().UtcDateTime.AddMinutes(1), clock.GetUtcNow().UtcDateTime, Subject: Owner));

        var expiry = clock.GetUtcNow().UtcDateTime.AddMinutes(10);
        var reconciler = new RecordingReconciler();
        var controller = new ClientController(
            store,
            new JwtIdentityProvider(),
            new AccessPolicy(Options.Create(new AccessOptions()), clock),
            reconciler,
            NullLogger<ClientController>.Instance)
        {
            ControllerContext = new ControllerContext
            {
                HttpContext = new DefaultHttpContext
                {
                    User = new ClaimsPrincipal(new ClaimsIdentity(
                    [
                        new Claim("sub", Owner),
                        new Claim("scope", "transportr:group:" + Group),
                        new Claim("exp", new DateTimeOffset(expiry, TimeSpan.Zero).ToUnixTimeSeconds().ToString())
                    ], "test"))
                }
            }
        };

        Assert.IsType<OkObjectResult>(await controller.Renew(clientId));
        Assert.Equal([clientId], reconciler.Reconciled);

        clock.Advance(TimeSpan.FromMinutes(11));
        Assert.IsType<NotFoundResult>(await controller.Renew(clientId));
        Assert.Single(reconciler.Reconciled);
    }

    private sealed class RecordingReconciler : IAccessReconciler
    {
        public List<Guid> Reconciled { get; } = [];

        public Task ReconcileAsync(Guid clientId, CancellationToken cancellationToken = default)
        {
            Reconciled.Add(clientId);
            return Task.CompletedTask;
        }

        public Task ReconcileAllAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    }
}
