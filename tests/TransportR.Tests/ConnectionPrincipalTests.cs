using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TransportR.Hub.SignalR.Access;
using TransportR.Hub.SignalR.Configuration;
using TransportR.Hub.SignalR.Hubs;
using TransportR.Hub.SignalR.Services;
using TransportR.Interfaces;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// What an open SignalR connection does with the token it was opened with: <c>Context.User</c> keeps
/// the expiry it was opened with, whatever the client presents afterward. A renewed token cannot
/// reach the hub over the connection, which is why renewal is a request of its own.
/// </summary>
/// <remarks>
/// Against a real socket: the transport decides how often a token is presented, and a
/// <c>TestServer</c> handler is not one a client would use in production.
/// </remarks>
public class ConnectionPrincipalTests
{
    private class ProbeHub : CommandHub
    {
        public ProbeHub(
            IStateStore stateStore,
            ITransferManager transferManager,
            IClientIdentityProvider identityProvider,
            IBridgeManager bridgeManager,
            SignalRTransferInitiator initiator,
            AccessPolicy policy,
            ConnectionRegistry connections,
            IOptions<AffinityOptions> affinityOptions,
            ILogger<CommandHub> logger,
            TimeProvider timeProvider)
            : base(stateStore, transferManager, identityProvider, bridgeManager, initiator, policy, connections, affinityOptions, logger, timeProvider)
        {
        }

        public string? TokenExpiry() => Context.User?.FindFirst("exp")?.Value;
    }

    [Theory]
    [InlineData(HttpTransportType.WebSockets)]
    [InlineData(HttpTransportType.ServerSentEvents)]
    [InlineData(HttpTransportType.LongPolling)]
    public async Task OpenConnection_KeepsThePrincipalItWasOpenedWith(HttpTransportType transport)
    {
        await using var host = await AuthenticatedHubServerFactory.StartLoopbackAsync(hub => hub.UseHub<ProbeHub>());

        // A new token, with a new expiry, on every call, and a lifetime inside the client's refresh
        // threshold: whatever asks the provider for a token gets a different one.
        var tokens = new TokenSource("probe-principal", TimeSpan.FromMinutes(1));
        await using var client = TestClients.Create(null, host.BaseUrl, tokens.GetAsync,
            configureConnection: options => options.Transports = transport);

        await client.ConnectAsync();

        var first = await client.ControlConnection.InvokeAsync<string?>(nameof(ProbeHub.TokenExpiry));

        // exp has a one-second grain: a token asked for sooner than that would not differ.
        await Task.Delay(TimeSpan.FromSeconds(2.2));

        var second = await client.ControlConnection.InvokeAsync<string?>(nameof(ProbeHub.TokenExpiry));

        Assert.NotNull(first);
        Assert.Equal(first, second);
    }
}
