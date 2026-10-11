using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.AspNetCore.TestHost;
using TransportR.Interfaces;
using TransportR.Protocol;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// A caller that is refused at <c>Connect</c> has only the message to go on, and a host cannot act
/// the same way on every refusal: an identity that holds no group has had its rights withdrawn,
/// whereas one the hub cannot resolve has a configuration problem.
/// </summary>
public class ConnectRefusalTests
{
    [Fact]
    public async Task Connect_OfAnIdentityThatHoldsNoGroup_IsRefusedWithAMessageThatSaysSo()
    {
        using var host = AuthenticatedHubServerFactory.CreateHost();
        var token = StubJwt.Create("withdrawn-subject", DateTime.UtcNow.AddHours(1));

        await using var connection = await StartAsync(host, token);
        var ex = await Assert.ThrowsAsync<HubException>(() => connection.InvokeAsync(nameof(ICommandHub.Connect), Guid.NewGuid(), null));

        Assert.Contains("no group", ex.Message, StringComparison.OrdinalIgnoreCase);
        // Advice to configure the hub is for whoever runs it, and this caller has a production token.
        Assert.DoesNotContain("dev mode", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Connect_OfAnIdentityTheHubCannotResolve_IsRefusedAsUnresolved()
    {
        // An authenticated caller that attests no subject: nothing to register it under.
        using var host = AuthenticatedHubServerFactory.CreateHost();
        var token = StubJwt.Create("", DateTime.UtcNow.AddHours(1), "some-group");

        await using var connection = await StartAsync(host, token);
        var ex = await Assert.ThrowsAsync<HubException>(() => connection.InvokeAsync(nameof(ICommandHub.Connect), Guid.NewGuid(), null));

        Assert.Contains("Unable to resolve client identity", ex.Message);
    }

    private static async Task<HubConnection> StartAsync(Microsoft.Extensions.Hosting.IHost host, string credential)
    {
        var handler = host.GetTestServer().CreateHandler();
        var connection = new HubConnectionBuilder()
            .WithUrl("http://localhost" + WireRoutes.CommandHub, options =>
            {
                options.HttpMessageHandlerFactory = _ => handler;
                options.Headers.Add("Authorization", $"{AuthenticatedHubServerFactory.Scheme} {credential}");
            })
            .Build();

        await connection.StartAsync();
        return connection;
    }
}
