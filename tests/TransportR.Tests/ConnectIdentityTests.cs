using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using TransportR.Interfaces;
using TransportR.Protocol;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// Under a subject that is not a Guid the client picks its own id, and <c>GetReceivers</c> shows
/// every id to every authorized sender. A registration must therefore stay with the subject that
/// made it.
/// </summary>
public class ConnectIdentityTests
{
    private const string Owner = "owner-principal";
    private const string Intruder = "intruder-principal";

    [Fact]
    public async Task Connect_UnderAnotherSubject_IsRefused()
    {
        using var host = AuthenticatedHubServerFactory.CreateHost();
        var clientId = Guid.NewGuid();

        await using var owner = await StartAsync(host, Owner);
        await owner.InvokeAsync(nameof(ICommandHub.Connect), clientId, null);

        await using var intruder = await StartAsync(host, Intruder);
        var ex = await Assert.ThrowsAsync<HubException>(
            () => intruder.InvokeAsync(nameof(ICommandHub.Connect), clientId, null));

        Assert.Contains("already in use", ex.Message, StringComparison.OrdinalIgnoreCase);

        var registration = await host.Services.GetRequiredService<IStateStore>().GetClientAsync(clientId);
        Assert.Equal(Owner, registration?.Subject);
    }

    [Fact]
    public async Task Connect_UnderTheSameSubject_TakesTheRegistrationOver()
    {
        // A reconnection arrives on a new connection under the same subject, and must win.
        using var host = AuthenticatedHubServerFactory.CreateHost();
        var clientId = Guid.NewGuid();

        await using var first = await StartAsync(host, Owner);
        await first.InvokeAsync(nameof(ICommandHub.Connect), clientId, null);

        await using var second = await StartAsync(host, Owner);
        await second.InvokeAsync(nameof(ICommandHub.Connect), clientId, null);

        var store = host.Services.GetRequiredService<IStateStore>();
        Assert.Equal(clientId, (await store.GetClientByConnectionIdAsync(second.ConnectionId!))?.ClientId);
    }

    [Fact]
    public async Task Connect_OverARegistrationInItsGracePeriod_IsRefused()
    {
        // A dropped registration is kept so its owner can reclaim it on reconnect. That is exactly
        // the window in which someone else must not.
        using var host = AuthenticatedHubServerFactory.CreateHost();
        var store = host.Services.GetRequiredService<IStateStore>();
        var clientId = Guid.NewGuid();

        var owner = await StartAsync(host, Owner);
        await owner.InvokeAsync(nameof(ICommandHub.Connect), clientId, null);
        await owner.DisposeAsync();

        var deadline = DateTime.UtcNow.AddSeconds(10);
        while ((await store.GetClientAsync(clientId))?.DisconnectedAtUtc == null && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50);
        }
        Assert.NotNull((await store.GetClientAsync(clientId))?.DisconnectedAtUtc);

        await using var intruder = await StartAsync(host, Intruder);
        await Assert.ThrowsAsync<HubException>(
            () => intruder.InvokeAsync(nameof(ICommandHub.Connect), clientId, null));
    }

    private static async Task<HubConnection> StartAsync(Microsoft.Extensions.Hosting.IHost host, string subject)
    {
        var handler = host.GetTestServer().CreateHandler();
        var connection = new HubConnectionBuilder()
            .WithUrl("http://localhost" + WireRoutes.CommandHub, options =>
            {
                options.HttpMessageHandlerFactory = _ => handler;
                options.Headers.Add("Authorization", $"{AuthenticatedHubServerFactory.Scheme} {subject}");
            })
            .Build();

        await connection.StartAsync();
        return connection;
    }
}
