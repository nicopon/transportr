using System.Net;
using Microsoft.Extensions.DependencyInjection;
using TransportR.Interfaces;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// The client picks its own id, whatever its subject, and discovery shows every id to every
/// authorized sender. A registration must therefore stay with the subject that made it.
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

        await using var owner = await OpenAsync(host, clientId, Owner);
        Assert.Equal(HttpStatusCode.OK, owner.Status);

        await using var intruder = await OpenAsync(host, clientId, Intruder);
        Assert.Equal(HttpStatusCode.NotFound, intruder.Status);

        var registration = await host.Services.GetRequiredService<IStateStore>().GetClientAsync(clientId);
        Assert.Equal(Owner, registration?.Subject);
    }

    [Fact]
    public async Task Connect_UnderTheSameSubject_TakesTheRegistrationOver()
    {
        // A reconnection arrives on a new connection under the same subject, and must win.
        using var host = AuthenticatedHubServerFactory.CreateHost();
        var clientId = Guid.NewGuid();

        await using var first = await OpenAsync(host, clientId, Owner);
        await using var second = await OpenAsync(host, clientId, Owner);
        Assert.Equal(HttpStatusCode.OK, second.Status);

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

        var owner = await OpenAsync(host, clientId, Owner);
        await owner.DisposeAsync();

        var deadline = DateTime.UtcNow.AddSeconds(10);
        while ((await store.GetClientAsync(clientId))?.DisconnectedAtUtc == null && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50);
        }
        Assert.NotNull((await store.GetClientAsync(clientId))?.DisconnectedAtUtc);

        await using var intruder = await OpenAsync(host, clientId, Intruder);
        Assert.Equal(HttpStatusCode.NotFound, intruder.Status);
    }

    [Fact]
    public async Task Connect_UnderAGuidSubject_RegistersTheIdTheClientNames()
    {
        // The client never learns its token's subject: an id taken from it would be one that no
        // request of the client names, its renewal included.
        using var host = AuthenticatedHubServerFactory.CreateHost();
        var store = host.Services.GetRequiredService<IStateStore>();
        var subject = Guid.NewGuid().ToString();
        var clientId = Guid.NewGuid();

        await using var stream = await OpenAsync(host, clientId, subject);
        Assert.Equal(HttpStatusCode.OK, stream.Status);
        Assert.Equal(subject, (await store.GetClientAsync(clientId))?.Subject);
        Assert.Null(await store.GetClientAsync(Guid.Parse(subject)));

        using var renewal = await RawHub.RenewAsync(host, clientId,
            RawHub.Token(subject, groups: AuthenticatedHubServerFactory.DefaultGroup));
        Assert.Equal(HttpStatusCode.OK, renewal.StatusCode);
    }

    private static Task<RawStream> OpenAsync(Microsoft.Extensions.Hosting.IHost host, Guid clientId, string subject) =>
        RawHub.OpenAsync(host, clientId, RawHub.Token(subject, groups: AuthenticatedHubServerFactory.DefaultGroup));
}
