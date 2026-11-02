using System.Net;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// A caller refused at registration is told nothing more than that, as on every other route. Whoever
/// runs the hub has to tell the causes apart: an identity that holds no group has had its rights
/// withdrawn, whereas one the hub cannot resolve has a configuration problem.
/// </summary>
public class ConnectRefusalTests
{
    [Fact]
    public async Task Connect_OfAnIdentityThatHoldsNoGroup_IsRefused_AndTheHubLogSaysSo()
    {
        var hubLogs = new LogRecorder();
        using var host = AuthenticatedHubServerFactory.CreateHost(configureServices: s => s.AddLogging(l => l.AddProvider(hubLogs)));
        var token = StubJwt.Create("withdrawn-subject", DateTime.UtcNow.AddHours(1));

        await using var stream = await RawHub.OpenAsync(host, Guid.NewGuid(), token);

        Assert.Equal(HttpStatusCode.NotFound, stream.Status);
        Assert.Contains(hubLogs.MessagesFrom(LogLevel.Warning), m => m.Contains("holds no group"));
    }

    [Fact]
    public async Task Connect_OfAnIdentityTheHubCannotResolve_IsRefused_AndTheHubLogSaysSo()
    {
        // An authenticated caller that attests no subject: nothing to register it under.
        var hubLogs = new LogRecorder();
        using var host = AuthenticatedHubServerFactory.CreateHost(configureServices: s => s.AddLogging(l => l.AddProvider(hubLogs)));
        var token = StubJwt.Create("", DateTime.UtcNow.AddHours(1), "some-group");

        await using var stream = await RawHub.OpenAsync(host, Guid.NewGuid(), token);

        Assert.Equal(HttpStatusCode.NotFound, stream.Status);
        Assert.Contains(hubLogs.MessagesFrom(LogLevel.Warning), m => m.Contains("unable to resolve a client identity"));
    }
}
