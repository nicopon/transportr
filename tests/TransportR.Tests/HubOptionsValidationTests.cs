using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using TransportR.Configuration;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// A timing the hub cannot run with is refused when the host starts, not discovered when a
/// transfer meets it.
/// </summary>
public class HubOptionsValidationTests
{
    [Theory]
    [InlineData(300, 60, 300, 300, 3600, "StalledProgressTimeoutSeconds")]
    [InlineData(300, 60, 120, 300, 3600, "StalledProgressTimeoutSeconds")]
    [InlineData(0, 60, null, 300, 3600, "BridgeTimeoutSeconds")]
    [InlineData(300, 0, null, 300, 3600, "TransferInactivityTimeoutSeconds")]
    [InlineData(300, 60, null, 0, 3600, "DisconnectedGraceSeconds")]
    [InlineData(300, 60, null, 300, -1, "ConnectedTtlSeconds")]
    public void ATimingTheHubCannotRunWith_IsRefusedWhenTheHostStarts(
        int bridge, int inactivity, int? stalled, int grace, int connectedTtl, string named)
    {
        var failure = Record.Exception(() => Start(bridge, inactivity, stalled, grace, connectedTtl));

        var refused = Assert.IsType<OptionsValidationException>(failure);
        Assert.Contains(named, refused.Message);
    }

    [Fact]
    public void AStalledProgressBoundPastTheBridgeTimeout_IsAccepted()
    {
        using var host = Start(bridge: 300, inactivity: 60, stalled: 301, grace: 300, connectedTtl: 3600);

        var plane = host.Services.GetRequiredService<IOptions<DataPlaneConfiguration>>().Value;
        Assert.Equal(301, plane.StalledProgressTimeoutSeconds);
    }

    private static IHost Start(int bridge, int inactivity, int? stalled, int grace, int connectedTtl) =>
        TestHubServerFactory.CreateHost(services =>
        {
            services.Configure<DataPlaneConfiguration>(o =>
            {
                o.BridgeTimeoutSeconds = bridge;
                o.TransferInactivityTimeoutSeconds = inactivity;
                o.StalledProgressTimeoutSeconds = stalled;
            });
            services.Configure<ClientRegistryConfiguration>(o =>
            {
                o.DisconnectedGraceSeconds = grace;
                o.ConnectedTtlSeconds = connectedTtl;
            });
        });
}
