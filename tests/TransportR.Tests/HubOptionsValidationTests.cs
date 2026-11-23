using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using TransportR.Hub;
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
    [InlineData(300, 2, null, 300, 3600, "TransferInactivityTimeoutSeconds")]
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

    [Fact]
    public void AnInactivityTimeoutThatLeavesAHeartbeatIntervalToAnnounce_IsAccepted()
    {
        // At 2 s no announced interval is both 1 s or more and under half of it, and every stream
        // would be refused; 3 s leaves one.
        using var host = Start(bridge: 300, inactivity: 3, stalled: null, grace: 300, connectedTtl: 3600);

        var plane = host.Services.GetRequiredService<IOptions<DataPlaneConfiguration>>().Value;
        Assert.Equal(3, plane.TransferInactivityTimeoutSeconds);
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

    [Fact]
    public void FluentConfigureHelpers_ApplyOptions_AndRefuseNullDelegates()
    {
        var services = new ServiceCollection();
        var builder = services.AddDataHub();

        Assert.Throws<ArgumentNullException>(() => builder.ConfigureDataPlane(null!));
        Assert.Throws<ArgumentNullException>(() => builder.ConfigureAccess(null!));
        Assert.Throws<ArgumentNullException>(() => builder.ConfigureClientRegistry(null!));

        using var host = TestHubServerFactory.CreateHost(sc => sc
            .AddDataHub()
            .UseDevMode()
            .ConfigureDataPlane(o =>
            {
                o.BridgeTimeoutSeconds = 120;
                o.TransferInactivityTimeoutSeconds = 45;
                o.StalledProgressTimeoutSeconds = 200;
            })
            .ConfigureAccess(o =>
            {
                o.AccessSweepIntervalSeconds = 15;
                o.MaxOpenTransfersPerClient = 64;
            })
            .ConfigureClientRegistry(o =>
            {
                o.DisconnectedGraceSeconds = 120;
                o.ConnectedTtlSeconds = 1800;
            }));

        var plane = host.Services.GetRequiredService<IOptions<DataPlaneConfiguration>>().Value;
        var access = host.Services.GetRequiredService<IOptions<AccessOptions>>().Value;
        var registry = host.Services.GetRequiredService<IOptions<ClientRegistryConfiguration>>().Value;

        Assert.Equal(120, plane.BridgeTimeoutSeconds);
        Assert.Equal(45, plane.TransferInactivityTimeoutSeconds);
        Assert.Equal(200, plane.StalledProgressTimeoutSeconds);
        Assert.Equal(15, access.AccessSweepIntervalSeconds);
        Assert.Equal(64, access.MaxOpenTransfersPerClient);
        Assert.Equal(120, registry.DisconnectedGraceSeconds);
        Assert.Equal(1800, registry.ConnectedTtlSeconds);
    }

    [Fact]
    public void FluentConfigureHelpers_TriggerStartupValidationOnInvalidValues()
    {
        var failure = Record.Exception(() => TestHubServerFactory.CreateHost(services => services
            .AddDataHub()
            .UseDevMode()
            .ConfigureDataPlane(o => o.BridgeTimeoutSeconds = 0)));

        var refused = Assert.IsType<OptionsValidationException>(failure);
        Assert.Contains("BridgeTimeoutSeconds", refused.Message);
    }
}
