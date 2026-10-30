using Microsoft.Extensions.DependencyInjection;
using TransportR.Hub;
using TransportR.Interfaces;

namespace TransportR.Tests;

/// <summary>
/// <c>AddDataHub()</c>'s default <c>IHubProgressMonitor</c> is covered end-to-end by
/// <see cref="SignalRIntegrationTests.Transfer_DeliversEveryItem_WhenTheHostRegisteredNoProgressMonitor"/>;
/// this checks the other half - that a host's own registration is not shadowed by that default.
/// </summary>
public class DataHubProgressMonitorTests
{
    [Fact]
    public void AddDataHub_KeepsAHostRegisteredProgressMonitor()
    {
        var services = new ServiceCollection();
        var hostMonitor = new RecordingProgressMonitor();
        services.AddSingleton<IHubProgressMonitor>(hostMonitor);

        services.AddDataHub().UseDevMode().Build();

        using var provider = services.BuildServiceProvider();
        Assert.Same(hostMonitor, provider.GetRequiredService<IHubProgressMonitor>());
    }

    private sealed class RecordingProgressMonitor : IHubProgressMonitor
    {
        public void InitializeTransfer(string transferId) { }

        public void OnBytesTransferred(long bytes) { }

        public void CompleteTransfer(string transferId) { }
    }
}
