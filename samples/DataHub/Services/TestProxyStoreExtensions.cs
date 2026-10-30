using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using TransportR.Hub.SignalR;
using TransportR.Interfaces;

namespace DataHub.Services;

public static class TestProxyStoreExtensions
{
    /// <summary>
    /// Keeps the hub's state in the TestProxy, which every hub instance behind it shares.
    /// </summary>
    /// <param name="proxyUrl">URL of the TestProxy (e.g., http://localhost:5000).</param>
    public static DataHubBuilder UseTestProxyStore(this DataHubBuilder builder, string proxyUrl)
    {
        builder.Services.Replace(ServiceDescriptor.Singleton<IStateStore>(new TestProxyStateStore(proxyUrl)));
        return builder;
    }
}
