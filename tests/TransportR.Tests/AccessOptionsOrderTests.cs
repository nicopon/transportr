using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using TransportR.Hub;

namespace TransportR.Tests;

/// <summary>
/// <c>AddDataHub()</c> sets no access option of its own, so a host's <c>Configure</c> holds on
/// either side of that call. The release notes tell a host so.
/// </summary>
public class AccessOptionsOrderTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ClientsMayOpenTransfers_HoldsOnEitherSideOfAddDataHub(bool setBefore)
    {
        var services = new ServiceCollection();

        if (setBefore) services.Configure<AccessOptions>(o => o.ClientsMayOpenTransfers = false);
        services.AddDataHub().UseDevMode().Build();
        if (!setBefore) services.Configure<AccessOptions>(o => o.ClientsMayOpenTransfers = false);

        using var provider = services.BuildServiceProvider();
        Assert.False(provider.GetRequiredService<IOptions<AccessOptions>>().Value.ClientsMayOpenTransfers);
    }
}
