using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using TransportR.Hub;
using TransportR.Hub.Identity;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// Which <see cref="IClientIdentityProvider"/> serves, and what <c>MapDataHub()</c> arms in
/// consequence: the provider a host registers wins on either side of <c>AddDataHub()</c>, the
/// development one only when the host asked for it.
/// </summary>
public class IdentityProviderRegistrationTests
{
    [Fact]
    public void AHostProviderRegisteredBeforeAddDataHub_IsTheOneThatServes()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IClientIdentityProvider, HostIdentityProvider>();

        services.AddDataHub();

        using var provider = services.BuildServiceProvider();
        Assert.IsType<HostIdentityProvider>(provider.GetRequiredService<IClientIdentityProvider>());
    }

    [Fact]
    public void AHostProviderRegisteredAfterAddDataHub_IsTheOneThatServes()
    {
        var services = new ServiceCollection();

        services.AddDataHub();
        services.AddSingleton<IClientIdentityProvider, HostIdentityProvider>();

        using var provider = services.BuildServiceProvider();
        Assert.IsType<HostIdentityProvider>(provider.GetRequiredService<IClientIdentityProvider>());
    }

    [Fact]
    public void AddDataHub_AloneRegistersTheTokenValidatingProvider()
    {
        var services = new ServiceCollection();

        services.AddDataHub();

        using var provider = services.BuildServiceProvider();
        Assert.IsType<JwtIdentityProvider>(provider.GetRequiredService<IClientIdentityProvider>());
    }

    [Fact]
    public void UseDevMode_RegistersTheDevelopmentProviderWithTheHostsOptions()
    {
        var services = new ServiceCollection();

        services.AddDataHub().UseDevMode(options => options.AllowAnonymous = true);

        using var provider = services.BuildServiceProvider();
        Assert.IsType<DevIdentityProvider>(provider.GetRequiredService<IClientIdentityProvider>());
        Assert.True(provider.GetRequiredService<DevIdentityOptions>().AllowAnonymous);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AHostProvider_MappedWithoutArgument_ChallengesAnAnonymousStreamRequest(bool registeredBeforeAddDataHub)
    {
        using var host = AuthenticatedHubServerFactory.CreateHost(
            mapHub: endpoints => endpoints.MapDataHub(),
            configureBeforeHub: registeredBeforeAddDataHub
                ? services => services.AddSingleton<IClientIdentityProvider, HostIdentityProvider>()
                : null,
            configureServices: registeredBeforeAddDataHub
                ? null
                : services => services.AddSingleton<IClientIdentityProvider, HostIdentityProvider>());
        Assert.IsType<HostIdentityProvider>(host.Services.GetRequiredService<IClientIdentityProvider>());
        using var client = host.GetTestClient();

        using var response = await client.GetAsync($"http://localhost{WireRoutes.StreamBase}/any-transfer/receive")
            .WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task ADevModeHub_MappedWithoutArgument_LeavesAnAnonymousStreamRequestUnchallenged()
    {
        using var host = new HostBuilder()
            .ConfigureWebHost(web => web
                .UseTestServer()
                .ConfigureServices(services => services.AddDataHub().UseDevMode())
                .Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints => endpoints.MapDataHub());
                }))
            .Start();
        using var client = host.GetTestClient();

        using var response = await client.GetAsync($"http://localhost{WireRoutes.StreamBase}/any-transfer/receive")
            .WaitAsync(TimeSpan.FromSeconds(10));

        Assert.NotEqual(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    private sealed class HostIdentityProvider : IClientIdentityProvider
    {
        public ClientIdentity? ResolveIdentity(
            System.Security.Claims.ClaimsPrincipal? user,
            Guid? providedClientId,
            IEnumerable<string>? providedGroups) => null;
    }
}
