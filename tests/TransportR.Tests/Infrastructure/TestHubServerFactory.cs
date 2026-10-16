using Microsoft.AspNetCore.Builder;
using TransportR.Protocol;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TransportR.Hub.SignalR;

namespace TransportR.Tests.Infrastructure;

public static class TestHubServerFactory
{
    public const string HubUrl = "http://localhost" + WireRoutes.CommandHub;

    public static IHost CreateHost(Action<IServiceCollection>? configureServices = null)
    {
        return new HostBuilder()
            .ConfigureWebHost(webBuilder => Configure(webBuilder.UseTestServer(), configureServices))
            .Start();
    }

    /// <summary>
    /// The same hub on Kestrel, on the loopback interface. For a test whose outcome depends on how a
    /// cancelled request ends: in memory, a client's cancelled request completes only once the hub's
    /// handler has, whereas a socket ends it on the client's side.
    /// </summary>
    public static async Task<LoopbackHost> StartLoopbackAsync(Action<IServiceCollection>? configureServices = null)
    {
        var host = new HostBuilder()
            .ConfigureWebHost(webBuilder => Configure(webBuilder.UseKestrel().UseUrls("http://127.0.0.1:0"), configureServices))
            .Build();

        await host.StartAsync();

        var baseUrl = host.Services.GetRequiredService<IServer>()
            .Features.Get<IServerAddressesFeature>()!.Addresses.First();
        return new LoopbackHost(host, baseUrl);
    }

    private static void Configure(IWebHostBuilder webBuilder, Action<IServiceCollection>? configureServices = null)
    {
        webBuilder
            .ConfigureServices(services =>
            {
                services.AddLogging(logging => logging.AddConsole().SetMinimumLevel(LogLevel.Debug));

                // Add TransportR Hub services (InMemory by default)
                services.AddDataHub()
                        .UseDevMode(options =>
                        {
                            options.AllowAnonymous = true;
                            options.DefaultGroups = ["test"];
                        })
                        .Build();

                configureServices?.Invoke(services);
            })
            .Configure(app =>
            {
                app.UseRouting();
                // Map the Hub
                app.UseEndpoints(endpoints =>
                {
                    endpoints.MapDataHub(requireAuth: false);
                });
            });
    }
}

public sealed class LoopbackHost : IAsyncDisposable
{
    private readonly IHost _host;

    internal LoopbackHost(IHost host, string baseUrl)
    {
        _host = host;
        BaseUrl = baseUrl;
    }

    public string BaseUrl { get; }

    public IServiceProvider Services => _host.Services;

    public async ValueTask DisposeAsync()
    {
        await _host.StopAsync();
        _host.Dispose();
    }
}
