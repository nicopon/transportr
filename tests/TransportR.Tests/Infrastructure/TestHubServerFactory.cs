using Microsoft.AspNetCore.Builder;
using TransportR.Protocol;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TransportR.Hub.SignalR;
using TransportR.Interfaces;

namespace TransportR.Tests.Infrastructure;

public static class TestHubServerFactory
{
    public const string HubUrl = "http://localhost" + WireRoutes.CommandHub;

    public static IHost CreateHost()
    {
        return new HostBuilder()
            .ConfigureWebHost(webBuilder =>
            {
                webBuilder
                    .UseTestServer()
                    .ConfigureServices(services =>
                    {
                        services.AddLogging(logging => logging.AddConsole().SetMinimumLevel(LogLevel.Debug));
                        services.AddSingleton<IHubProgressMonitor, SilentProgressMonitor>();

                        // Add TransportR Hub services (InMemory by default)
                        services.AddDataHub()
                                .UseDevMode(options => 
                                {
                                    options.AllowAnonymous = true;
                                    options.DefaultGroups = ["test"];
                                })
                                .Build();
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
            })
            .Start();
    }
}
