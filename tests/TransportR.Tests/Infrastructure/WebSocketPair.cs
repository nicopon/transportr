using System.Net.WebSockets;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace TransportR.Tests.Infrastructure;

/// <summary>
/// Both ends of one WebSocket over the loopback interface, on Kestrel: a <c>TestServer</c> socket raises
/// other exceptions, has no backpressure and ignores an aborted connection, which is most of what a test
/// of a stream over a WebSocket is about.
/// </summary>
internal sealed class WebSocketPair : IAsyncDisposable
{
    private readonly IHost _host;
    private readonly TaskCompletionSource _release;

    private WebSocketPair(IHost host, TaskCompletionSource release, ClientWebSocket client, WebSocket server)
    {
        _host = host;
        _release = release;
        Client = client;
        Server = server;
    }

    public ClientWebSocket Client { get; }

    public WebSocket Server { get; }

    public static async Task<WebSocketPair> OpenAsync()
    {
        var accepted = new TaskCompletionSource<WebSocket>(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var host = new HostBuilder()
            .ConfigureWebHost(web => web
                .UseKestrel()
                .UseUrls("http://127.0.0.1:0")
                .Configure(app =>
                {
                    app.UseWebSockets();
                    app.Run(async context =>
                    {
                        using var socket = await context.WebSockets.AcceptWebSocketAsync();
                        accepted.TrySetResult(socket);
                        await release.Task;
                    });
                }))
            .Build();
        await host.StartAsync();

        var address = host.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        var client = new ClientWebSocket();
        await client.ConnectAsync(new Uri(address.Replace("http://", "ws://")), CancellationToken.None);

        return new WebSocketPair(host, release, client, await accepted.Task);
    }

    public async ValueTask DisposeAsync()
    {
        Client.Abort();
        Client.Dispose();
        _release.TrySetResult();
        await _host.StopAsync();
        _host.Dispose();
    }
}
