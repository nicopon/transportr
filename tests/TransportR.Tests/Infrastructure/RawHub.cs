using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Hosting;
using TransportR.Protocol;

namespace TransportR.Tests.Infrastructure;

/// <summary>
/// A bare hub connection and a bare request, authenticated with a token of the stub scheme, for a
/// test about what the hub does and not about what the client library does.
/// </summary>
public static class RawHub
{
    public static string Token(string subject, TimeSpan? lifetime = null, params string[] groups) =>
        StubJwt.Create(subject, DateTime.UtcNow + (lifetime ?? TimeSpan.FromHours(1)), groups);

    public static async Task<HubConnection> StartAsync(IHost host, string token)
    {
        var handler = host.GetTestServer().CreateHandler();
        var connection = new HubConnectionBuilder()
            .WithUrl("http://localhost" + WireRoutes.CommandHub, options =>
            {
                options.HttpMessageHandlerFactory = _ => handler;
                options.AccessTokenProvider = () => Task.FromResult<string?>(token);
            })
            .Build();

        await connection.StartAsync();
        return connection;
    }

    public static Task<HttpResponseMessage> RenewAsync(IHost host, Guid clientId, string? token)
    {
        var request = new HttpRequestMessage(HttpMethod.Put, $"http://localhost{WireRoutes.ClientBase}/{clientId}/token");
        if (token != null)
        {
            request.Headers.Add("Authorization", "Bearer " + token);
        }

        return host.GetTestClient().SendAsync(request);
    }
}
