using System.Net;
using System.Net.Http.Json;
using System.Net.ServerSentEvents;
using System.Text.Json;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Hosting;
using TransportR.Protocol;

namespace TransportR.Tests.Infrastructure;

/// <summary>
/// Bare requests of the control plane, and a bare event stream, authenticated with a token of the stub
/// scheme, for a test about what the hub does and not about what the client library does.
/// </summary>
public static class RawHub
{
    public static string Token(string subject, TimeSpan? lifetime = null, params string[] groups) =>
        StubJwt.Create(subject, DateTime.UtcNow + (lifetime ?? TimeSpan.FromHours(1)), groups);

    /// <summary>
    /// Opens <paramref name="clientId"/>'s event stream, which registers it. The answer's status says
    /// whether the hub did; a stream it accepted is read up to its first event, the registration.
    /// </summary>
    public static async Task<RawStream> OpenAsync(IHost host, Guid clientId, string? token, params string[] groups)
    {
        var query = $"{ControlPlane.HeartbeatParameter}=15000"
            + string.Concat(groups.Select(g => $"&{ControlPlane.GroupsParameter}={Uri.EscapeDataString(g)}"));
        var request = Request(HttpMethod.Get, clientId, $"{WireRoutes.EventsSegment}?{query}", token);

        var response = await host.GetTestClient().SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
        var stream = new RawStream(response);
        if (response.StatusCode == HttpStatusCode.OK)
        {
            var registered = await stream.NextEventAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(ControlPlane.Registered, registered?.EventType);
            stream.ConnectionId = JsonSerializer.Deserialize<RegisteredEvent>(registered!.Value.Data, ControlPlane.Json)!.ConnectionId;
        }

        return stream;
    }

    /// <summary>A request under <paramref name="clientId"/>'s path, with <paramref name="body"/> as JSON.</summary>
    public static Task<HttpResponseMessage> SendAsync(IHost host, HttpMethod method, Guid clientId, string path, string? token, object? body = null)
    {
        var request = Request(method, clientId, path, token);
        if (body != null)
        {
            request.Content = JsonContent.Create(body, body.GetType(), options: ControlPlane.Json);
        }

        return host.GetTestClient().SendAsync(request);
    }

    public static Task<HttpResponseMessage> RenewAsync(IHost host, Guid clientId, string? token) =>
        SendAsync(host, HttpMethod.Put, clientId, WireRoutes.TokenSegment, token);

    private static HttpRequestMessage Request(HttpMethod method, Guid clientId, string path, string? token)
    {
        var request = new HttpRequestMessage(method, $"http://localhost{WireRoutes.ClientBase}/{clientId}/{path}");
        if (token != null)
        {
            request.Headers.Add("Authorization", "Bearer " + token);
        }

        return request;
    }
}

/// <summary>A client's event stream as the hub answered it. Disposing it is the client leaving.</summary>
public sealed class RawStream : IAsyncDisposable
{
    private readonly HttpResponseMessage _response;
    private IAsyncEnumerator<SseItem<string>>? _events;

    internal RawStream(HttpResponseMessage response)
    {
        _response = response;
    }

    public HttpStatusCode Status => _response.StatusCode;

    /// <summary>The id the hub registered the client under, once it has.</summary>
    public string? ConnectionId { get; internal set; }

    /// <summary>The next event but the keepalives, or null when the hub ended the stream first.</summary>
    public async Task<SseItem<string>?> NextEventAsync(TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        _events ??= SseParser.Create(await _response.Content.ReadAsStreamAsync()).EnumerateAsync().GetAsyncEnumerator();

        while (await _events.MoveNextAsync().AsTask().WaitAsync(cts.Token))
        {
            if (_events.Current.EventType != ControlPlane.KeepAlive) return _events.Current;
        }

        return null;
    }

    public async ValueTask DisposeAsync()
    {
        if (_events != null)
        {
            try { await _events.DisposeAsync(); } catch (Exception) { /* the stream is gone either way */ }
        }

        _response.Dispose();
    }
}
