using System.Net;
using System.Net.WebSockets;
using Microsoft.Extensions.Logging;
using TransportR.Client.Sessions;
using TransportR.Protocol;

namespace TransportR.Client.Internal;

/// <summary>
/// Opens a WebSocket on the data plane and proves that the path carries it, for a transfer's write
/// stream and for the write-transport probe alike: the probe is worth only what a stream would find.
/// </summary>
internal static class ProvenWebSocket
{
    /// <summary>A proven socket, or why the path does not carry one.</summary>
    public readonly record struct Opening(WebSocket? Socket, string? Unavailable);

    /// <summary>
    /// Opens a socket on <paramref name="uri"/> and proves it, the upgrade and the proof together within
    /// <paramref name="bound"/>. Anything short of a proven socket that is not the hub's own verdict says
    /// the path does not carry one. A token refused with a 401 is renewed once.
    /// </summary>
    /// <param name="newSocket">The socket to open, its request headers set, given the access token to present.</param>
    /// <exception cref="StreamRefusedException">The hub refused the request, or refused the renewed token too.</exception>
    public static async Task<Opening> OpenAsync(
        IStreamTransport transport,
        Uri uri,
        Func<string?, ClientWebSocket> newSocket,
        TimeSpan bound,
        ILogger logger,
        CancellationToken token)
    {
        var refreshed = false;
        while (true)
        {
            var accessToken = await transport.GetAccessTokenAsync(refreshed);
            var socket = newSocket(accessToken);
            using var opening = CancellationTokenSource.CreateLinkedTokenSource(token);
            opening.CancelAfter(bound);

            var proven = false;
            try
            {
                await socket.ConnectAsync(uri, transport.HttpClient, opening.Token);
                await ProveAsync(socket, opening.Token);
                proven = true;
                return new Opening(socket, null);
            }
            catch (OperationCanceledException) when (!token.IsCancellationRequested)
            {
                return new Opening(null, socket.HttpStatusCode == HttpStatusCode.SwitchingProtocols
                    ? $"the path accepted the upgrade but did not carry its probe within {bound.TotalSeconds:0.#}s"
                    : $"the upgrade was not answered within {bound.TotalSeconds:0.#}s");
            }
            catch (WebSocketException)
            {
                switch (socket.HttpStatusCode)
                {
                    case HttpStatusCode.Gone:
                    case HttpStatusCode.NotFound when CarriesRefusal(socket):
                        throw new StreamRefusedException(StreamOutcome.Gone, $"Transfer Gone ({socket.HttpStatusCode})");

                    case HttpStatusCode.Unauthorized when !refreshed:
                        logger.LogWarning("Stream Unauthorized (401). Refreshing token and retrying...");
                        refreshed = true;
                        continue;

                    case HttpStatusCode.Unauthorized:
                        throw new StreamRefusedException(StreamOutcome.Retryable, "Unauthorized (401) - Check credentials");

                    case HttpStatusCode.SwitchingProtocols:
                        return new Opening(null, "the path accepted the upgrade but did not carry its probe");

                    case 0:
                        return new Opening(null, "the connection failed before the upgrade was answered");

                    default:
                        return new Opening(null, $"the upgrade was answered {(int)socket.HttpStatusCode}");
                }
            }
            finally
            {
                if (!proven)
                {
                    socket.Dispose();
                }
            }
        }
    }

    /// <summary>A socket to open, over HTTP/1.1 like the rest of the data plane.</summary>
    /// <param name="type">The transfer's connection the stream belongs to; null for the probe, which has none.</param>
    public static ClientWebSocket NewSocket(ConnectionType? type, string? affinity, string? accessToken, TimeSpan keepAliveInterval)
    {
        var socket = new ClientWebSocket();
        var options = socket.Options;
        options.HttpVersion = HttpVersion.Version11;
        options.HttpVersionPolicy = HttpVersionPolicy.RequestVersionExact;
        options.CollectHttpResponseDetails = true;

        // No KeepAliveTimeout: it aborts a sound stream as soon as the hub stops reading, which is what a
        // hub parked on a slow peer does. The interval alone sends what a front end's idle timer needs.
        options.KeepAliveInterval = keepAliveInterval;
        if (type is { } connection)
        {
            options.SetRequestHeader(DataPlane.ConnectionTypeHeader, connection.ToString());
        }

        if (!string.IsNullOrEmpty(affinity))
        {
            options.SetRequestHeader("Cookie", affinity);
        }

        if (!string.IsNullOrEmpty(accessToken))
        {
            options.SetRequestHeader("Authorization", $"Bearer {accessToken}");
        }

        return socket;
    }

    /// <summary>The <c>ws</c> or <c>wss</c> address of <paramref name="url"/>, with <paramref name="query"/>.</summary>
    public static Uri UriOf(string url, string query)
    {
        var uri = new UriBuilder(url) { Query = query };
        uri.Scheme = uri.Scheme == Uri.UriSchemeHttps ? "wss" : "ws";
        return uri.Uri;
    }

    private static bool CarriesRefusal(ClientWebSocket socket) =>
        socket.HttpResponseHeaders?.Keys.Any(name => string.Equals(name, WireRoutes.RefusedHeader, StringComparison.OrdinalIgnoreCase)) == true;

    private static async Task ProveAsync(WebSocket socket, CancellationToken token)
    {
        await socket.SendAsync(ReadOnlyMemory<byte>.Empty, WebSocketMessageType.Text, endOfMessage: true, token);

        var answer = await socket.ReceiveAsync(new byte[1].AsMemory(), token);
        if (answer.MessageType != WebSocketMessageType.Text)
        {
            throw new WebSocketException(WebSocketError.InvalidMessageType, "The probe was not answered with a text message.");
        }
    }
}
