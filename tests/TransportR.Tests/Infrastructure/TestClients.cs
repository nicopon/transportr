using Microsoft.AspNetCore.Http.Connections.Client;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Client.SignalR.Configuration;
using TransportR.Client.SignalR.Internal;
using TransportR.Client.SignalR.Services;
using TransportR.Protocol;
using TransportR.Serialization.MessagePack.Services;

namespace TransportR.Tests.Infrastructure;

/// <summary>
/// Builds a client the way <c>TransportRClientBuilder</c> does, with the two things the builder does
/// not let a test choose: the handler that carries it (a <c>TestServer</c> one, or the network) and
/// the options of its HTTP connection (the SignalR transport).
/// </summary>
internal static class TestClients
{
    public static SignalRDataClient<string> Create(
        HttpMessageHandler? handler,
        string baseUrl,
        Func<Task<string>>? tokenProvider = null,
        Action<ClientOptions>? configure = null,
        Action<HttpConnectionOptions>? configureConnection = null,
        ILogger? logger = null,
        string? accessToken = null)
    {
        var options = new ClientOptions();
        configure?.Invoke(options);

        logger ??= NullLogger.Instance;

        var connectionManager = new HubConnectionManager(
            baseUrl + WireRoutes.CommandHub,
            options,
            logger,
            initialAccessToken: accessToken,
            tokenProvider: tokenProvider,
            configureHttpConnection: httpOptions =>
            {
                if (handler != null)
                {
                    httpOptions.HttpMessageHandlerFactory = _ => handler;
                }

                configureConnection?.Invoke(httpOptions);
            });

        var httpClient = handler != null
            ? new HttpClient(handler) { BaseAddress = new Uri(baseUrl) }
            : new HttpClient(new SocketsHttpHandler()) { BaseAddress = new Uri(baseUrl), Timeout = Timeout.InfiniteTimeSpan };

        return new SignalRDataClient<string>(
            connectionManager,
            new SessionRegistry<string>(logger),
            new MessagePackBatchSerializer<string>(),
            logger,
            defaultBatchSize: 100,
            options,
            baseUrl + WireRoutes.StreamBase,
            baseUrl + WireRoutes.ClientBase,
            httpClient,
            new Microsoft.IO.RecyclableMemoryStreamManager());
    }
}

/// <summary>
/// A client's token provider: a fresh token on every call, expiring <c>lifetime</c> after it was
/// asked for, which is what a client-credentials grant does.
/// </summary>
public sealed class TokenSource
{
    private readonly string _subject;
    private readonly TimeSpan _lifetime;
    private int _calls;

    /// <summary>Tokens the client cannot read its own expiry from, as an encrypted one would be.</summary>
    public bool UnreadableByClients { get; init; }

    public TokenSource(string subject, TimeSpan lifetime, params string[] groups)
    {
        _subject = subject;
        _lifetime = lifetime;
        Groups = groups.Length == 0 ? [AuthenticatedHubServerFactory.DefaultGroup] : groups;
    }

    /// <summary>The groups the next token carries: what an identity provider answers, and may change.</summary>
    public string[] Groups { get; set; }

    public int Calls => Volatile.Read(ref _calls);

    public Task<string> GetAsync()
    {
        Interlocked.Increment(ref _calls);
        var expiry = DateTime.UtcNow + _lifetime;
        return Task.FromResult(UnreadableByClients
            ? StubJwt.CreateUnreadableByClients(_subject, expiry, Groups)
            : StubJwt.Create(_subject, expiry, Groups));
    }
}
