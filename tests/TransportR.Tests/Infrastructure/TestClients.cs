using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Client.Configuration;
using TransportR.Client.Internal;
using TransportR.Client.Services;
using TransportR.Models;
using TransportR.Protocol;
using TransportR.Serialization.MessagePack.Services;

namespace TransportR.Tests.Infrastructure;

/// <summary>
/// Builds a client the way <c>TransportRClientBuilder</c> does, with the two things the builder does
/// not let a test choose: the handler that carries it (a <c>TestServer</c> one, or the network) and
/// its options.
/// </summary>
internal static class TestClients
{
    public static TransportRClient<string> Create(
        HttpMessageHandler? handler,
        string baseUrl,
        Func<Task<string>>? tokenProvider = null,
        Action<ClientOptions>? configure = null,
        ILogger? logger = null,
        string? accessToken = null)
    {
        var options = new ClientOptions();

        // A TestServer handler cannot carry a ClientWebSocket: over it, a client would fall back on
        // every transfer. A client on the network keeps the default.
        if (handler != null)
        {
            options.WriteTransportPolicy = WriteTransportPolicy.SegmentsOnly;
        }

        configure?.Invoke(options);

        logger ??= NullLogger.Instance;

        var httpClient = handler != null
            ? new HttpClient(handler) { BaseAddress = new Uri(baseUrl), Timeout = Timeout.InfiniteTimeSpan }
            : new HttpClient(new SocketsHttpHandler { UseCookies = false }) { BaseAddress = new Uri(baseUrl), Timeout = Timeout.InfiniteTimeSpan };

        var channel = new ControlChannel(baseUrl, Guid.NewGuid(), httpClient, options, logger,
            initialAccessToken: accessToken, tokenProvider: tokenProvider);

        return new TransportRClient<string>(
            channel,
            new SessionRegistry<string>(logger),
            new MessagePackBatchSerializer<string>(),
            logger,
            defaultBatchSize: 100,
            options,
            baseUrl + WireRoutes.StreamBase,
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
