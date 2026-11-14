using System.Net;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.IO;
using TransportR.Client.Configuration;
using TransportR.Client.Internal;
using TransportR.Protocol;

namespace TransportR.Client;

/// <summary>
/// What a client needs to reach the hub, assembled by the builder of either kind of client.
/// </summary>
internal sealed record ClientWiring(
    ControlChannel Channel,
    ClientOptions Options,
    string DataUrl,
    HttpClient HttpClient,
    RecyclableMemoryStreamManager Streams);

/// <summary>
/// The settings the builders of both kinds of client share, with their checks, and the assembly of what
/// they configure: the one place a setting is validated and the one place a client's plumbing is made.
/// </summary>
internal sealed class ClientBuilderState
{
    private string? _url;
    private int _batchFlushIntervalMs = 1000;
    private int? _maxInFlightBatches;
    private ILoggerFactory? _loggerFactory;
    private string? _accessToken;
    private Func<Task<string>>? _tokenProvider;
    private TimeSpan? _tokenRefreshThreshold;
    private WriteTransportPolicy _writeTransportPolicy = WriteTransportPolicy.PreferWebSocket;

    public BufferCapacity? ReceiveCapacity { get; private set; }

    public int MaxBatchBytes { get; private set; } = ClientOptions.DefaultMaxBatchBytes;

    public void SetUrl(string url) => _url = url;

    public void SetBatchTimeout(int timeoutMs) => _batchFlushIntervalMs = timeoutMs;

    public void SetMaxInFlightBatches(int max)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(max, 0, nameof(max));
        _maxInFlightBatches = max;
    }

    public void SetMaxBatchBytes(int maxBytes)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(maxBytes, 0, nameof(maxBytes));
        MaxBatchBytes = maxBytes;
    }

    public void SetReceiveCapacity(BufferCapacity capacity)
    {
        if (capacity.IsEmpty)
            throw new ArgumentException(
                "A receive capacity needs at least one of the two dimensions bounded. Leave it "
              + "unset for an unbounded receiver.", nameof(capacity));

        ReceiveCapacity = capacity;
    }

    public void SetLoggerFactory(ILoggerFactory loggerFactory) => _loggerFactory = loggerFactory;

    public void SetAccessToken(string token) => _accessToken = token;

    public void SetTokenProvider(Func<Task<string>> provider) => _tokenProvider = provider;

    public void SetTokenRefreshThreshold(TimeSpan threshold)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(threshold, TimeSpan.Zero, nameof(threshold));
        _tokenRefreshThreshold = threshold;
    }

    public void SetWriteTransport(WriteTransportPolicy policy)
    {
        if (!Enum.IsDefined(policy))
        {
            throw new ArgumentOutOfRangeException(nameof(policy), policy, "Not a write transport policy.");
        }

        _writeTransportPolicy = policy;
    }

    public void RequireUrl()
    {
        if (string.IsNullOrWhiteSpace(_url))
        {
            throw new InvalidOperationException("Hub URL must be configured using WithUrl().");
        }
    }

    public ILogger CreateLogger<TCategory>() => (_loggerFactory ?? NullLoggerFactory.Instance).CreateLogger<TCategory>();

    /// <param name="logger">The client's own, made by <see cref="CreateLogger{TCategory}"/>.</param>
    public ClientWiring Assemble(ILogger logger)
    {
        RequireUrl();
        var loggerFactory = _loggerFactory ?? NullLoggerFactory.Instance;

        var options = new ClientOptions
        {
            BatchFlushIntervalMs = _batchFlushIntervalMs,
            MaxInFlightBatches = _maxInFlightBatches,
            WriteTransportPolicy = _writeTransportPolicy
        };

        options.ReceiveBufferCapacity = ReceiveCapacity;
        options.MaxBatchBytes = MaxBatchBytes;
        if (_tokenRefreshThreshold is { } threshold) options.TokenRefreshThreshold = threshold;

        // Neither end bounded is a supported configuration and the right one for a consumer that
        // keeps up, so this is a warning and not a refusal. It is loud because the failure it
        // precedes is not: nothing pushes back anywhere, and a consumer that falls behind is
        // paid for in the sender's memory and then the receiver's, until one of them dies.
        if (_maxInFlightBatches is null && ReceiveCapacity is null)
        {
            logger.LogWarning(
                "No backpressure configured: neither WithMaxInFlightBatches nor WithReceiveCapacity "
              + "is set, so a consumer slower than the producer is absorbed in memory rather than "
              + "throttled.");
        }

        var hubUrl = _url!.TrimEnd('/');
        var dataUrl = hubUrl + WireRoutes.StreamBase;

        var streamManager = new RecyclableMemoryStreamManager();

        // No cookie container: the control channel sets each request's cookies itself, since a
        // transfer's requests carry another affinity than the client's own.
        var httpHandler = new SocketsHttpHandler
        {
            AutomaticDecompression = DecompressionMethods.None,
            UseCookies = false
        };

        var httpClient = new HttpClient(httpHandler)
        {
            Timeout = Timeout.InfiniteTimeSpan
        };

        var channel = new ControlChannel(
            hubUrl,
            Guid.NewGuid(),
            httpClient,
            options,
            loggerFactory.CreateLogger<ControlChannel>(),
            _accessToken,
            _tokenProvider);

        return new ClientWiring(channel, options, dataUrl, httpClient, streamManager);
    }
}
