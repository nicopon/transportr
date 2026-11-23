using System.Net;
using System.Security.Cryptography;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Client.Configuration;
using TransportR.Client.Internal;

namespace TransportR.Client.Builder;

/// <summary>
/// What a client needs to reach the hub, assembled by the builder of either kind of client.
/// </summary>
internal sealed record ClientWiring(
    ControlChannel Channel,
    ClientOptions Options,
    string DataUrl,
    HttpClient HttpClient);

/// <summary>
/// The settings the builders of both kinds of client share, with their checks, and the assembly of what
/// they configure: the one place a setting is validated and the one place a client's plumbing is made.
/// </summary>
internal sealed class ClientBuilderState
{
    private static readonly TimeSpan LongestBatchTimeout = TimeSpan.FromMilliseconds(int.MaxValue);

    // The longest wait Task.WaitAsync and Task.Delay take, which is how a session waits on this delay.
    private static readonly TimeSpan LongestAcknowledgementTimeout = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    private string? _url;
    private Guid? _clientId;
    private int _batchFlushIntervalMs = 1000;
    private int? _maxInFlightBatches;
    private ILoggerFactory? _loggerFactory;
    private string? _accessToken;
    private Func<Task<string>>? _tokenProvider;
    private Func<HttpMessageHandler, ILoggerFactory, Func<Task<string>>>? _tokenProviderFactory;
    private Action<SocketsHttpHandler>? _configureHttpHandler;
    private HttpMessageHandler? _customHttpHandler;
    private bool _disposeHttpHandler;
    private TimeSpan? _tokenRefreshThreshold;
    private TimeSpan? _acknowledgementTimeout;
    private WriteTransportPolicy _writeTransportPolicy = WriteTransportPolicy.PreferWebSocket;
    private bool _probeWriteTransport;
    private EndToEndEncryption? _endToEndEncryption;

    public BufferCapacity? ReceiveCapacity { get; private set; }

    public int MaxBatchBytes { get; private set; } = ClientOptions.DefaultMaxBatchBytes;

    public void SetUrl(string url) => _url = url;

    public void SetClientId(Guid clientId)
    {
        if (clientId == Guid.Empty)
        {
            throw new ArgumentException("Client ID cannot be empty.", nameof(clientId));
        }

        _clientId = clientId;
    }

    public void SetBatchTimeout(TimeSpan timeout)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero, nameof(timeout));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(timeout, LongestBatchTimeout, nameof(timeout));
        if (timeout.Ticks % TimeSpan.TicksPerMillisecond != 0)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), timeout, "The batch timeout must be a whole number of milliseconds.");
        }

        _batchFlushIntervalMs = (int)timeout.TotalMilliseconds;
    }

    public void SetAcknowledgementTimeout(TimeSpan timeout)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero, nameof(timeout));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(timeout, LongestAcknowledgementTimeout, nameof(timeout));
        _acknowledgementTimeout = timeout;
    }

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

    public void SetTokenProvider(Func<Task<string>> provider)
    {
        _tokenProvider = provider;
        _tokenProviderFactory = null;
    }

    public void SetTokenProviderFactory(Func<HttpMessageHandler, ILoggerFactory, Func<Task<string>>> factory)
    {
        _tokenProviderFactory = factory;
        _tokenProvider = null;
    }

    public void SetHttpHandlerConfiguration(Action<SocketsHttpHandler> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        _configureHttpHandler = configure;
    }

    public void SetHttpMessageHandler(HttpMessageHandler handler, bool disposeHandler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        _customHttpHandler = handler;
        _disposeHttpHandler = disposeHandler;
    }

    public void SetEndToEndEncryption() => _endToEndEncryption = new EndToEndEncryption();

    public void SetEndToEndEncryption(Func<Guid, byte[]?> preSharedKey)
    {
        ArgumentNullException.ThrowIfNull(preSharedKey);
        _endToEndEncryption = new EndToEndEncryption(preSharedKey);
    }

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

    public void EnableWriteTransportProbe() => _probeWriteTransport = true;

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

        if (_endToEndEncryption is not null && !AesGcm.IsSupported)
        {
            throw new PlatformNotSupportedException("End-to-end encryption needs AES-GCM, which this platform does not provide.");
        }

        if (_customHttpHandler is not null && _configureHttpHandler is not null)
        {
            throw new InvalidOperationException("Cannot configure both ConfigureHttpHandler and WithHttpMessageHandler.");
        }

        HttpMessageHandler httpHandler;
        bool disposeHandler;
        if (_customHttpHandler is not null)
        {
            ValidateHandlerChain(_customHttpHandler);
            httpHandler = _customHttpHandler;
            disposeHandler = _disposeHttpHandler;
        }
        else
        {
            httpHandler = CreateHttpHandler();
            disposeHandler = true;
        }

        var loggerFactory = _loggerFactory ?? NullLoggerFactory.Instance;

        var options = new ClientOptions
        {
            BatchFlushIntervalMs = _batchFlushIntervalMs,
            MaxInFlightBatches = _maxInFlightBatches,
            WriteTransportPolicy = _writeTransportPolicy,
            ProbeWriteTransport = _probeWriteTransport,
            EndToEndEncryption = _endToEndEncryption
        };

        options.ReceiveBufferCapacity = ReceiveCapacity;
        options.MaxBatchBytes = MaxBatchBytes;
        if (_tokenRefreshThreshold is { } threshold) options.TokenRefreshThreshold = threshold;
        if (_acknowledgementTimeout is { } acknowledgementTimeout) options.CompleteStallTimeout = acknowledgementTimeout;

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

        // The hub's HttpClient owns the handler when built internally or when disposeHandler is true.
        // A token provider made here reaches its identity provider through the same handler without owning it.
        var httpClient = new HttpClient(httpHandler, disposeHandler)
        {
            Timeout = Timeout.InfiniteTimeSpan
        };

        var channel = new ControlChannel(
            hubUrl,
            _clientId ?? Guid.NewGuid(),
            httpClient,
            options,
            loggerFactory.CreateLogger<ControlChannel>(),
            _accessToken,
            _tokenProviderFactory?.Invoke(httpHandler, loggerFactory) ?? _tokenProvider);

        return new ClientWiring(channel, options, dataUrl, httpClient);
    }

    private static void ValidateHandlerChain(HttpMessageHandler handler)
    {
        for (var current = handler; current is not null; current = (current as DelegatingHandler)?.InnerHandler)
        {
            switch (current)
            {
                case SocketsHttpHandler sockets:
                    CheckHandlerSettings("WithHttpMessageHandler", sockets.UseCookies, sockets.AutomaticDecompression);
                    break;
                case HttpClientHandler httpHandler:
                    CheckHandlerSettings("WithHttpMessageHandler", httpHandler.UseCookies, httpHandler.AutomaticDecompression);
                    break;
            }
        }
    }

    private SocketsHttpHandler CreateHttpHandler()
    {
        // No cookie container: the control channel sets each request's cookies itself, since a
        // transfer's requests carry another affinity than the client's own.
        var handler = new SocketsHttpHandler
        {
            AutomaticDecompression = DecompressionMethods.None,
            UseCookies = false
        };

        try
        {
            _configureHttpHandler?.Invoke(handler);
            CheckHandlerSettings("ConfigureHttpHandler", handler.UseCookies, handler.AutomaticDecompression);
        }
        catch
        {
            handler.Dispose();
            throw;
        }

        return handler;
    }

    private static void CheckHandlerSettings(string origin, bool useCookies, DecompressionMethods automaticDecompression)
    {
        if (useCookies)
        {
            throw Refused(origin, nameof(SocketsHttpHandler.UseCookies),
                "the client sets the cookies of each request itself, and a cookie container would override them");
        }

        if (automaticDecompression != DecompressionMethods.None)
        {
            throw Refused(origin, nameof(SocketsHttpHandler.AutomaticDecompression),
                "it invites compression on the way to the client, which may hold a stream back instead of forwarding it as it is written");
        }
    }

    private static InvalidOperationException Refused(string origin, string setting, string why) =>
        new($"{origin} may not set {setting}: {why}.");
}
