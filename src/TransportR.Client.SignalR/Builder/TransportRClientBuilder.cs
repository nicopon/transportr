using System.Net;
using Microsoft.AspNetCore.SignalR.Client;
using TransportR.Interfaces;
using TransportR.Protocol;
using TransportR.Client.SignalR.Services;
using Microsoft.Extensions.Logging; 
using Microsoft.Extensions.Logging.Abstractions; // Restore
using TransportR.Client.SignalR.Internal;
using TransportR.Client.SignalR.Configuration;

namespace TransportR.Client.SignalR;

/// <summary>
/// A fluent builder for configuring and creating a <see cref="SignalRDataClient{T}"/> carrying
/// items of type <typeparamref name="T"/>.
/// </summary>
/// <remarks>
/// The item type is declared here rather than at <see cref="Build"/> so that the serializer and
/// the client agree at compile time: a serializer that cannot carry <typeparamref name="T"/>
/// — <c>WithArrowSerialization</c> on anything but <c>RecordBatch</c> — is not an overload that
/// exists, so the mismatch is a compile error rather than a throw on the first batch.
/// </remarks>
/// <typeparam name="T">The type of items transfers built by this builder will carry.</typeparam>
public class TransportRClientBuilder<T>
{
    private string? _url;
    private int _batchSize = 1000;
    private int _batchFlushIntervalMs = 1000;
    private int? _maxInFlightBatches;
    private BufferCapacity? _receiveCapacity;
    private ILoggerFactory? _loggerFactory;
    private IBatchSerializer<T>? _serializer;
    private string? _accessToken;
    private Func<Task<string>>? _tokenProvider;
    private TimeSpan? _tokenRefreshThreshold;

    /// <summary>
    /// Sets the base URL of the TransportR Hub.
    /// </summary>
    public TransportRClientBuilder<T> WithUrl(string url)
    {
        _url = url;
        return this;
    }

    /// <summary>
    /// Sets the default batch size for transfers.
    /// </summary>
    public TransportRClientBuilder<T> WithBatchSize(int batchSize)
    {
        _batchSize = batchSize;
        return this;
    }

    /// <summary>
    /// Sets the batch accumulation timeout.
    /// </summary>
    public TransportRClientBuilder<T> WithBatchTimeout(int timeoutMs)
    {
        _batchFlushIntervalMs = timeoutMs;
        return this;
    }

    /// <summary>
    /// Bounds how many batches may be in flight — flushed but not yet acknowledged — before the
    /// producer is made to wait. Unbounded by default.
    /// </summary>
    /// <remarks>
    /// This is the only backpressure path from the receiver back to the producer. Left unset, a
    /// slow receiver makes the sender hold the whole source in memory: TCP throttles the HTTP
    /// write, but nothing upstream of it. The window counts batches, so it bounds roughly
    /// <c>max × batch size</c> — see "Backpressure" in <c>ResilienceArchitecture.md</c> for sizing.
    /// </remarks>
    public TransportRClientBuilder<T> WithMaxInFlightBatches(int max)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(max, 0, nameof(max));
        _maxInFlightBatches = max;
        return this;
    }

    /// <summary>
    /// Bounds what the receiver holds for a consumer that is behind. Unbounded by default — the
    /// counterpart of <see cref="WithMaxInFlightBatches"/> at the other end of the transfer.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Pass a count, a wire-byte budget, or both; the first one reached is the one that binds,
    /// so an Arrow consumer is held by the bytes and a record consumer by the count without
    /// either having to disable the other. At least one is required — calling this to bound
    /// nothing is a contradiction, and not calling it is how you say that.
    /// </para>
    /// <para>
    /// Raise the byte figure for throughput on a fast consumer, lower it to cap the receiver's
    /// footprint. See <see cref="BufferCapacity"/> for what a wire byte does and does not
    /// measure.
    /// </para>
    /// </remarks>
    /// <param name="maxItems">Maximum buffered items, or <c>null</c> not to bound the count.</param>
    /// <param name="maxBytes">Maximum buffered wire bytes, or <c>null</c> not to bound the size.</param>
    public TransportRClientBuilder<T> WithReceiveCapacity(int? maxItems = null, long? maxBytes = null)
        => WithReceiveCapacity(BufferCapacity.Of(maxItems, maxBytes));

    /// <inheritdoc cref="WithReceiveCapacity(int?, long?)"/>
    public TransportRClientBuilder<T> WithReceiveCapacity(BufferCapacity capacity)
    {
        if (capacity.IsEmpty)
            throw new ArgumentException(
                "A receive capacity needs at least one of the two dimensions bounded. Leave it "
              + "unset for an unbounded receiver.", nameof(capacity));

        _receiveCapacity = capacity;
        return this;
    }

    /// <summary>
    /// Sets the logger factory.
    /// </summary>
    public TransportRClientBuilder<T> WithLoggerFactory(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
        return this;
    }

    /// <summary>
    /// Sets the batch serializer.
    /// </summary>
    public TransportRClientBuilder<T> WithSerializer(IBatchSerializer<T> serializer)
    {
        _serializer = serializer;
        return this;
    }

    /// <summary>
    /// Sets the authentication token (static).
    /// </summary>
    public TransportRClientBuilder<T> WithAccessToken(string token)
    {
        _accessToken = token;
        return this;
    }

    /// <summary>
    /// Sets the authentication token provider (dynamic).
    /// </summary>
    public TransportRClientBuilder<T> WithTokenProvider(Func<Task<string>> provider)
    {
        _tokenProvider = provider;
        return this;
    }

    /// <summary>
    /// How long before a token expires the client asks its provider for a new one and presents it to
    /// the hub. Default: 5 minutes, and never more than half of what is left of the token, so a token
    /// shorter than two thresholds is renewed once rather than continuously.
    /// </summary>
    /// <remarks>
    /// The hub closes a connection whose token it has not been given a new one for, so this is how
    /// long the identity provider may be out of reach before connections start to close. It also
    /// governs when a transport asks the provider for a token as it connects. A provider that caches
    /// (<c>WithClientCredentials</c> does, until its own <c>RefreshThreshold</c>) returns the same token
    /// until then, and the renewal retries until it does not.
    /// </remarks>
    public TransportRClientBuilder<T> WithTokenRefreshThreshold(TimeSpan threshold)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(threshold, TimeSpan.Zero, nameof(threshold));
        _tokenRefreshThreshold = threshold;
        return this;
    }

    /// <summary>
    /// Builds the <see cref="SignalRDataClient{T}"/>.
    /// </summary>
    public SignalRDataClient<T> Build()
    {
        if (string.IsNullOrWhiteSpace(_url))
        {
            throw new InvalidOperationException("Hub URL must be configured using WithUrl().");
        }
        
        // Defaults
        var loggerFactory = _loggerFactory ?? NullLoggerFactory.Instance;
        var logger = loggerFactory.CreateLogger<SignalRDataClient<T>>();
        // The serializer is mandatory and there is no default: picking one for the caller would
        // pick their wire format, and the serialization packages are separate on purpose.
        if (_serializer == null)
        {
            throw new InvalidOperationException("Serializer must be configured using WithSerializer().");
        }

        var options = new ClientOptions
        {
            BatchFlushIntervalMs = _batchFlushIntervalMs,
            MaxInFlightBatches = _maxInFlightBatches
        };

        options.ReceiveBufferCapacity = _receiveCapacity;
        if (_tokenRefreshThreshold is { } threshold) options.TokenRefreshThreshold = threshold;

        // Neither end bounded is a supported configuration and the right one for a consumer that
        // keeps up, so this is a warning and not a refusal. It is loud because the failure it
        // precedes is not: nothing pushes back anywhere, and a consumer that falls behind is
        // paid for in the sender's memory and then the receiver's, until one of them dies.
        if (_maxInFlightBatches is null && _receiveCapacity is null)
        {
            logger.LogWarning(
                "No backpressure configured: neither WithMaxInFlightBatches nor WithReceiveCapacity "
              + "is set, so a consumer slower than the producer is absorbed in memory rather than "
              + "throttled.");
        }
        
        var hubUrl = _url.TrimEnd('/');
        var controlUrl = hubUrl + WireRoutes.CommandHub;
        var dataUrl = hubUrl + WireRoutes.StreamBase;
        var clientUrl = hubUrl + WireRoutes.ClientBase;

        // Create Internal Components
        var connectionManager = new HubConnectionManager(
            controlUrl, 
            options, 
            loggerFactory.CreateLogger<HubConnectionManager>(),
            _accessToken,
            _tokenProvider
        );
        
        var sessionRegistry = new SessionRegistry<T>(logger);

        // Create Infrastructure dependencies
        var streamManager = new Microsoft.IO.RecyclableMemoryStreamManager();
        
        // Configure HttpClient using the Manager's CookieContainer
        var httpHandler = new SocketsHttpHandler
        {
            AutomaticDecompression = DecompressionMethods.None,
            UseCookies = true,
            CookieContainer = connectionManager.CookieContainer
        };
        
        var httpClient = new HttpClient(httpHandler)
        {
             Timeout = Timeout.InfiniteTimeSpan
        };

        return new SignalRDataClient<T>(
            connectionManager,
            sessionRegistry,
            _serializer,
            logger,
            _batchSize,
            options,
            dataUrl,
            clientUrl,
            httpClient,
            streamManager
        );
    }
}
