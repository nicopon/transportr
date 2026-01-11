using System.Net;
using TransportR.Interfaces;
using TransportR.Client.SignalR.Services;
using Microsoft.Extensions.Logging; 
using Microsoft.Extensions.Logging.Abstractions; // Restore
using TransportR.Client.SignalR.Internal;
using TransportR.Client.SignalR.Configuration;

namespace TransportR.Client.SignalR;

/// <summary>
/// A fluent builder for configuring and creating a <see cref="SignalRDataClient{T}"/>.
/// </summary>
public class TransportRClientBuilder
{
    private string? _url;
    private int _batchSize = 1000;
    private int _batchFlushIntervalMs = 1000;
    private ILoggerFactory? _loggerFactory;
    private IBatchSerializer? _serializer;
    private string? _accessToken;
    private Func<Task<string>>? _tokenProvider;

    /// <summary>
    /// Sets the base URL of the TransportR Hub.
    /// </summary>
    public TransportRClientBuilder WithUrl(string url)
    {
        _url = url;
        return this;
    }

    /// <summary>
    /// Sets the default batch size for transfers.
    /// </summary>
    public TransportRClientBuilder WithBatchSize(int batchSize)
    {
        _batchSize = batchSize;
        return this;
    }

    /// <summary>
    /// Sets the batch accumulation timeout.
    /// </summary>
    public TransportRClientBuilder WithBatchTimeout(int timeoutMs)
    {
        _batchFlushIntervalMs = timeoutMs;
        return this;
    }

    /// <summary>
    /// Sets the logger factory.
    /// </summary>
    public TransportRClientBuilder WithLoggerFactory(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
        return this;
    }

    /// <summary>
    /// Sets the batch serializer.
    /// </summary>
    public TransportRClientBuilder WithSerializer(IBatchSerializer serializer)
    {
        _serializer = serializer;
        return this;
    }

    /// <summary>
    /// Sets the authentication token (static).
    /// </summary>
    public TransportRClientBuilder WithAccessToken(string token)
    {
        _accessToken = token;
        return this;
    }

    /// <summary>
    /// Sets the authentication token provider (dynamic).
    /// </summary>
    public TransportRClientBuilder WithTokenProvider(Func<Task<string>> provider)
    {
        _tokenProvider = provider;
        return this;
    }

    /// <summary>
    /// Configures the client to use MessagePack serialization.
    /// </summary>
    // DEPRECATED/REMOVED in favor of Extensions
    
    /// <summary>
    /// Configures the client to use Client Credentials flow for authentication.
    /// </summary>
    // DEPRECATED/REMOVED in favor of Extensions

    /// <summary>
    /// Builds the <see cref="SignalRDataClient{T}"/>.
    /// </summary>
    public SignalRDataClient<T> Build<T>()
    {
        if (string.IsNullOrWhiteSpace(_url))
        {
            throw new InvalidOperationException("Hub URL must be configured using WithUrl().");
        }
        
        // Defaults
        var loggerFactory = _loggerFactory ?? NullLoggerFactory.Instance;
        var logger = loggerFactory.CreateLogger<SignalRDataClient<T>>();
        // If no serializer provided, we can't build? Or rely on default?
        // Protocol requires BinaryPrimitives.
        // Assuming Serializer is mandatory or user must provide one.
        // There is no public default implementation visible here
        // Let's assume user must provide it (as per previous constructor).
        if (_serializer == null)
        {
            throw new InvalidOperationException("Serializer must be configured using WithSerializer().");
        }

        var options = new ClientOptions { BatchFlushIntervalMs = _batchFlushIntervalMs };
        
        var hubUrl = _url.TrimEnd('/');
        var controlUrl = $"{hubUrl}/hub/command";
        var dataUrl = $"{hubUrl}/api/stream";

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
            httpClient,
            streamManager
        );
    }
}
