using System.Net;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Http.Connections.Client;

using TransportR.Client.SignalR.Configuration;

namespace TransportR.Client.SignalR.Internal;

/// <summary>
/// Manages the lifecycle of the SignalR HubConnection.
/// Handles connection building, authentication, automatic reconnections, and HA affinity redirects.
/// </summary>
internal class HubConnectionManager : IAsyncDisposable
{
    private readonly string _hubUrl;
    private readonly ClientOptions _options;
    private readonly ILogger _logger;
    private readonly Func<Task<string>>? _tokenProvider;
    
    private HubConnection? _hubConnection;
    private readonly CookieContainer _cookieContainer = new();
    private Action<HubConnection>? _registrationCallback;
    
    // Auth state
    private string? _currentAccessToken;
    private DateTime? _tokenExpiry;

    public HubConnection Connection => _hubConnection ?? throw new InvalidOperationException("Connection not initialized");
    public CookieContainer CookieContainer => _cookieContainer;

    private readonly Action<HttpConnectionOptions>? _configureHttpConnection;

    public HubConnectionManager(
        string hubUrl, 
        ClientOptions options, 
        ILogger logger, 
        string? initialAccessToken = null,
        Func<Task<string>>? tokenProvider = null,
        Action<HttpConnectionOptions>? configureHttpConnection = null)
    {
        _hubUrl = hubUrl;
        _options = options;
        _logger = logger;
        _currentAccessToken = initialAccessToken;
        _tokenProvider = tokenProvider;
        _configureHttpConnection = configureHttpConnection;
        
        if (!string.IsNullOrEmpty(initialAccessToken))
        {
            _tokenExpiry = JwtHelper.ExtractExpiry(initialAccessToken);
        }
    }

    public void Initialize(Action<HubConnection> registrationCallback)
    {
        _registrationCallback = registrationCallback;
        BuildAndConfigureConnection();
    }

    private void BuildAndConfigureConnection()
    {
        var builder = new HubConnectionBuilder()
            .WithUrl(_hubUrl, options =>
            {
                options.AccessTokenProvider = () => GetAccessTokenAsync(false);
                options.Cookies = _cookieContainer;
                _configureHttpConnection?.Invoke(options);
            })
            .WithAutomaticReconnect();
            
        _hubConnection = builder.Build();
        _registrationCallback?.Invoke(_hubConnection);
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
         if (_hubConnection == null) throw new InvalidOperationException("Not initialized");
         
         const int maxRetries = 5;
         var delay = TimeSpan.FromMilliseconds(500);
         
         for (int attempt = 1; attempt <= maxRetries; attempt++)
         {
             try
             {
                 await _hubConnection.StartAsync(cancellationToken);
                 return;
             }
             catch (Exception ex) when (attempt < maxRetries && !cancellationToken.IsCancellationRequested)
             {
                 _logger.LogWarning("Connection attempt {Attempt}/{MaxRetries} failed: {Message}. Retrying in {Delay}ms...", 
                     attempt, maxRetries, ex.Message, delay.TotalMilliseconds);
                 await Task.Delay(delay, cancellationToken);
                 delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, 5000));
             }
         }
         // Last attempt
         await _hubConnection.StartAsync(cancellationToken);
    }

    public async Task HandleAffinityRedirectAsync(string cookieName, string affinityToken, Func<Task> reconnectAction)
    {
        _logger.LogInformation("Affinity redirect received. Reconnecting with cookie {CookieName}={Token}", cookieName, affinityToken);
        
        // Add cookie
        var uri = new Uri(_hubUrl);
        var cookie = new Cookie(cookieName, affinityToken) { Domain = uri.Host, Path = "/" };
        _cookieContainer.Add(cookie);
        
        // Stop current
        if (_hubConnection != null)
        {
            await _hubConnection.StopAsync();
            await _hubConnection.DisposeAsync();
        }
        
        // Rebuild (this triggers registrationCallback)
        BuildAndConfigureConnection();
        
        // Reconnect logic (caller must handle Connect invoke?)
        // The caller (SignalRDataClient) needs to call StartAsync + ConnectAsync.
        // We can just call start here?
        // But ConnectAsync usually takes args (clientId, groups).
        // Pass a reconnectAction?
        
        await StartAsync(CancellationToken.None);
        await reconnectAction();
    }

    public async Task<string?> GetAccessTokenAsync(bool forceRefresh = false)
    {
        if (_tokenProvider == null) return _currentAccessToken;
        
        if (forceRefresh || _currentAccessToken == null || 
            (_tokenExpiry.HasValue && DateTime.UtcNow > _tokenExpiry.Value - _options.TokenRefreshThreshold))
        {
            try
            {
                _logger.LogInformation("Acquiring token (Force={Force})...", forceRefresh);
                _currentAccessToken = await _tokenProvider();
                _tokenExpiry = JwtHelper.ExtractExpiry(_currentAccessToken);
                _logger.LogInformation("Token acquired, new expiry: {Expiry}", _tokenExpiry?.ToString("u") ?? "unknown");
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to acquire token, using existing token");
            }
        }
        return _currentAccessToken;
    }

    public async ValueTask DisposeAsync()
    {
        if (_hubConnection != null)
        {
            await _hubConnection.DisposeAsync();
        }
    }
}
