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
        AddRegistrationCallback(registrationCallback);
        BuildAndConfigureConnection();
    }

    /// <summary>
    /// Adds another callback invoked whenever the connection is (re)built, composing with whatever
    /// is already registered rather than replacing it. A host hooking its own handlers onto this
    /// connection (<c>TransportRClientBuilder.WithControlConnection</c>) and TransportR's own
    /// registration (<see cref="TransportR.Client.SignalR.Services.SignalRTransferSignaling{T}"/>)
    /// both have to survive an affinity redirect's rebuild - the single field this used to be let
    /// whichever called last silently drop the other.
    /// </summary>
    public void AddRegistrationCallback(Action<HubConnection> callback)
    {
        _registrationCallback += callback;

        // The connection may already exist (the application hooks in after TransportR's own
        // Initialize call): apply the new callback to it now rather than waiting for a rebuild
        // that might never come.
        if (_hubConnection != null)
        {
            callback(_hubConnection);
        }
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
        
        // Starting the connection is ours; re-registering on it is not. ConnectAsync needs the
        // client id and the groups, which live on SignalRDataClient, so the caller hands us the
        // re-registration as a callback rather than this type learning about groups.

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
