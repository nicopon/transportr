using System.Net;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Http.Connections.Client;

using TransportR.Client.Configuration;

namespace TransportR.Client.Internal;

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

    // A connection the hub closes cleanly, or that SignalR gave up reconnecting, is restarted for as
    // long as the application has not disposed the client. Stopping it ourselves (an affinity
    // redirect, a refused registration, disposal) is not a closure to recover from.
    private readonly CancellationTokenSource _lifetime = new();
    private readonly object _restartLock = new();
    private Task? _restart;
    private bool _restarting;
    private bool _closedWhileRestarting;
    private bool _replacing;
    private bool _disposed;

    private static readonly TimeSpan FirstRestartDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxRestartDelay = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Run once a closed connection has been started again, to register the client on it. The
    /// connection is new to the hub, which knows nothing of what this client was before.
    /// </summary>
    public Func<Task>? ConnectionRestarted { get; set; }

    public bool HasTokenProvider => _tokenProvider != null;

    /// <summary>The expiry the current token declares, when this client can read it.</summary>
    public DateTime? TokenExpiry => _tokenExpiry;

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

    /// <param name="registrationCallback">
    /// Run on every connection this manager builds, the first and each one an affinity redirect
    /// rebuilds: the handlers it registers would not survive the rebuild otherwise.
    /// </param>
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
            
        var connection = builder.Build();
        _hubConnection = connection;
        connection.Closed += error => OnClosed(connection, error);
        _registrationCallback?.Invoke(connection);
    }

    private Task OnClosed(HubConnection connection, Exception? error)
    {
        if (_replacing || !ReferenceEquals(connection, _hubConnection))
        {
            return Task.CompletedTask;
        }

        lock (_restartLock)
        {
            // Read under the lock DisposeAsync raises the flag under: a closure that passed an
            // earlier check must not take _lifetime.Token from a source disposal has since released.
            if (_disposed) return Task.CompletedTask;

            // Closed again while still being brought back (a hub that closes what it just accepted):
            // the running restart goes round once more rather than leaving the client out.
            if (_restarting)
            {
                _closedWhileRestarting = true;
                return Task.CompletedTask;
            }

            _restarting = true;

            // Closed is awaited by SignalR, so the work is handed off. A clean close is the hub ending a
            // connection on purpose (a token that ran out): automatic reconnection does not apply to it,
            // and nothing would bring the client back.
            _logger.LogWarning(error, "The connection to the hub was closed. Starting it again.");
            _restart = RestartAsync(connection, _lifetime.Token);
        }

        return Task.CompletedTask;
    }

    private async Task RestartAsync(HubConnection connection, CancellationToken cancellationToken)
    {
        var delay = FirstRestartDelay;

        // A connection that an affinity redirect replaced is no one's to restart.
        while (!cancellationToken.IsCancellationRequested && ReferenceEquals(connection, _hubConnection))
        {
            try
            {
                // Half to full delay: a fleet cut off together, by an identity provider that went down,
                // must not come back in step.
                await Task.Delay(delay * (0.5 + Random.Shared.NextDouble() * 0.5), cancellationToken);

                // Starting asks the token provider for a token, so the connection comes back with the
                // one the old connection lacked.
                await connection.StartAsync(cancellationToken);
                _logger.LogInformation("The connection to the hub is back.");

                if (ConnectionRestarted is { } restarted) await restarted();

                lock (_restartLock)
                {
                    if (!_closedWhileRestarting && connection.State != HubConnectionState.Disconnected)
                    {
                        _restarting = false;
                        return;
                    }

                    _closedWhileRestarting = false;
                }

                delay = FirstRestartDelay;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Could not start the connection again: {Message}. Next attempt in at most {Delay}s.",
                    ex.Message, delay.TotalSeconds);
                delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, MaxRestartDelay.Ticks));
            }
        }

        lock (_restartLock) _restarting = false;
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
    }

    /// <summary>
    /// Stops the connection and puts in its place a new one, not started: what a client that the hub
    /// refused to register holds, so that its next <see cref="StartAsync"/> starts from scratch.
    /// </summary>
    public async Task ResetAsync()
    {
        // SignalR raises Closed on the thread pool, possibly once the flag is down again; by then the
        // stopped connection is no longer the current one, and OnClosed leaves it alone.
        _replacing = true;
        try
        {
            await Connection.StopAsync();
            await Connection.DisposeAsync();
            BuildAndConfigureConnection();
        }
        finally
        {
            _replacing = false;
        }
    }

    public async Task HandleAffinityRedirectAsync(string cookieName, string affinityToken, Func<Task> reconnectAction)
    {
        _logger.LogInformation("Affinity redirect received. Reconnecting with cookie {CookieName}={Token}", cookieName, affinityToken);
        
        // Add cookie
        var uri = new Uri(_hubUrl);
        var cookie = new Cookie(cookieName, affinityToken) { Domain = uri.Host, Path = "/" };
        _cookieContainer.Add(cookie);
        
        // The closure this causes is ours, not the hub's.
        _replacing = true;
        try
        {
            // Stop current
            if (_hubConnection != null)
            {
                await _hubConnection.StopAsync();
                await _hubConnection.DisposeAsync();
            }

            // Rebuild (this triggers registrationCallback)
            BuildAndConfigureConnection();

            // Starting the connection is ours; re-registering on it is not. ConnectAsync needs the
            // client id and the groups, which live on TransportRClient, so the caller hands us the
            // re-registration as a callback rather than this type learning about groups.

            await StartAsync(CancellationToken.None);
        }
        finally
        {
            _replacing = false;
        }

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
                await AcquireTokenAsync();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to acquire token, using existing token");
            }
        }
        return _currentAccessToken;
    }

    /// <summary>
    /// Asks the provider for a token now, whatever the one held is worth, and says so when it
    /// cannot. A transport that needs a token takes the old one on failure
    /// (<see cref="GetAccessTokenAsync"/>); renewal must know that it did not get a new one.
    /// </summary>
    public async Task<string> AcquireTokenAsync()
    {
        if (_tokenProvider == null) throw new InvalidOperationException("No token provider is configured.");

        var token = await _tokenProvider();
        _currentAccessToken = token;
        _tokenExpiry = JwtHelper.ExtractExpiry(token);
        _logger.LogInformation("Token acquired, new expiry: {Expiry}", _tokenExpiry?.ToString("u") ?? "unknown");
        return token;
    }

    public async ValueTask DisposeAsync()
    {
        // A host that disconnects its client and then disposes it reaches this twice, and the second
        // Cancel() on the disposed source would throw. A second caller does not wait for the first.
        // The flag is up before the connection is disposed: its Closed event must not read as a
        // closure to restart from.
        Task? restart;
        lock (_restartLock)
        {
            if (_disposed) return;
            _disposed = true;
            restart = _restart;
        }

        _lifetime.Cancel();

        if (restart != null)
        {
            try { await restart; } catch (Exception) { /* a restart that failed has said so already */ }
        }

        if (_hubConnection != null)
        {
            await _hubConnection.DisposeAsync();
        }

        _lifetime.Dispose();
    }
}
