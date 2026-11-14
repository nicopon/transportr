using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using TransportR.Client.Internal;

namespace TransportR.Client.Auth;

/// <summary>
/// Token provider that acquires OAuth tokens using the client credentials flow.
/// Automatically caches tokens and refreshes them before expiry.
/// </summary>
internal class ClientCredentialsTokenProvider : IDisposable
{
    private readonly ClientCredentialsOptions _options;
    private readonly HttpClient _httpClient;
    private readonly ILogger<ClientCredentialsTokenProvider>? _logger;
    private readonly SemaphoreSlim _tokenLock = new(1, 1);

    private string? _cachedToken;
    private DateTime? _tokenExpiry;
    private bool _disposed;

    /// <summary>
    /// Creates a new token provider with the specified options.
    /// </summary>
    /// <param name="options">Client credentials configuration.</param>
    /// <param name="logger">Optional logger.</param>
    /// <param name="httpClient">Optional HttpClient (for testing).</param>
    public ClientCredentialsTokenProvider(
        ClientCredentialsOptions options,
        ILogger<ClientCredentialsTokenProvider>? logger = null,
        HttpClient? httpClient = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));

        if (!options.IsConfigured)
            throw new ArgumentException("ClientCredentialsOptions is not fully configured", nameof(options));

        _logger = logger;
        _httpClient = httpClient ?? new HttpClient();
    }

    /// <summary>
    /// Gets a valid access token, acquiring or refreshing as needed.
    /// This method is thread-safe.
    /// </summary>
    public async Task<string> GetTokenAsync(CancellationToken cancellationToken = default)
    {
        // Fast path: return cached token if still valid
        if (_cachedToken != null && _tokenExpiry.HasValue &&
            DateTime.UtcNow < _tokenExpiry.Value - _options.RefreshThreshold)
        {
            return _cachedToken;
        }

        // Slow path: acquire lock and refresh token
        await _tokenLock.WaitAsync(cancellationToken);
        try
        {
            // Double-check after acquiring lock
            if (_cachedToken != null && _tokenExpiry.HasValue &&
                DateTime.UtcNow < _tokenExpiry.Value - _options.RefreshThreshold)
            {
                return _cachedToken;
            }

            _logger?.LogInformation("Acquiring token from {TokenEndpoint}...", _options.TokenEndpoint);

            var token = await AcquireTokenAsync(cancellationToken);
            _cachedToken = token;
            _tokenExpiry = JwtHelper.ExtractExpiry(token);

            _logger?.LogInformation("Token acquired, expires at {Expiry}",
                _tokenExpiry?.ToString("u") ?? "unknown");

            return token;
        }
        finally
        {
            _tokenLock.Release();
        }
    }

    /// <summary>
    /// Gets the token provider delegate for use with TransportRClient.
    /// </summary>
    public Func<Task<string>> AsTokenProvider() => () => GetTokenAsync();

    private async Task<string> AcquireTokenAsync(CancellationToken cancellationToken)
    {
        var content = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials",
            ["client_id"] = _options.ClientId,
            ["client_secret"] = _options.ClientSecret,
            ["scope"] = _options.Scope ?? string.Empty
        });

        var response = await _httpClient.PostAsync(_options.TokenEndpoint, content, cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            var error = await response.Content.ReadAsStringAsync(cancellationToken);
            throw new InvalidOperationException(
                $"Token acquisition failed: {response.StatusCode} - {error}");
        }

        var json = await response.Content.ReadAsStringAsync(cancellationToken);
        using var doc = JsonDocument.Parse(json);

        if (!doc.RootElement.TryGetProperty("access_token", out var tokenElement))
        {
            throw new InvalidOperationException("Token response did not contain access_token");
        }

        return tokenElement.GetString()
            ?? throw new InvalidOperationException("access_token was null");
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _tokenLock.Dispose();
        _httpClient.Dispose();
    }
}
