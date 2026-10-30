namespace TransportR.Auth.ClientCredentials;

/// <summary>
/// Configuration options for OAuth client credentials token acquisition.
/// </summary>
internal class ClientCredentialsOptions
{
    /// <summary>
    /// The token endpoint URL (e.g., "http://localhost:5252/connect/token").
    /// </summary>
    public string TokenEndpoint { get; set; } = string.Empty;

    /// <summary>
    /// The OAuth client identifier.
    /// </summary>
    public string ClientId { get; set; } = string.Empty;

    /// <summary>
    /// The OAuth client secret.
    /// </summary>
    public string ClientSecret { get; set; } = string.Empty;

    /// <summary>
    /// Optional scope to request (e.g., "transportr:group:senders").
    /// </summary>
    public string? Scope { get; set; }

    /// <summary>
    /// Time before token expiry to trigger a refresh. Default is 5 minutes.
    /// </summary>
    public TimeSpan RefreshThreshold { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Validates that required options are configured.
    /// </summary>
    public bool IsConfigured => 
        !string.IsNullOrEmpty(TokenEndpoint) && 
        !string.IsNullOrEmpty(ClientId) && 
        !string.IsNullOrEmpty(ClientSecret);
}
