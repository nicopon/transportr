
namespace TransportR.Auth.SimpleIdp;

/// <summary>
/// Configuration options for the embedded SimpleIdp.
/// </summary>
public class SimpleIdpOptions
{
    /// <summary>
    /// Token lifetime. Default is 1 hour.
    /// </summary>
    public TimeSpan TokenLifetime { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// Configured clients.
    /// </summary>
    public List<SimpleIdpClient> Clients { get; } = new();

    /// <summary>
    /// Adds a client with the specified credentials and groups.
    /// </summary>
    /// <param name="clientId">The client identifier.</param>
    /// <param name="secret">The client secret.</param>
    /// <param name="groups">The groups (will be converted to transportr:group:* scopes).</param>
    public SimpleIdpOptions AddClient(string clientId, string secret, params string[] groups)
    {
        Clients.Add(new SimpleIdpClient
        {
            ClientId = clientId,
            Secret = secret,
            Groups = groups.ToList()
        });
        return this;
    }

    /// <summary>
    /// Adds a client with raw scopes (e.g., "transportr:group:senders").
    /// Use this for advanced scenarios where you need full control over scopes.
    /// </summary>
    /// <param name="clientId">The client identifier.</param>
    /// <param name="secret">The client secret.</param>
    /// <param name="scopes">Raw scopes to grant.</param>
    /// <param name="displayName">Optional display name.</param>
    public SimpleIdpOptions AddClientWithScopes(string clientId, string secret, IEnumerable<string> scopes, string? displayName = null)
    {
        Clients.Add(new SimpleIdpClient
        {
            ClientId = clientId,
            Secret = secret,
            Scopes = scopes.ToList(),
            DisplayName = displayName
        });
        return this;
    }
}

/// <summary>
/// Represents a client configuration for the SimpleIdp.
/// </summary>
public class SimpleIdpClient
{
    /// <summary>
    /// The client identifier.
    /// </summary>
    public string ClientId { get; set; } = string.Empty;

    /// <summary>
    /// The client secret.
    /// </summary>
    public string Secret { get; set; } = string.Empty;

    /// <summary>
    /// Groups this client belongs to (converted to transportr:group:* scopes).
    /// </summary>
    public List<string> Groups { get; set; } = new();

    /// <summary>
    /// Raw scopes for advanced scenarios. If set, Groups is ignored.
    /// </summary>
    public List<string> Scopes { get; set; } = new();

    /// <summary>
    /// Display name for the client.
    /// </summary>
    public string? DisplayName { get; set; }

    /// <summary>
    /// Gets the effective scopes for this client.
    /// Uses Scopes if set, otherwise converts Groups to transportr:group:* format.
    /// </summary>
    public IEnumerable<string> GetEffectiveScopes()
    {
        if (Scopes.Count > 0)
            return Scopes;
        return Groups.Select(g => $"transportr:group:{g}");
    }
}

