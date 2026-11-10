using System.Security.Claims;
using Microsoft.Extensions.Logging;
using TransportR.Interfaces;
using TransportR.Models;

namespace TransportR.Identity;

/// <summary>
/// Development mode identity provider that accepts client-provided values.
/// </summary>
/// <remarks>
/// Use this provider only for development/testing scenarios where authentication is disabled.
/// It trusts client-provided clientId and groups without validation.
/// </remarks>
internal class DevIdentityProvider : IClientIdentityProvider
{
    private readonly DevIdentityOptions _options;
    private readonly ILogger<DevIdentityProvider>? _logger;

    public DevIdentityProvider(DevIdentityOptions options, ILogger<DevIdentityProvider>? logger = null)
    {
        _options = options;
        _logger = logger;
    }

    public ClientIdentity? ResolveIdentity(
        ClaimsPrincipal? user,
        Guid? providedClientId,
        IEnumerable<string>? providedGroups)
    {
        // In dev mode, client MUST provide identity
        if (providedClientId == null)
        {
            if (!_options.AllowAnonymous)
            {
                _logger?.LogError("DevIdentityProvider: ClientId is required in dev mode (connection rejected)");
                return null;
            }
            providedClientId = Guid.NewGuid();
        }

        var groups = providedGroups?.ToHashSet() ?? new HashSet<string>();

        // Use default groups if none provided
        if (groups.Count == 0)
        {
            if (_options.DefaultGroups.Count > 0)
            {
                groups = _options.DefaultGroups.ToHashSet();
                _logger?.LogDebug("DevIdentityProvider: Using default groups [{Groups}]", string.Join(", ", groups));
            }
            else
            {
                _logger?.LogWarning("DevIdentityProvider: No groups provided and no defaults configured");
                return null;
            }
        }

        _logger?.LogInformation("DevIdentityProvider: Resolved identity - ClientId={ClientId}, Groups=[{Groups}] (DEV MODE)",
            providedClientId.Value, string.Join(", ", groups));

        return new ClientIdentity(providedClientId.Value, groups, TokenExpiry: null, Subject: null);
    }
}

/// <summary>
/// Options for development mode identity provider.
/// </summary>
public class DevIdentityOptions
{
    /// <summary>
    /// Default groups to assign if client doesn't provide any.
    /// </summary>
    public List<string> DefaultGroups { get; set; } = new();

    /// <summary>
    /// If true, allows connections without clientId (generates one automatically).
    /// </summary>
    public bool AllowAnonymous { get; set; } = false;
}
