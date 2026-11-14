using System.Security.Claims;
using Microsoft.Extensions.Logging;

namespace TransportR.Hub.Identity;

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
