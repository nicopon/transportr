using Microsoft.Extensions.Options;
using TransportR.Hub.Configuration;
using TransportR.Interfaces;
using TransportR.Models;

namespace TransportR.Hub.Access;

/// <summary>What <see cref="AccessPolicy"/> found. Each surface turns it into its own refusal.</summary>
internal enum AccessVerdict
{
    Allowed,

    /// <summary>The token's <c>exp</c>, plus the configured margin, is behind us.</summary>
    TokenExpired,

    /// <summary>The identity holds no group: it has no right at all.</summary>
    NoGroups,

    /// <summary>The flow control service does not let the source's groups reach the target's.</summary>
    NotAuthorized,

    /// <summary>Two identities that had to be the same are not.</summary>
    WrongSubject,

    /// <summary>The hub reserves opening transfers to its host.</summary>
    HostOnly
}

/// <summary>
/// The one place that decides whether a client's token is still good, whether it holds any right,
/// whether it may send to another, and whether two identities are one.
/// </summary>
/// <remarks>
/// It judges what the hub recorded about a client (a registration) or what a request attests (an
/// identity), never the principal of a connection: that one is fixed when the connection opens and
/// says nothing of a token renewed since.
/// </remarks>
internal sealed class AccessPolicy
{
    private readonly IFlowControlService? _flowControl;
    private readonly TimeSpan _margin;
    private readonly bool _clientsMayOpenTransfers;
    private readonly TimeProvider _time;

    public AccessPolicy(IOptions<AccessOptions> options, TimeProvider timeProvider, IFlowControlService? flowControlService = null)
    {
        _margin = TimeSpan.FromSeconds(options.Value.TokenExpiryMarginSeconds);
        _clientsMayOpenTransfers = options.Value.ClientsMayOpenTransfers;
        _time = timeProvider;
        _flowControl = flowControlService;
    }

    public AccessVerdict Evaluate(ClientRegistration client) => Judge(client.Groups, client.TokenExpiry);

    public AccessVerdict Evaluate(ClientIdentity identity) => Judge(identity.Groups, identity.TokenExpiry);

    /// <summary>
    /// Whether <paramref name="source"/> may open a transfer to <paramref name="target"/>: both
    /// must be good, and the groups must allow it.
    /// </summary>
    public AccessVerdict Evaluate(ClientRegistration source, ClientRegistration target)
    {
        var verdict = Evaluate(source);
        if (verdict != AccessVerdict.Allowed) return verdict;

        verdict = Evaluate(target);
        return verdict != AccessVerdict.Allowed ? verdict : MayReach(source, target);
    }

    /// <summary>
    /// The groups half of <see cref="Evaluate(ClientRegistration, ClientRegistration)"/>, for discovery:
    /// a target whose token has expired stays listed, because a caller who finds nobody fails
    /// outright whereas one that tries a dead client retries.
    /// </summary>
    public AccessVerdict MayReach(ClientRegistration source, ClientRegistration target)
    {
        if (source.Groups.Count == 0 || target.Groups.Count == 0) return AccessVerdict.NoGroups;

        return _flowControl == null || _flowControl.CanSendTo(source.Groups, target.Groups)
            ? AccessVerdict.Allowed
            : AccessVerdict.NotAuthorized;
    }

    /// <summary>
    /// Whether a client may look for receivers and open a transfer itself. The host's
    /// <see cref="ITransferInitiator"/> is not judged here: it opens transfers either way.
    /// </summary>
    public AccessVerdict MayClientOpenTransfers() =>
        _clientsMayOpenTransfers ? AccessVerdict.Allowed : AccessVerdict.HostOnly;

    /// <summary>
    /// Strict equality of the subjects the IdP attested. A registration with no subject is not
    /// claimed by a caller that has one, which is what keeps another client's id from being taken
    /// over; two absent subjects are equal, which is dev mode, where nothing is attested.
    /// </summary>
    public static AccessVerdict SameIdentity(string? registeredSubject, string? presentedSubject) =>
        string.Equals(registeredSubject, presentedSubject, StringComparison.Ordinal)
            ? AccessVerdict.Allowed
            : AccessVerdict.WrongSubject;

    private AccessVerdict Judge(IReadOnlyCollection<string> groups, DateTime? tokenExpiry)
    {
        if (tokenExpiry is { } expiry && _time.GetUtcNow().UtcDateTime > expiry + _margin)
        {
            return AccessVerdict.TokenExpired;
        }

        return groups.Count == 0 ? AccessVerdict.NoGroups : AccessVerdict.Allowed;
    }
}
