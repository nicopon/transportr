namespace TransportR.Models;

/// <summary>
/// Information about a connected client.
/// </summary>
public class ClientInfo
{
    /// <summary>
    /// Gets or sets the unique client identifier.
    /// </summary>
    public Guid ClientId { get; set; }

    /// <summary>
    /// Gets or sets the groups this client belongs to.
    /// </summary>
    public HashSet<string> Groups { get; set; } = [];

    /// <summary>
    /// When this client's control connection was last seen dropping, or <c>null</c> if it is
    /// currently connected.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A registration is deliberately NOT removed when its connection drops — on an unstable
    /// network the same client usually comes back within seconds under the same identifier, and
    /// deleting it would break a transfer that was about to resume. The consequence is that
    /// discovery can hand back a client whose process is gone, and nothing distinguishes it from a
    /// live one until a transfer is opened against it.
    /// </para>
    /// <para>
    /// This field exposes that difference rather than acting on it. The hub does not filter on it:
    /// a caller that finds nobody fails outright, whereas a caller that tries a dead client simply
    /// tries again, so hiding a briefly silent client is the worse error. Callers that have more
    /// than one candidate should prefer those with no value here.
    /// </para>
    /// </remarks>
    public DateTime? DisconnectedAtUtc { get; set; }
}
