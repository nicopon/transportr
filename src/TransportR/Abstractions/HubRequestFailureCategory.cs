namespace TransportR;

/// <summary>
/// Why the hub refused a request or could not carry it out: the
/// <see cref="HubRequestFailedException.Category"/> of the failure.
/// </summary>
/// <remarks>
/// A category says no more of a client's rights than the message does. A target that is unknown, one
/// the caller may not reach and one that is gone are <see cref="Refused"/> alike, on purpose; the hub
/// log says which.
/// </remarks>
public enum HubRequestFailureCategory
{
    /// <summary>
    /// The hub refused the request, and does not say why. The target is unknown, out of the caller's
    /// rights or unreachable from this hub instance; or the caller's own token, groups or registration
    /// no longer let it act; or the route is reserved to the host. Says nothing of anyone's rights.
    /// </summary>
    Refused,

    /// <summary>
    /// The client is at a ceiling of the hub's, handshakes at once or open transfers (a 429). The
    /// same request may succeed once one of its transfers has ended.
    /// </summary>
    RateLimited,

    /// <summary>
    /// The handshake did not complete within its timeout: the clients did not answer the hub in time, or,
    /// on a client, the hub did not answer the transfer request within that timeout and the client's
    /// margin.
    /// </summary>
    HandshakeTimedOut,

    /// <summary>
    /// A client the hub asked to take part refused the transfer, or its own code failed while taking it.
    /// The hub asks the receiver of every transfer, and the sender of a transfer the host opens, a sender
    /// it cannot reach then falling here too. A receiver it cannot reach is <see cref="Refused"/>, never
    /// this.
    /// </summary>
    TargetRefusedOrFailedToPrepare,

    /// <summary>
    /// Both clients accepted, but the hub ended the transfer before its handshake completed: the access
    /// sweep, a renewal that withdrew a right, the inactivity check or the host.
    /// </summary>
    EndedDuringHandshake,

    /// <summary>
    /// The hub's answer to a transfer request named nothing to go on: a redirect to another instance
    /// that says which, or an acceptance without the transfer's id.
    /// </summary>
    Other,
}
