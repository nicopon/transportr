namespace TransportR.Configuration;

/// <summary>
/// Controls how long a client registration survives in the state store.
/// </summary>
/// <remarks>
/// <para>
/// A registration is not deleted when its control connection drops: on an unstable network the
/// same client usually reconnects within seconds under the same identifier, and removing it would
/// break a transfer that was about to resume. It is instead marked with
/// <see cref="Models.ClientInfo.DisconnectedAtUtc"/> and given a grace period.
/// </para>
/// <para>
/// The two values below are the eviction policy that goes with that marker. Without one, the two
/// implementations fail in opposite directions: an in-memory registration lives forever, and a
/// Redis one disappears on a fixed timer whether or not the client is still there.
/// </para>
/// </remarks>
public class ClientRegistryConfiguration
{
    /// <summary>
    /// How long a registration whose connection has dropped is kept before eviction.
    /// Default: 300 seconds.
    /// </summary>
    /// <remarks>
    /// This is the window in which a reconnecting client can claim its registration back. It
    /// matches <c>DataPlaneConfiguration.BridgeTimeoutSeconds</c> on purpose: there is no
    /// point keeping a registration alive longer than the transfer it would resume.
    /// </remarks>
    public int DisconnectedGraceSeconds { get; set; } = 300;

    /// <summary>
    /// How long a connected registration survives without the hub hearing from that client at all.
    /// Default: 3600 seconds. Applies to stores with server-side expiry (Redis); the in-memory
    /// store keeps connected registrations for the lifetime of the process.
    /// </summary>
    /// <remarks>
    /// This is a backstop for registrations orphaned by a hub that died without running
    /// <c>OnDisconnectedAsync</c>, not a liveness check. Any hub call from the client refreshes it.
    /// Set it comfortably above the longest a client may sit idle between transfers: expiring a
    /// client that is still connected removes it from discovery while it is perfectly usable, and
    /// a caller that finds nobody fails outright rather than retrying.
    /// </remarks>
    public int ConnectedTtlSeconds { get; set; } = 3600;
}
