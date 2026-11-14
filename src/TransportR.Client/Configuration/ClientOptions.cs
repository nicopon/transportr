namespace TransportR.Client.Configuration;

/// <summary>
/// What a client runs with, assembled by <c>TransportRClientBuilder.Build</c>. Only the builder's
/// own settings reach it; the timings and the buffer size are fixed.
/// </summary>
internal class ClientOptions
{
    /// <summary>
    /// Timeout for Pong response during reconnection. Default: 10 seconds.
    /// </summary>
    public TimeSpan PongTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Buffer size for HTTP stream writes. Default: 512KB.
    /// </summary>
    public int StreamBufferSize { get; set; } = 512 * 1024;

    /// <summary>
    /// What may carry this client's write streams. Default: a WebSocket where the path carries one.
    /// </summary>
    public WriteTransportPolicy WriteTransportPolicy { get; set; } = WriteTransportPolicy.PreferWebSocket;

    /// <summary>
    /// How long opening a WebSocket may take, the upgrade and the probe together, before the path is
    /// taken as not carrying one. Default: 5 seconds.
    /// </summary>
    public TimeSpan WebSocketOpenBound { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Whether the client proves, after each registration, what the path to the hub carries of a write
    /// stream, for the hub to announce. Default: off.
    /// </summary>
    public bool ProbeWriteTransport { get; set; }

    /// <summary>
    /// Interval for per-transfer heartbeat reporting. Default: 1 second.
    /// </summary>
    /// <remarks>
    /// This reports transfer statistics and only runs while a transfer does. It does not keep the
    /// client's registration alive — see <see cref="PresenceHeartbeatInterval"/>.
    /// Keep it well below the hub's <c>TransferInactivityTimeoutSeconds</c> (60 s): the hub judges a
    /// transfer from its registration, and one that stays silent for that long is terminated.
    /// </remarks>
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Interval at which the client tells the hub it is still there, independently of any
    /// transfer. Default: 15 seconds.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is what keeps a registration alive while the client sits idle between transfers, and
    /// what detects that the hub no longer knows this stream — after a hub restart, an evicted
    /// registration, or another stream of the same identity that took the registration over.
    /// </para>
    /// <para>
    /// It is also how the hub knows the client is there at all: the client announces it when it opens
    /// its stream, and the hub closes a stream it has heard nothing from for two of them. A cut that
    /// closes no connection leaves no other trace on the hub's side, so the default puts a client cut
    /// off in silence out of discovery within about 30 seconds.
    /// </para>
    /// <para>
    /// Keep it comfortably below the hub's <c>ConnectedTtlSeconds</c>, which is what it refreshes.
    /// </para>
    /// </remarks>
    public TimeSpan PresenceHeartbeatInterval { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// How long <c>CompleteAsync</c> keeps re-sending its Complete signal while nothing at all is
    /// advancing, before giving up on the transfer. Default: 60 seconds.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The clock measures a stall, not the completion itself: every acknowledgement that arrives
    /// resets it. A transfer whose tail is still draining — the receiver refusing Complete because
    /// batches are still in flight is the normal ending, not the exceptional one — keeps
    /// acknowledging, so a legitimately long ending is never cut short. A peer that has gone away
    /// acknowledges nothing, and that is what this bounds.
    /// </para>
    /// <para>
    /// The bound is the client's own, and at the hub's defaults it is reached first. While
    /// <c>CompleteAsync</c> waits, the sender's per-transfer heartbeats keep the transfer active, so
    /// <c>TransferInactivityTimeoutSeconds</c> does not end it. A receiver that went away leaves its
    /// bridges short of <c>Active</c>, which the hub ends no sooner than <c>BridgeTimeoutSeconds</c>
    /// (300 s by default) later; one still connected but no longer reading keeps them <c>Active</c>,
    /// and only <c>StalledProgressTimeoutSeconds</c>, where it is set, ends that transfer at the hub.
    /// </para>
    /// <para>
    /// The same clock bounds a producer parked on a full <see cref="MaxInFlightBatches"/> window:
    /// both waits are released only by an acknowledgement.
    /// </para>
    /// </remarks>
    public TimeSpan CompleteStallTimeout { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// How long the hub waits, for a transfer this client opens, on the receiver's answer. Default: 20
    /// seconds.
    /// </summary>
    /// <remarks>
    /// The hub declares no default for it: the request carries this value, so one setting in one place
    /// answers for it. The hub answers within it or not at all, which is what bounds
    /// <c>InitTransferAsync</c> once a margin is added.
    /// </remarks>
    public TimeSpan HandshakeTimeout { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>
    /// How long before a token expires the client asks its provider for a new one and presents it to
    /// the hub. Default: 5 minutes, and never more than half of what is left of the token.
    /// </summary>
    /// <remarks>
    /// Also the point from which a transport that connects asks the provider rather than reuse the
    /// token it holds.
    /// </remarks>
    public TimeSpan TokenRefreshThreshold { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Timeout for batch accumulation in milliseconds. Default: 1000ms.
    /// </summary>
    public int BatchFlushIntervalMs { get; set; } = 1000;

    /// <summary>
    /// Maximum number of batches allowed in flight — flushed by the accumulator but not yet
    /// acknowledged by the receiver. <c>null</c> (the default) means unbounded.
    /// </summary>
    public int? MaxInFlightBatches { get; set; }

    /// <summary>
    /// How much the receiver may hold for a consumer that is behind, before the incoming stream
    /// is made to wait. <c>null</c> (the default) means unbounded.
    /// </summary>
    public BufferCapacity? ReceiveBufferCapacity { get; set; }

    /// <summary>
    /// The largest batch payload, in bytes, this client receives or sends. Default: 256 MiB.
    /// </summary>
    /// <remarks>
    /// A receiver checks the length a chunk announces before it rents memory for it, and refuses one
    /// above this as a protocol violation that fails the transfer. A sender fails its transfer on a
    /// serialized batch above it, and a byte client refuses at <c>Build</c> a batch size above it.
    /// Closed by default on purpose: the length comes off the wire, from a peer that may lie.
    /// </remarks>
    public int MaxBatchBytes { get; set; } = DefaultMaxBatchBytes;

    public const int DefaultMaxBatchBytes = 256 * 1024 * 1024;
}
