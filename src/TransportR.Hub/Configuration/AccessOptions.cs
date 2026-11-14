namespace TransportR.Hub;

/// <summary>
/// Options for the decision on whether a client may still act, and for how often it is re-taken.
/// </summary>
public class AccessOptions
{
    /// <summary>
    /// Seconds a token is still honoured after its <c>exp</c>, for clock drift between the hub and
    /// the identity provider. Default: 0, strict. A negative value is refused at startup.
    /// </summary>
    /// <remarks>
    /// This governs the hub's own decisions only. A request that opens a connection or a stream is
    /// judged by the host's JwtBearer middleware, against its own <c>ClockSkew</c>, so a margin
    /// beyond that one keeps open what is already open and nothing new.
    /// </remarks>
    public int TokenExpiryMarginSeconds { get; set; }

    /// <summary>
    /// Seconds between two passes that judge every registered client again: a token that ran out
    /// closes its connection and ends its transfers, and a transfer whose groups may no longer
    /// reach each other ends. Default: 5. Zero or less is refused at startup.
    /// </summary>
    /// <remarks>
    /// A client is closed at most this long after its expiry plus
    /// <see cref="TokenExpiryMarginSeconds"/>. Each instance judges the connections it holds and the
    /// transfers it runs, against the registrations of the shared store.
    /// </remarks>
    public int AccessSweepIntervalSeconds { get; set; } = 5;

    /// <summary>
    /// Whether a client may look for receivers and open a transfer itself. Default: true. When
    /// false, a client asking for either is refused with an error, and only the host opens
    /// transfers, through <see cref="ITransferInitiator"/>.
    /// </summary>
    public bool ClientsMayOpenTransfers { get; set; } = true;

    /// <summary>
    /// How many transfers one client may be opening at once on this instance, each a request of its
    /// whose handshake waits on the receiver. Default: 32; null for no ceiling. Zero or less is
    /// refused at startup.
    /// </summary>
    /// <remarks>
    /// A request past it is refused with a 429 before anything is written. Counted per instance,
    /// where the handshake waits. A transfer the host opens through <see cref="ITransferInitiator"/>
    /// is neither held to it nor counted.
    /// </remarks>
    public int? MaxHandshakesPerClient { get; set; } = 32;

    /// <summary>
    /// How many transfers one client may hold open among those it opened itself, on every instance
    /// that shares the state store. Default: 256; null for no ceiling. Zero or less is refused at
    /// startup.
    /// </summary>
    /// <remarks>
    /// A request past it is refused with a 429 before anything is written. Counted on the sender, from
    /// the sessions of the state store (<see cref="IStateStore.CountTransfersOpenedByAsync"/>). The
    /// count and the write of the new session are two steps, so requests that arrive together can pass
    /// it together, as many as <see cref="MaxHandshakesPerClient"/> lets in at once. A transfer the
    /// host opens through <see cref="ITransferInitiator"/> is neither held to it nor counted.
    /// </remarks>
    public int? MaxOpenTransfersPerClient { get; set; } = 256;
}
