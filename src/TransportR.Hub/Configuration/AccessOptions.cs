namespace TransportR.Hub.Configuration;

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
    /// transfers, through <see cref="Interfaces.ITransferInitiator"/>.
    /// </summary>
    public bool ClientsMayOpenTransfers { get; set; } = true;
}
