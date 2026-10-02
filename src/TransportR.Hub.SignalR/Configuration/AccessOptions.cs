namespace TransportR.Hub.SignalR.Configuration;

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
}
