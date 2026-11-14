namespace TransportR.Hub;

/// <summary>
/// What reached the hub of a client's write-transport probe, before any transfer. Each registration of
/// the client probes again, and the last announcement for a client is the one that stands.
/// </summary>
/// <param name="ClientId">The client registered on the stream of events the probe named.</param>
/// <param name="Transport">What carried the probe to the hub.</param>
public sealed record WriteTransportProbed(Guid ClientId, WriteTransport Transport);
