namespace TransportR.Hub;

/// <summary>
/// One opening of a client's write stream, as the hub took it on and what carries it. A reconnection is
/// another opening, and the last one for a transfer's stream is the one that stands.
/// </summary>
/// <param name="TransferId">The transfer the stream belongs to.</param>
/// <param name="ClientId">The participant that writes it: the sender for <see cref="ConnectionType.Data"/>, the receiver for <see cref="ConnectionType.Control"/>.</param>
/// <param name="Stream">The stream the client writes.</param>
/// <param name="Transport">What carries the client's writes on this opening.</param>
public sealed record WriteStreamOpened(string TransferId, Guid ClientId, ConnectionType Stream, WriteTransport Transport);
