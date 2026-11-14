namespace TransportR;

/// <summary>
/// What carries a client's write stream: the sender's data, the receiver's acknowledgements. Read
/// streams are long GETs whatever this says.
/// </summary>
public enum WriteTransport
{
    /// <summary>One WebSocket per opening of the stream.</summary>
    WebSocket,

    /// <summary>
    /// A succession of short POST requests, which pass wherever a request does, at the price of one
    /// segment per round trip.
    /// </summary>
    Segments,
}
