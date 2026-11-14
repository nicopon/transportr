namespace TransportR.Hub.Interfaces;

/// <summary>
/// Interface for the server-side data broker that bridges sender and receiver streams.
/// </summary>
/// <remarks>
/// <para>
/// The server acts as a "dumb pipe" - it forwards bytes between sender and receiver
/// without deserializing or processing the content. This design ensures maximum
/// performance and prevents the server from becoming a bottleneck.
/// </para>
/// <para>
/// Each transfer uses separate bridges for Data and Control channels.
/// </para>
/// </remarks>
internal interface ITransportRServer
{
    /// <summary>
    /// Handles the sending stream (Client → Server).
    /// </summary>
    /// <param name="transferId">Unique identifier for the transfer.</param>
    /// <param name="type">The type of connection (Data or Control).</param>
    /// <param name="input">The incoming stream from the sender.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <remarks>
    /// This method blocks until the sender stream is complete or cancelled.
    /// It pipes data directly to the corresponding receiver stream.
    /// </remarks>
    Task HandleSendAsync(string transferId, ConnectionType type, Stream input, CancellationToken cancellationToken = default);

    /// <summary>
    /// Handles the receiving stream (Server → Client).
    /// </summary>
    /// <param name="transferId">Unique identifier for the transfer.</param>
    /// <param name="type">The type of connection (Data or Control).</param>
    /// <param name="output">The outgoing stream to the receiver.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <remarks>
    /// This method blocks until the transfer is complete or cancelled.
    /// It waits for the sender stream to connect and then receives piped data.
    /// </remarks>
    Task HandleReceiveAsync(string transferId, ConnectionType type, Stream output, CancellationToken cancellationToken = default);
}
