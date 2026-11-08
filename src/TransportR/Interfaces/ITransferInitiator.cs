namespace TransportR.Interfaces;

/// <summary>
/// Opens a transfer between two registered clients on behalf of the application hosting the hub,
/// rather than at the sender's request.
/// </summary>
/// <remarks>
/// The transfer goes through the same authorization and handshake as one a sender initiates. The
/// sender's handle reaches it through <see cref="IDataSender{T}.OnSendRequested"/>, whereas a sender
/// that asks for a transfer is not called back and starts from the id it is returned; the receiver
/// sees an ordinary <see cref="IDataReceiver{T}.OnTransferStarted"/>.
/// </remarks>
public interface ITransferInitiator
{
    /// <summary>
    /// Opens a transfer from <paramref name="senderClientId"/> to <paramref name="receiverClientId"/>
    /// and returns once both clients are ready.
    /// </summary>
    /// <param name="senderClientId">The registered client that will send.</param>
    /// <param name="receiverClientId">The registered client that will receive.</param>
    /// <param name="timeoutMs">Timeout for the handshake in milliseconds.</param>
    /// <param name="cancellationToken">Abandons the handshake; the receiver is told to cancel.</param>
    /// <returns>
    /// The transfer identifier, which the handles both clients were given carry as
    /// <c>TransferId</c>. A client receives its handle during the handshake, before this returns.
    /// </returns>
    /// <exception cref="Abstractions.HubRequestFailedException">
    /// Either client is unknown, has an expired token, is not authorized to reach the other, is
    /// unreachable or refuses, or the handshake times out. The message is generic; the detail is in
    /// the hub log.
    /// </exception>
    Task<string> InitTransferAsync(
        Guid senderClientId,
        Guid receiverClientId,
        int timeoutMs,
        CancellationToken cancellationToken = default);
}
