
namespace TransportR.Interfaces;

/// <summary>
/// Callback interface that clients implement to receive commands from the hub.
/// </summary>
/// <remarks>
/// These methods are invoked by the hub during transfer orchestration via SignalR.
/// </remarks>
public interface ICommandClient
{
    /// <summary>
    /// Called by the hub to notify a receiver that a transfer is about to start.
    /// </summary>
    /// <param name="transferId">The unique transfer identifier.</param>
    /// <param name="senderId">The ID of the client initiating the transfer.</param>
    /// <returns>True if the receiver is ready; false to reject the transfer.</returns>
    Task<bool> GetReadyToReceive(string transferId, Guid senderId);

    /// <summary>
    /// Called by a hub that readies the sender of a transfer the sender asked for, once the receiver
    /// is ready.
    /// </summary>
    /// <remarks>
    /// <c>TransportR.Hub.SignalR</c> does not call it: the hub runs one invocation at a time per
    /// client, so the answer would queue behind anything else the sender invoked during its own
    /// <see cref="ICommandHub.InitTransfer"/>. The sender starts its session from the id that
    /// <see cref="ICommandHub.InitTransfer"/> returns instead.
    /// </remarks>
    /// <param name="transferId">The unique transfer identifier.</param>
    /// <param name="batchSize">The negotiated batch size for this transfer.</param>
    /// <param name="receiverId">The ID of the receiving client.</param>
    /// <returns>True if the sender is ready; false to abort the transfer.</returns>
    Task<bool> GetReadyToSend(string transferId, int batchSize, Guid receiverId);

    /// <summary>
    /// Called by the hub, once the receiver is ready, when the application hosting it opens a
    /// transfer from this client, which did not ask for it.
    /// </summary>
    /// <param name="transferId">The unique transfer identifier.</param>
    /// <param name="batchSize">The negotiated batch size for this transfer.</param>
    /// <param name="receiverId">The ID of the receiving client.</param>
    /// <returns>
    /// True once the transfer has been handed to the client's code; false when nothing in the
    /// client takes such transfers, which fails the transfer before any data moves.
    /// </returns>
    Task<bool> RequestSend(string transferId, int batchSize, Guid receiverId);

    /// <summary>
    /// Called by the hub to cancel an in-progress or pending transfer.
    /// </summary>
    /// <param name="transferId">The unique transfer identifier to cancel.</param>
    Task CancelTransfer(string transferId);

    /// <summary>
    /// Called by the hub in response to a Ping to confirm connectivity.
    /// </summary>
    /// <param name="transferId">The transfer identifier from the Ping request.</param>
    Task Pong(string transferId);
}

