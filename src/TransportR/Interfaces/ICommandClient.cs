using System.Threading.Tasks;

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
    /// Called by the hub to notify a sender that the receiver is ready.
    /// </summary>
    /// <param name="transferId">The unique transfer identifier.</param>
    /// <param name="batchSize">The negotiated batch size for this transfer.</param>
    /// <param name="receiverId">The ID of the receiving client.</param>
    /// <returns>True if the sender is ready; false to abort the transfer.</returns>
    Task<bool> GetReadyToSend(string transferId, int batchSize, Guid receiverId);

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

