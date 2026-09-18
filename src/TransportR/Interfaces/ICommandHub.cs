using TransportR.Models;

namespace TransportR.Interfaces;

/// <summary>
/// Hub interface that clients call to interact with the broker.
/// </summary>
/// <remarks>
/// This interface defines the SignalR hub methods for client registration,
/// discovery, transfer orchestration, and heartbeat reporting.
/// </remarks>
public interface ICommandHub
{
    /// <summary>
    /// Registers a client with the hub.
    /// </summary>
    /// <param name="clientId">Optional client identifier. When authenticated, derived from JWT 'sub' claim if null.</param>
    /// <param name="groups">Optional groups. When authenticated, derived from JWT scopes if null.</param>
    Task Connect(Guid? clientId = null, IEnumerable<string>? groups = null);

    /// <summary>
    /// Gets a list of receivers that the calling client is authorized to send data to.
    /// </summary>
    /// <remarks>
    /// Only returns clients with CanReceive=true that belong to groups the caller is authorized to send to.
    /// </remarks>
    /// <returns>List of authorized receivers.</returns>
    Task<List<ClientInfo>> GetReceivers();

    /// <summary>
    /// Initiates a transfer between the calling client and the target.
    /// </summary>
    /// <param name="targetClientId">The ID of the receiver client.</param>
    /// <param name="batchSize">Optional batch size (defaults to 500).</param>
    /// <param name="timeoutMs">Timeout for the handshake in milliseconds.</param>
    /// <returns>The unique transfer identifier.</returns>
    /// <exception cref="Exception">Thrown if the target is not found or handshake times out.</exception>
    Task<string> InitTransfer(Guid targetClientId, int? batchSize = 500, int timeoutMs = 5000);
    
    /// <summary>
    /// Reports that the calling connection is still present, and asks whether the hub still knows
    /// it. Independent of any transfer.
    /// </summary>
    /// <returns>
    /// <c>true</c> if the hub holds a registration for this connection; <c>false</c> if the caller
    /// must register again with <see cref="Connect"/>.
    /// </returns>
    /// <remarks>
    /// <para>
    /// Presence is what a registration expresses, and a registration expires. The per-transfer
    /// heartbeats below cannot keep it alive: they only exist while a transfer does, and they
    /// carry transfer statistics, not liveness of the client.
    /// </para>
    /// <para>
    /// The <c>false</c> answer matters as much as the keep-alive. A hub that restarted, a
    /// registration evicted after its grace period, or a SignalR reconnection that gave the client
    /// a new connection id all leave the caller connected but unknown — discovery returns nothing
    /// for it and nobody can open a transfer against it. Re-registering is how it recovers.
    /// </para>
    /// </remarks>
    Task<bool> Heartbeat();

    /// <summary>
    /// Reports sender metrics to the hub for monitoring.
    /// </summary>
    /// <param name="transferId">The transfer identifier.</param>
    /// <param name="itemsSent">Total items sent so far.</param>
    /// <param name="acksReceived">Total ACKs received.</param>
    Task SenderHeartbeat(string transferId, long itemsSent, long acksReceived);

    /// <summary>
    /// Reports receiver metrics to the hub for monitoring.
    /// </summary>
    /// <param name="transferId">The transfer identifier.</param>
    /// <param name="itemsReceived">Total items received so far.</param>
    /// <param name="acksSent">Total ACKs sent.</param>
    /// <param name="lastAckedBatchId">The last batch ID that was acknowledged (for checkpoint).</param>
    Task ReceiverHeartbeat(string transferId, long itemsReceived, long acksSent, int lastAckedBatchId = -1);

    /// <summary>
    /// Probes connectivity to the hub. Used to verify network reachability before reconnecting HTTP streams.
    /// </summary>
    /// <param name="transferId">The transfer identifier for context.</param>
    /// <returns>The hub responds with Pong callback to confirm connectivity.</returns>
    Task Ping(string transferId);



    /// <summary>
    /// Notifies the hub that a transfer has completed successfully.
    /// The hub will clean up resources and notify the other party to close streams.
    /// </summary>
    /// <param name="transferId">The transfer identifier.</param>
    Task TransferComplete(string transferId);
}

