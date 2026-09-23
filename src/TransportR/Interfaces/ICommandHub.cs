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
    /// <remarks>
    /// Refused when the resolved client id is already registered under a different attested
    /// subject, including a registration whose connection dropped but is still within its grace
    /// period. Registering again under the same subject replaces the registration.
    /// </remarks>
    Task Connect(Guid? clientId = null, IEnumerable<string>? groups = null);

    /// <summary>
    /// Gets a list of receivers that the calling client is authorized to send data to.
    /// </summary>
    /// <remarks>
    /// Only returns clients belonging to groups the caller is authorized to send to. A registration
    /// whose control connection is down is still returned, carrying its disconnection marker: a
    /// caller that finds nobody fails outright, whereas one that tries a dead client retries.
    /// </remarks>
    /// <returns>List of authorized receivers.</returns>
    Task<List<ClientInfo>> GetReceivers();

    /// <summary>
    /// Initiates a transfer between the calling client and the target.
    /// </summary>
    /// <param name="targetClientId">The ID of the receiver client.</param>
    /// <param name="batchSize">Items per batch.</param>
    /// <param name="timeoutMs">Timeout for the handshake in milliseconds.</param>
    /// <returns>The unique transfer identifier.</returns>
    /// <exception cref="Exception">Thrown if the target is not found or handshake times out.</exception>
    /// <remarks>
    /// Neither argument has a default. Both are settings the caller already holds one value for,
    /// and a second default here would be a second answer to the same question.
    /// </remarks>
    Task<string> InitTransfer(Guid targetClientId, int batchSize, int timeoutMs);
    
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
    Task ReceiverHeartbeat(string transferId, long itemsReceived, long acksSent);

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

