// Re-export abstract classes from Abstractions namespace for backward compatibility
using TransportR.Abstractions;
using TransportR.Models;

namespace TransportR.Client;

/// <summary>
/// Base interface for TransportR clients that can connect to and disconnect from a hub.
/// </summary>
public interface IDataClient
{
    /// <summary>
    /// Gets the unique identifier for this client.
    /// </summary>
    Guid ClientId { get; }

    /// <summary>
    /// Connects to the hub with specified groups.
    /// </summary>
    /// <param name="groups">Optional groups. When authenticated and null, derived from JWT scopes.</param>
    /// <param name="cancellationToken">Token to cancel the connection attempt.</param>
    /// <exception cref="HubRequestFailedException">
    /// The hub refused the registration. The client is left as it was before the call, and may connect
    /// again.
    /// </exception>
    Task ConnectAsync(IEnumerable<string>? groups = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Disconnects from the hub and cleans up all active sessions. The client cannot connect again
    /// afterwards: a host that needs the hub again builds another one.
    /// </summary>
    Task DisconnectAsync();

    /// <summary>
    /// Gets information about all active transfers.
    /// </summary>
    /// <returns>A list of transfer information for all active sessions.</returns>
    IReadOnlyList<TransferInfo> GetTransfers();
}

/// <summary>
/// Interface for a client that can receive data transfers.
/// </summary>
/// <typeparam name="T">The type of items to receive.</typeparam>
public interface IDataReceiver<T> : IDataClient
{
    /// <summary>
    /// Event fired when a new transfer is initiated targeting this receiver.
    /// </summary>
    /// <remarks>
    /// The handler consumes items from the transfer's <see cref="Receive{T}.ReceiveAsync"/> method
    /// and owns the handle it is given: nothing else disposes it, and a handle left alive keeps its
    /// session registered with the client and reporting to the hub for as long as the client lives.
    /// </remarks>
    event Func<Receive<T>, Task> OnTransferStarted;
}

/// <summary>
/// Interface for a client that can send data transfers.
/// </summary>
/// <typeparam name="T">The type of items to send.</typeparam>
public interface IDataSender<T> : IDataClient
{
    /// <summary>
    /// Initiates a new transfer to the specified target client.
    /// </summary>
    /// <param name="targetClientId">The ID of the receiver client.</param>
    /// <param name="batchSize">Optional batch size override. Uses default if not specified.</param>
    /// <returns>A Send handle to transmit items through.</returns>
    /// <exception cref="HubRequestFailedException">
    /// The hub refused the transfer, or the handshake failed: the receiver is unknown, unreachable or
    /// refused it, or it timed out.
    /// </exception>
    Task<Send<T>> InitTransferAsync(Guid targetClientId, int? batchSize = null);

    /// <summary>
    /// Event fired when the application hosting the hub opens a transfer from this client, rather
    /// than this client calling <see cref="InitTransferAsync"/>.
    /// </summary>
    /// <remarks>
    /// The handler owns the handle it is given, as with <c>OnTransferStarted</c>: it sends through
    /// it, completes it and disposes it. It runs inside the handshake, which is bounded by a
    /// timeout, so it hands the handle off and returns rather than sending from within. With no
    /// handler attached the client refuses such transfers, and the host's request fails.
    /// </remarks>
    event Func<Send<T>, Task> OnSendRequested;

    /// <summary>
    /// Gets a list of receivers that this client is authorized to send data to.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the request.</param>
    /// <returns>List of authorized receiver clients.</returns>
    /// <exception cref="HubRequestFailedException">The hub refused the request.</exception>
    Task<List<ClientInfo>> GetReceiversAsync(CancellationToken cancellationToken = default);
}

