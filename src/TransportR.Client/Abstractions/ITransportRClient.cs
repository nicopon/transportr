// Re-export abstract classes from Abstractions namespace for backward compatibility

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
    /// Registers the handler called when a new transfer is initiated targeting this receiver.
    /// </summary>
    /// <param name="handler">The handler receiving each transfer handle.</param>
    /// <remarks>
    /// <para>
    /// Only one handler may be registered per client. Calling this method more than once throws
    /// <see cref="InvalidOperationException"/>.
    /// </para>
    /// <para>
    /// The handler consumes items from the transfer's <see cref="Receive{T}.ReceiveAsync"/> method
    /// and owns the handle it is given: nothing else disposes it, and a handle left alive keeps its
    /// session registered with the client and reporting to the hub for as long as the client lives.
    /// </para>
    /// <para>
    /// The client answers the hub once the handler has returned its task, not once that task has
    /// ended: the handler may consume in its body, or hand the handle off and return. An exception
    /// thrown before it returns, or a task already faulted or cancelled when it does, refuses the
    /// transfer, and the call that opened it fails. A task that faults or is cancelled later fails the
    /// transfer instead: the client logs it and releases the handle in the handler's place, which
    /// abandons a transfer that has not ended, and the sender fails with
    /// <see cref="TransferFailedException"/>. A transfer that has ended keeps its outcome.
    /// </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="handler"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException">A handler is already registered.</exception>
    void OnTransferStarted(Func<Receive<T>, Task> handler);
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
    /// <param name="cancellationToken">Token to cancel the transfer initiation.</param>
    /// <returns>A Send handle to transmit items through.</returns>
    /// <exception cref="HubRequestFailedException">
    /// The hub refused the transfer, or the handshake failed: the receiver is unknown, unreachable or
    /// refused it, or it timed out.
    /// </exception>
    Task<Send<T>> InitTransferAsync(Guid targetClientId, int? batchSize = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Registers the handler called when the application hosting the hub opens a transfer from this
    /// client, rather than this client calling <see cref="InitTransferAsync"/>.
    /// </summary>
    /// <param name="handler">The handler sending through each transfer handle.</param>
    /// <remarks>
    /// Only one handler may be registered per client. Calling this method more than once throws
    /// <see cref="InvalidOperationException"/>.
    /// The handler owns the handle it is given, as with <see cref="IDataReceiver{T}.OnTransferStarted"/>:
    /// it sends through it, completes it and disposes it. The client answers the hub once the handler has returned
    /// its task, not once that task has ended, so the handshake's timeout bounds only what the
    /// handler does before it returns: it may send in its body, or hand the handle off and return.
    /// An exception thrown before it returns, or a task already faulted or cancelled when it does,
    /// refuses the transfer: the client releases the handle, and the host's request fails. A task
    /// that faults or is cancelled later fails the transfer instead: the client logs it and releases
    /// the handle in the handler's place, which abandons a transfer that has not ended, and the
    /// receiver fails with <see cref="TransferFailedException"/>. A transfer that has ended keeps its
    /// outcome. With no handler attached the client refuses such transfers, and the host's request
    /// fails.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="handler"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException">A handler is already registered.</exception>
    void OnSendRequested(Func<Send<T>, Task> handler);

    /// <summary>
    /// Gets a list of receivers that this client is authorized to send data to.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the request.</param>
    /// <returns>List of authorized receiver clients.</returns>
    /// <exception cref="HubRequestFailedException">The hub refused the request.</exception>
    Task<List<ClientInfo>> GetReceiversAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the receivers of <see cref="GetReceiversAsync(CancellationToken)"/> that belong to
    /// <paramref name="group"/>: the hub's answer, kept to those whose <see cref="ClientInfo.Groups"/>
    /// contain it.
    /// </summary>
    /// <remarks>
    /// A receiver whose stream is down is kept, since a registration outlives its stream: the caller
    /// still picks one whose <see cref="ClientInfo.DisconnectedAtUtc"/> is <c>null</c>.
    /// </remarks>
    /// <param name="group">The group to keep, compared as given: case matters.</param>
    /// <param name="cancellationToken">Token to cancel the request.</param>
    /// <returns>The authorized receivers in the group.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="group"/> is null, empty or whitespace. Raised by the call, before any request.
    /// </exception>
    /// <exception cref="HubRequestFailedException">The hub refused the request.</exception>
    Task<List<ClientInfo>> GetReceiversAsync(string group, CancellationToken cancellationToken = default);
}

