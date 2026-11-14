namespace TransportR.Hub;

/// <summary>
/// Represents the persistent session state of a transfer for High Availability.
/// This record is stored in the shared state store (e.g. Redis) to allow
/// any Hub instance to validate the existence and participants of a transfer.
/// </summary>
/// <param name="TransferId">The unique transfer identifier.</param>
/// <param name="SenderId">The client ID of the sender.</param>
/// <param name="ReceiverId">The client ID of the receiver.</param>
/// <param name="CreatedAtUtc">When the transfer was initiated.</param>
/// <param name="ExpiresAtUtc">When the session expires (for cleanup).</param>
public record TransferSession(
    string TransferId,
    Guid SenderId,
    Guid ReceiverId,
    DateTime CreatedAtUtc,
    DateTime ExpiresAtUtc
)
{
    /// <summary>
    /// Whether the sender opened the transfer through its own request, rather than the host through
    /// <see cref="ITransferInitiator"/>: only such a session counts against
    /// <see cref="AccessOptions.MaxOpenTransfersPerClient"/>.
    /// </summary>
    public bool OpenedBySender { get; init; }
}
