namespace TransportR.Client;

/// <summary>
/// Thrown when a transfer ends without delivering its data.
/// </summary>
/// <remarks>
/// Raised by <see cref="Send{T}.CompleteAsync"/> and by <see cref="Receive{T}.ReceiveAsync"/> or
/// <see cref="Receive{T}.WaitForCompletionAsync"/> on the receiving side.
/// Catching it is the only way to tell a failed transfer from a slow one: both leave the handle
/// looking the same, and a completion that returns normally is the sole success signal either side
/// gets. The handle is dead once this is thrown — dispose it, do not retry on it.
/// </remarks>
public class TransferFailedException : Exception
{
    /// <summary>
    /// The transfer that failed.
    /// </summary>
    public string TransferId { get; }

    /// <summary>
    /// Why the transfer failed, as this end saw it.
    /// </summary>
    public TransferFailureCategory Category { get; }

    public TransferFailedException(string transferId, TransferFailureCategory category, string message) : base(message)
    {
        TransferId = transferId;
        Category = category;
    }
}
