namespace TransportR.Abstractions;

/// <summary>
/// Thrown when a transfer ends without delivering its data.
/// </summary>
/// <remarks>
/// Raised by <see cref="Send{T}.CompleteAsync"/> and <see cref="Receive{T}.WaitForCompletionAsync"/>.
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

    public TransferFailedException(string transferId, string message) : base(message)
    {
        TransferId = transferId;
    }
}
