namespace TransportR.Abstractions;

/// <summary>
/// Base class for transfer handles, providing the transfer identifier.
/// </summary>
public abstract class Transfer
{
    /// <summary>
    /// Gets the unique identifier for this transfer.
    /// </summary>
    public string TransferId { get; }

    /// <summary>
    /// Creates a new transfer with the specified ID.
    /// </summary>
    /// <param name="transferId">The unique transfer identifier.</param>
    protected Transfer(string transferId)
    {
        TransferId = transferId;
    }
}
