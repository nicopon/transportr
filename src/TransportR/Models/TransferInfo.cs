namespace TransportR.Models;

/// <summary>
/// Direction of a data transfer from the client's perspective.
/// </summary>
public enum TransferDirection
{
    /// <summary>Client is sending data.</summary>
    Sending,
    
    /// <summary>Client is receiving data.</summary>
    Receiving
}

/// <summary>
/// Status of a data transfer.
/// </summary>
public enum TransferStatus
{
    /// <summary>Transfer is actively processing data.</summary>
    Active,
    
    /// <summary>A receiver holds every batch and the hub has yet to read its acknowledgement. A sender never reports it.</summary>
    Completing,
    
    /// <summary>Transfer completed successfully.</summary>
    Completed,
    
    /// <summary>Transfer failed or was cancelled.</summary>
    Failed
}

/// <summary>
/// Information about an active or recent data transfer.
/// </summary>
/// <param name="TransferId">Unique identifier for the transfer.</param>
/// <param name="DataUrl">URL of the data plane endpoint used for this transfer.</param>
/// <param name="Direction">Whether this client is sending or receiving.</param>
/// <param name="Status">Current status of the transfer.</param>
/// <param name="ItemsProcessed">Number of items sent or received.</param>
/// <param name="BytesTransferred">Total bytes transferred (compressed).</param>
/// <param name="StartedAt">When the transfer started.</param>
/// <param name="Duration">How long the transfer has been running (or ran).</param>
public record TransferInfo(
    string TransferId,
    string DataUrl,
    TransferDirection Direction,
    TransferStatus Status,
    long ItemsProcessed,
    long BytesTransferred,
    DateTime StartedAt,
    TimeSpan? Duration,
    long AcksReceived = 0,
    long AcksSent = 0
);
