namespace TransportR.Interfaces;

/// <summary>
/// Interface for monitoring data transfer progress at the hub level.
/// </summary>
/// <remarks>
/// <para>
/// This interface is designed for hub-side monitoring only, respecting the 
/// "dumb-pipe" architecture principle where the hub forwards bytes without 
/// deserializing the content.
/// </para>
/// <para>
/// Client-side progress is read from the <c>TransferInfo</c> records the client exposes. The
/// library emits no metrics of its own — see <c>samples/DataHub</c> for a hub that builds its own
/// meter on top of this interface.
/// </para>
/// </remarks>
public interface IHubProgressMonitor
{
    /// <summary>
    /// Called when a new transfer is initialized.
    /// </summary>
    /// <param name="transferId">Unique identifier for the transfer.</param>
    void InitializeTransfer(string transferId);

    /// <summary>
    /// Called when bytes are transferred through the hub.
    /// </summary>
    /// <param name="bytes">Number of compressed bytes transferred.</param>
    void OnBytesTransferred(long bytes);

    /// <summary>
    /// Called when a transfer is completed.
    /// </summary>
    /// <param name="transferId">Unique identifier for the transfer.</param>
    void CompleteTransfer(string transferId);
}
