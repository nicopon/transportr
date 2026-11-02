namespace TransportR.Interfaces;

/// <summary>
/// Receives the bytes the hub forwards, as they cross it.
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
    /// Called as the bytes of a data stream cross the hub, after each read, inside the copy and from
    /// every transfer at once: an implementation stays short and thread-safe. The acknowledgements
    /// flowing back are not counted. No transfer id comes with the bytes, and the hub tells no one
    /// when a transfer starts or ends.
    /// </summary>
    /// <param name="bytes">Bytes just forwarded, as they travel on the wire.</param>
    void OnBytesTransferred(long bytes);
}
