namespace TransportR.Hub;

/// <summary>
/// Receives the bytes the hub forwards, as they cross it, and the transport that carries each write stream
/// or a client's write-transport probe.
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
    /// <param name="bytes">
    /// What one read returned, as it travels on the wire. It is counted before the hub writes it on, so
    /// a write that then fails is counted all the same; the end of each stream reports 0.
    /// </param>
    void OnBytesTransferred(long bytes);

    /// <summary>
    /// Called once per opening of a write stream, where the hub takes it on: a WebSocket once it has
    /// carried its first byte, a succession of segments when its first one is taken. Every reconnection
    /// calls again. It runs on the request that opens the stream, so an implementation stays short; one
    /// that throws is logged and the stream goes on. The hub does not say why a client chose the transport.
    /// </summary>
    void OnWriteStreamOpened(WriteStreamOpened opened) { }

    /// <summary>
    /// Called once per write-transport probe the hub admits, with what carried it: a client that enables
    /// the probe makes one after each registration of its stream of events, so a reconnection calls again.
    /// It runs on the probe's request, so an implementation stays short; one that throws is logged and the
    /// probe is answered all the same. It informs and decides nothing: each transfer still finds its own
    /// transport when its stream opens, and the probe does not see a front end that holds bytes back after it.
    /// </summary>
    void OnWriteTransportProbed(WriteTransportProbed probed) { }
}
