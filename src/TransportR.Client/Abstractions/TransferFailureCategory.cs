namespace TransportR.Client;

/// <summary>
/// Why a transfer ended without delivering its data: the <see cref="TransferFailedException.Category"/>
/// of the failure, as this end saw it.
/// </summary>
public enum TransferFailureCategory
{
    /// <summary>
    /// The hub ended the transfer, or refuses its data stream as one it does not hold: the host
    /// terminated it, a reaper found it silent or stalled, a right was withdrawn, or the hub lost it.
    /// The hub log says which; this says nothing of anyone's rights.
    /// </summary>
    EndedByHub,

    /// <summary>
    /// The other end gave up and said so: it released its handle, ran out of its acknowledgement
    /// timeout, could not serialize a batch or could not decode one. The message says which.
    /// </summary>
    AbandonedByPeer,

    /// <summary>
    /// On a sender, the receiver acknowledged nothing for the acknowledgement timeout
    /// (<see cref="TransportRClientBuilder{T}.WithAcknowledgementTimeout"/>), while the send window was
    /// full or during <c>CompleteAsync</c>. On a receiver that accepted the sender's <c>Complete</c>, the
    /// transfer did not finish closing within that timeout.
    /// </summary>
    AcknowledgementTimedOut,

    /// <summary>This end's handle was released before the transfer reached an outcome.</summary>
    HandleReleased,

    /// <summary>
    /// This end could not turn a batch into bytes, or the bytes are over <c>WithMaxBatchBytes</c>; or, on a
    /// receiver, could not turn the bytes back into a batch, which says that the two ends do not use the
    /// same serializer, settings, item type or bound. The peer learns it as <see cref="AbandonedByPeer"/>.
    /// </summary>
    UndecodableOrUnserializableBatch,

    /// <summary>
    /// What arrived breaks the wire protocol: a chunk of an unknown type or version, a length past the
    /// bound, a batch out of order, or an answer of the hub's that contradicts what this end wrote.
    /// Opening the stream again would meet it again.
    /// </summary>
    ProtocolViolation,

    /// <summary>
    /// The client requires a WebSocket for the stream it writes (<c>WriteTransportPolicy.RequireWebSocket</c>),
    /// and the path to the hub does not carry one.
    /// </summary>
    Other,

    /// <summary>
    /// End-to-end encryption refused the transfer at this end: the peer does not encrypt, or encrypts
    /// where this end does not; a confirmation, receipt or sealed batch did not verify, which a stream
    /// altered on its way and two different pre-shared keys both cause; or the pre-shared key provider
    /// gave no 32-byte key for the peer. The message names no key. The peer learns it as
    /// <see cref="AbandonedByPeer"/>.
    /// </summary>
    EndToEndEncryptionFailed,
}
