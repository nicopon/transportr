namespace TransportR.Client.Protocol;

/// <summary>
/// Reason codes carried in an <see cref="ChunkType.Abort"/> chunk's
/// <see cref="ChunkHeader.BatchId"/> field.
/// </summary>
internal enum AbortReason
{
    /// <summary>The sending end released its transfer handle before the transfer completed.</summary>
    ReleasedBeforeCompletion = 1,

    /// <summary>
    /// The sending end gave up after nothing was acknowledged for its stall timeout
    /// (<c>CompleteStallTimeout</c>), whether it was waiting on the send window or on Complete.
    /// </summary>
    AcknowledgementTimeout = 2,

    /// <summary>The sending end could not serialize a batch, so the batches after it never come.</summary>
    SerializationFailed = 3,

    /// <summary>
    /// The receiving end could not decode a batch: the two ends do not use the same serializer, the
    /// same settings, or the same item type.
    /// </summary>
    UndecodableBatch = 4,
}

internal static class AbortReasonExtensions
{
    public static string Describe(this AbortReason reason) => reason switch
    {
        AbortReason.ReleasedBeforeCompletion =>
            "The peer released its transfer handle before the transfer completed.",
        AbortReason.AcknowledgementTimeout =>
            "The peer gave up waiting for acknowledgements before the transfer completed.",
        AbortReason.SerializationFailed =>
            "The peer could not serialize a batch, and the transfer cannot complete.",
        AbortReason.UndecodableBatch =>
            "The peer could not decode a batch: both ends must use the same serializer and settings.",
        _ => $"The peer aborted the transfer (reason {(int)reason})."
    };
}
