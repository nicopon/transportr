namespace TransportR.Protocol;

/// <summary>
/// Reason codes carried in an <see cref="ChunkType.Abort"/> chunk's
/// <see cref="ChunkHeader.BatchId"/> field.
/// </summary>
public enum AbortReason
{
    /// <summary>The sending end released its transfer handle before the transfer completed.</summary>
    ReleasedBeforeCompletion = 1,
}

public static class AbortReasonExtensions
{
    public static string Describe(this AbortReason reason) => reason switch
    {
        AbortReason.ReleasedBeforeCompletion =>
            "The peer released its transfer handle before the transfer completed.",
        _ => $"The peer aborted the transfer (reason {(int)reason})."
    };
}
