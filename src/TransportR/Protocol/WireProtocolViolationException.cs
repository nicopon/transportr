namespace TransportR.Protocol;

/// <summary>
/// Thrown when a peer sends a wire message this protocol version cannot interpret: an unsupported
/// <see cref="ChunkHeader.Version"/>, a <see cref="ChunkType"/> outside the known vocabulary, or a
/// batch body the receiver's serializer cannot decode.
/// </summary>
/// <remarks>
/// Distinct from a transport-level failure on purpose: a dropped connection is retryable, a peer
/// speaking a protocol this version does not understand is not, and reconnecting only repeats the
/// same violation.
/// </remarks>
public class WireProtocolViolationException : Exception
{
    public WireProtocolViolationException(string message) : base(message)
    {
    }

    public WireProtocolViolationException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
