using TransportR.Protocol;

namespace TransportR.Client.Batching;

/// <summary>
/// A batch body arrived whole and the receiver's serializer could not decode it.
/// </summary>
/// <remarks>
/// A protocol violation, so fatal rather than retried: a resend would decode no better. Its own type
/// because, unlike a header this version cannot read, it leaves the sender a peer to tell.
/// </remarks>
internal sealed class UndecodableBatchException : WireProtocolViolationException
{
    public UndecodableBatchException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    /// For a batch that is not the kind this transfer carries, where there is no decoder to have failed.
    /// </summary>
    public UndecodableBatchException(string message)
        : base(message)
    {
    }
}
