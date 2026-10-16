using TransportR.Protocol;

namespace TransportR.Client.SignalR.Batching;

/// <summary>
/// A batch body arrived whole and the receiver's serializer could not decode it.
/// </summary>
/// <remarks>
/// A protocol violation, so fatal rather than retried: a resend would decode no better. Its own type
/// because, unlike a header this version cannot read, it leaves the sender a peer to tell.
/// </remarks>
internal sealed class UndecodableBatchException(string message, Exception innerException)
    : WireProtocolViolationException(message, innerException);
