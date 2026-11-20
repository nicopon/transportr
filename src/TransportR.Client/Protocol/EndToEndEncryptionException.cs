namespace TransportR.Client.Protocol;

/// <summary>
/// End-to-end encryption refused what arrived, or this end could not take part in it.
/// </summary>
/// <remarks>
/// A protocol violation, so fatal rather than retried: a share, a tag or a receipt that does not hold
/// holds no better on a reopened stream. Its own type, like <c>UndecodableBatchException</c>, because
/// it leaves the peer to be told, with <see cref="Reason"/>. Its message names no key.
/// </remarks>
internal sealed class EndToEndEncryptionException(AbortReason reason, string message) : WireProtocolViolationException(message)
{
    /// <summary><see cref="AbortReason.EncryptionModeMismatch"/> or <see cref="AbortReason.VerificationFailed"/>.</summary>
    public AbortReason Reason { get; } = reason;
}
