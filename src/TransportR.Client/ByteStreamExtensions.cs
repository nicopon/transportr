namespace TransportR.Client;

/// <summary>
/// The byte-stream mode of a <c>byte[]</c> client: the transfer carries one sequence of bytes, with no
/// serializer between the caller and the wire.
/// </summary>
public static class ByteStreamExtensions
{
    /// <summary>
    /// Builds a client that transfers a stream of bytes instead of serialized items.
    /// </summary>
    /// <remarks>
    /// <para>
    /// No boundary survives: neither the one between two calls of the sender nor the one between two
    /// batches. Both ends of a transfer must be in this mode, and a receiver that is not refuses the
    /// transfer, which ends it on both sides rather than delivering bytes it cannot read.
    /// </para>
    /// <para>
    /// Nothing is compressed. The mode cannot be combined with <c>WithSerializer</c>,
    /// <c>WithBatchSize</c>, or a receive capacity bounded by items; <c>Build</c> refuses each, in any
    /// order of the calls.
    /// </para>
    /// </remarks>
    /// <param name="builder">The builder of a client carrying <c>byte[]</c>.</param>
    /// <param name="batchBytes">
    /// The size, in bytes, at which a batch is cut. It sets what an acknowledgement, a send window slot
    /// and a resend cover, so the sender holds at most this much times <c>WithMaxInFlightBatches</c>. There
    /// is no default: the right figure depends on the use.
    /// </param>
    public static TransportRClientBuilder<byte[]> WithByteStream(this TransportRClientBuilder<byte[]> builder, int batchBytes)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(batchBytes, 0, nameof(batchBytes));
        builder.ByteStreamBatchBytes = batchBytes;
        return builder;
    }
}
