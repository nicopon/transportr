using TransportR.Protocol;

namespace TransportR.Client.Internal;

/// <summary>
/// The refusal of a batch that expands past <c>WithMaxBatchBytes</c>, shared by the serialization
/// packages that decompress, so a caller reads the same message whichever one carries the transfer.
/// </summary>
internal static class DecompressionBound
{
    public static WireProtocolViolationException Exceeded(long limit)
        => new($"A batch expands to more than the {limit} bytes WithMaxBatchBytes allows once decompressed.");

    /// <summary>
    /// The sender's side of the same bound: a batch that would be refused once decompressed is not
    /// sent. It is the sender's own batch, not a peer's, so it is no protocol violation.
    /// </summary>
    public static InvalidOperationException ExceededBeforeCompression(long limit)
        => new($"A batch is more than the {limit} bytes WithMaxBatchBytes allows before compression.");
}
