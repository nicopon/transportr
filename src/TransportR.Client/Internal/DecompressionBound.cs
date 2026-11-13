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
}
