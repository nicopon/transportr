using System.Buffers;
using TransportR.Client.Protocol;

namespace TransportR.Tests.TestHelpers;

/// <summary>Reads back the chunks a client wrote to a stub hub, with no bound on a payload's length.</summary>
internal static class WireChunks
{
    /// <summary>
    /// The next chunk, or <c>null</c> at the end of the stream. The payload's owner, <c>null</c> for a
    /// chunk without one, is the caller's to dispose.
    /// </summary>
    public static ValueTask<(ChunkHeader Header, IMemoryOwner<byte>? Payload)?> ReadAsync(Stream stream) =>
        ChunkIO.ReadOwnedChunkAsync(stream, int.MaxValue, "a chunk carries", CancellationToken.None);
}
