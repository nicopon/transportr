using TransportR.Client.Protocol;
using TransportR.Tests.TestHelpers;

namespace TransportR.Tests;

/// <summary>
/// What a transfer in clear puts on the wire, byte for byte, frozen: the stream handshake, two batches,
/// Complete and its acknowledgement, as each end writes them. A change to the wire of a transfer in
/// clear shows here first.
/// </summary>
public class ClearWireReferenceTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    private const string TransferId = "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0";

    // Version 2, type, payload length, BatchId, Count (little-endian), then the TransferId as
    // Guid.ToByteArray() writes it.
    private const string Id = "3C2D1E0F5A4B78698796A5B4C3D2E1F0";

    private static readonly string[] SenderWrites =
    [
        "0205" + "00000000" + "00000000" + "00000000" + Id,               // Ping
        "0201" + "05000000" + "00000000" + "01000000" + Id + "0100000000", // Batch 0, one item
        "0201" + "05000000" + "01000000" + "01000000" + Id + "0100000000", // Batch 1, one item
        "0203" + "00000000" + "01000000" + "00000000" + Id,               // Complete, last BatchId 1
    ];

    private static readonly string[] ReceiverWrites =
    [
        "0206" + "00000000" + "00000000" + "00000000" + Id, // Pong, next expected 0
        "0202" + "00000000" + "00000000" + "00000000" + Id, // BatchAck 0
        "0202" + "00000000" + "01000000" + "00000000" + Id, // BatchAck 1
        "0204" + "00000000" + "00000000" + "00000000" + Id, // CompleteAck
    ];

    [Fact]
    public async Task ATransferInClear_WritesTheReferenceBytes_AtBothEnds()
    {
        var hub = new RelayHub();
        await using var sender = new TestSession(new StubSessionContext(new HttpClient(hub)), TransferId, batchSize: 1, "http://test/data", affinity: null);
        await using var receiver = new TestSession(new StubSessionContext(new HttpClient(hub)), TransferId, batchSize: 1, "http://test/data", affinity: null);

        await receiver.StartReceiverAsync();
        await sender.StartSenderAsync();
        var received = Task.Run(async () =>
        {
            var count = 0;
            await foreach (var _ in receiver.ReceiveAsync(CancellationToken.None)) count++;
            return count;
        });

        await sender.SendAsync("a", CancellationToken.None);
        await sender.SendAsync("b", CancellationToken.None);
        await sender.CompleteAsync().WaitAsync(Bound);
        Assert.Equal(2, await received.WaitAsync(Bound));

        // Complete goes again every 5 s until it is acknowledged: a slow run may write it twice.
        Assert.Equal(SenderWrites, Hex(hub.SenderWrote).Distinct());
        Assert.Equal(ReceiverWrites, Hex(hub.ReceiverWrote));
    }

    private static IEnumerable<string> Hex(IEnumerable<WireChunk> chunks) => chunks.Select(chunk => Convert.ToHexString(chunk.Bytes));
}
