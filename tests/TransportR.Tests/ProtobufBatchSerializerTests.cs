using TransportR.Client.SignalR.Batching;
using TransportR.Serialization.Protobuf.Services;

namespace TransportR.Tests;

/// <summary>
/// The Protobuf serializer, through the protocol's own read path (see <see cref="ArrowBatchSerializerTests"/>).
/// </summary>
public class ProtobufBatchSerializerTests
{
    private static readonly List<string> Items = ["alpha", "beta", "gamma"];

    private static async Task<byte[]> WriteAsync(ProtobufBatchSerializer<string> serializer)
    {
        var buffer = new MemoryStream();
        await serializer.SerializeBatchAsync(buffer, [.. Items]);
        return buffer.ToArray();
    }

    [Fact]
    public async Task A_Batch_Survives_The_Round_Trip()
    {
        var serializer = new ProtobufBatchSerializer<string>();

        var back = await ReceiverState<string>.ReadBatchBodyFromMemoryAsync(serializer, await WriteAsync(serializer));

        Assert.Equal(Items, back);
    }

    [Fact]
    public async Task A_Truncated_Body_Is_A_Protocol_Violation_Not_An_Empty_Batch()
    {
        // The receive path drops a null batch as empty: returned instead of thrown, every item of
        // the transfer would be acknowledged and lost.
        var serializer = new ProtobufBatchSerializer<string>();
        var bytes = await WriteAsync(serializer);

        await Assert.ThrowsAsync<UndecodableBatchException>(
            () => ReceiverState<string>.ReadBatchBodyFromMemoryAsync(serializer, bytes.AsMemory(0, bytes.Length / 2)));
    }
}
