using System.Text;
using TransportR.Client.SignalR.Batching;
using TransportR.Serialization.Json.Services;

namespace TransportR.Tests;

/// <summary>
/// The JSON serializer, through the protocol's own read path (see <see cref="ArrowBatchSerializerTests"/>).
/// </summary>
public class JsonBatchSerializerTests
{
    private static readonly List<string> Items = ["alpha", "beta", "gamma"];

    private static async Task<byte[]> WriteAsync(JsonBatchSerializer<string> serializer)
    {
        var buffer = new MemoryStream();
        await serializer.SerializeBatchAsync(buffer, [.. Items]);
        return buffer.ToArray();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_Batch_Survives_The_Round_Trip(bool compress)
    {
        var serializer = new JsonBatchSerializer<string>(compress: compress);

        var back = await ReceiverState<string>.ReadBatchBodyFromMemoryAsync(serializer, await WriteAsync(serializer));

        Assert.Equal(Items, back);
    }

    [Fact]
    public async Task Uncompressed_The_Body_Is_Plain_Json()
    {
        var bytes = await WriteAsync(new JsonBatchSerializer<string>(compress: false));

        Assert.Equal("[\"alpha\",\"beta\",\"gamma\"]", Encoding.UTF8.GetString(bytes));
    }
}
