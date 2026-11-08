using TransportR.Client;

namespace TransportR.Tests;

/// <summary>
/// What <c>Build</c> refuses.
/// </summary>
public class ByteStreamBuilderTests
{
    private static TransportRClientBuilder<byte[]> Builder() =>
        new TransportRClientBuilder<byte[]>().WithUrl("http://localhost:1").WithMaxInFlightBatches(4);

    [Fact]
    public void Build_WithNeitherASerializerNorTheByteStream_KeepsItsRefusal()
    {
        var refused = Assert.Throws<InvalidOperationException>(() => Builder().Build());

        Assert.Equal("Serializer must be configured using WithSerializer().", refused.Message);
    }
}
