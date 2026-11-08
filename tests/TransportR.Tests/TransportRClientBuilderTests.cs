using TransportR.Client;

namespace TransportR.Tests;

/// <summary>
/// What <c>Build</c> of the builder of items refuses.
/// </summary>
public class TransportRClientBuilderTests
{
    private static TransportRClientBuilder<byte[]> Builder() =>
        new TransportRClientBuilder<byte[]>().WithUrl("http://localhost:1").WithMaxInFlightBatches(4);

    [Fact]
    public void Build_WithoutASerializer_KeepsItsRefusal()
    {
        var refused = Assert.Throws<InvalidOperationException>(() => Builder().Build());

        Assert.Equal("Serializer must be configured using WithSerializer().", refused.Message);
    }
}
