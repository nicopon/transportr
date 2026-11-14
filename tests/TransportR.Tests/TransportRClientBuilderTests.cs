using TransportR.Client;
using TransportR.Client.Sessions;
using TransportR.Tests.TestHelpers;

namespace TransportR.Tests;

/// <summary>
/// What <c>Build</c> of the builder of items refuses.
/// </summary>
public class TransportRClientBuilderTests
{
    private static TransportRClientBuilder<byte[]> Builder() =>
        new TransportRClientBuilder<byte[]>().WithUrl("http://localhost:1").WithMaxInFlightBatches(4);

    [Fact]
    public async Task WithBatchTimeout_CarriesTheIntervalToTheClient()
    {
        await using var client = new TransportRClientBuilder<string>()
            .WithUrl("http://localhost:1")
            .WithSerializer(new StubBatchSerializer())
            .WithMaxInFlightBatches(4)
            .WithBatchTimeout(TimeSpan.FromMilliseconds(250))
            .Build();

        Assert.Equal(250, client.Connection.Options.BatchFlushIntervalMs);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void WithBatchTimeout_RefusesAtTheCall_ADurationThatIsNotPositive(int milliseconds)
    {
        var builder = Builder();

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithBatchTimeout(TimeSpan.FromMilliseconds(milliseconds)));
    }

    [Fact]
    public void Build_WithoutASerializer_KeepsItsRefusal()
    {
        var refused = Assert.Throws<InvalidOperationException>(() => Builder().Build());

        Assert.Equal("Serializer must be configured using WithSerializer().", refused.Message);
    }
}
