using TransportR.Client;
using TransportR.Client.Sessions;
using TransportR.Client.Configuration;
using TransportR.Serialization.MessagePack.Services;

namespace TransportR.Tests;

/// <summary>
/// Which configurations of the byte-stream mode <c>Build</c> accepts, and that the order of the calls
/// does not change the verdict.
/// </summary>
public class ByteStreamBuilderTests
{
    private static TransportRClientBuilder<byte[]> Builder() =>
        new TransportRClientBuilder<byte[]>().WithUrl("http://localhost:1").WithMaxInFlightBatches(4);

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void WithByteStream_RefusesABudgetOfZeroOrLess(int batchBytes)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => Builder().WithByteStream(batchBytes));
    }

    [Fact]
    public async Task Build_InByteStreamMode_NeedsNoSerializer_AndCarriesTheBudget()
    {
        await using var client = Builder().WithByteStream(4096).Build();

        Assert.Equal(4096, ((ISessionConfig)client).Options.ByteStreamBatchBytes);
        Assert.Null(((ISessionConfig<byte[]>)client).Serializer);
    }

    [Fact]
    public async Task Build_WithASerializer_StaysInTheItemMode()
    {
        await using var client = Builder().WithSerializer(new MessagePackBatchSerializer<byte[]>()).Build();

        Assert.Null(((ISessionConfig)client).Options.ByteStreamBatchBytes);
        Assert.NotNull(((ISessionConfig<byte[]>)client).Serializer);
    }

    [Fact]
    public void Build_WithNeitherASerializerNorTheByteStream_KeepsItsRefusal()
    {
        var refused = Assert.Throws<InvalidOperationException>(() => Builder().Build());

        Assert.Equal("Serializer must be configured using WithSerializer().", refused.Message);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Build_RefusesASerializerWithTheByteStream_InEitherOrder(bool byteStreamFirst)
    {
        var builder = Builder();
        if (byteStreamFirst)
            builder.WithByteStream(1024).WithSerializer(new MessagePackBatchSerializer<byte[]>());
        else
            builder.WithSerializer(new MessagePackBatchSerializer<byte[]>()).WithByteStream(1024);

        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Build_RefusesABatchSizeWithTheByteStream_InEitherOrder(bool byteStreamFirst)
    {
        var builder = Builder();
        if (byteStreamFirst)
            builder.WithByteStream(1024).WithBatchSize(1000);
        else
            builder.WithBatchSize(1000).WithByteStream(1024);

        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(false, true)]
    public void Build_RefusesAnItemBoundOnTheReceiveCapacity_WithTheByteStream_InEitherOrder(bool byteStreamFirst, bool alsoBytes)
    {
        var capacity = alsoBytes ? BufferCapacity.ItemsAndBytes(10, 1 << 20) : BufferCapacity.Items(10);
        var builder = Builder();
        if (byteStreamFirst)
            builder.WithByteStream(1024).WithReceiveCapacity(capacity);
        else
            builder.WithReceiveCapacity(capacity).WithByteStream(1024);

        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Build_AcceptsAByteBoundOnTheReceiveCapacity_WithTheByteStream_InEitherOrder(bool byteStreamFirst)
    {
        var capacity = BufferCapacity.Bytes(1 << 20);
        var builder = Builder();
        if (byteStreamFirst)
            builder.WithByteStream(1024).WithReceiveCapacity(capacity);
        else
            builder.WithReceiveCapacity(capacity).WithByteStream(1024);

        await using var client = builder.Build();

        Assert.Equal(1024, ((ISessionConfig)client).Options.ByteStreamBatchBytes);
    }

    [Fact]
    public void Build_RefusalsStayGeneric()
    {
        var refusals = new[]
        {
            Assert.Throws<InvalidOperationException>(() => Builder().WithByteStream(1024)
                .WithSerializer(new MessagePackBatchSerializer<byte[]>()).Build()),
            Assert.Throws<InvalidOperationException>(() => Builder().WithByteStream(1024).WithBatchSize(10).Build()),
            Assert.Throws<InvalidOperationException>(() => Builder().WithByteStream(1024)
                .WithReceiveCapacity(maxItems: 10).Build()),
        };

        foreach (var refused in refusals)
        {
            Assert.Null(refused.InnerException);
            Assert.DoesNotContain("localhost", refused.Message);
            Assert.DoesNotContain("ByteStreamBatchBytes", refused.Message);
            Assert.DoesNotContain("group", refused.Message, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public async Task InitTransferAsync_InByteStreamMode_RefusesABatchSize_BeforeAnyRequest()
    {
        // The hub does not exist: a request would fail with something else, and late.
        await using var client = Builder().WithByteStream(1024).Build();

        await Assert.ThrowsAsync<ArgumentException>(
            () => client.InitTransferAsync(Guid.NewGuid(), batchSize: 10).WaitAsync(TimeSpan.FromSeconds(5)));
    }
}
