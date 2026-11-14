using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Client;
using TransportR.Client.Sessions;

namespace TransportR.Tests;

/// <summary>
/// What <see cref="ByteStreamClientBuilder"/> accepts: the batch size is the constructor's, the other
/// settings are those of the typed builder's, and what only an item client could be given is not there.
/// </summary>
public class ByteStreamClientBuilderTests
{
    private static ByteStreamClientBuilder Builder(int batchBytes = 4096) =>
        new ByteStreamClientBuilder(batchBytes).WithUrl("http://localhost:1").WithMaxInFlightBatches(4);

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void TheConstructor_RefusesABudgetOfZeroOrLess(int batchBytes)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ByteStreamClientBuilder(batchBytes));
    }

    [Theory]
    [InlineData(int.MaxValue - 3)]
    [InlineData(int.MaxValue)]
    public void TheConstructor_RefusesABudgetThatCannotHoldItsLengthPrefix(int batchBytes)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ByteStreamClientBuilder(batchBytes));
    }

    [Fact]
    public void TheConstructor_AcceptsTheLargestBudgetAnIntCanHold()
    {
        _ = new ByteStreamClientBuilder(int.MaxValue - 4);
    }

    [Fact]
    public async Task Build_NeedsOnlyTheUrl_AndCarriesTheSettingsTheBuilderWasGiven()
    {
        await using var client = new ByteStreamClientBuilder(4096)
            .WithUrl("http://localhost:1")
            .WithBatchTimeout(TimeSpan.FromMilliseconds(250))
            .WithMaxInFlightBatches(4)
            .WithReceiveCapacity(maxBytes: 1 << 20)
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithTokenRefreshThreshold(TimeSpan.FromMinutes(1))
            .WithLoggerFactory(NullLoggerFactory.Instance)
            .Build();

        var options = ((ISessionConfig)client).Options;
        Assert.Equal(250, options.BatchFlushIntervalMs);
        Assert.Equal(4, options.MaxInFlightBatches);
        Assert.Equal(BufferCapacity.Bytes(1 << 20), options.ReceiveBufferCapacity);
        Assert.Equal(WriteTransportPolicy.SegmentsOnly, options.WriteTransportPolicy);
        Assert.Equal(TimeSpan.FromMinutes(1), options.TokenRefreshThreshold);
    }

    [Fact]
    public void Build_WithoutAUrl_Refuses()
    {
        var refused = Assert.Throws<InvalidOperationException>(() => new ByteStreamClientBuilder(1024).Build());

        Assert.Equal("Hub URL must be configured using WithUrl().", refused.Message);
    }

    public static TheoryData<TimeSpan> RefusedBatchTimeouts => new()
    {
        TimeSpan.Zero,
        TimeSpan.FromMilliseconds(-5),
        TimeSpan.FromTicks(TimeSpan.TicksPerMillisecond / 2),
        TimeSpan.FromTicks(TimeSpan.TicksPerMillisecond + 1),
        TimeSpan.FromMilliseconds((double)int.MaxValue + 1),
    };

    [Theory]
    [MemberData(nameof(RefusedBatchTimeouts))]
    public void WithBatchTimeout_RefusesAtTheCall_WhatIsNotAWholePositiveNumberOfMilliseconds(TimeSpan timeout)
    {
        var builder = Builder();

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithBatchTimeout(timeout));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(int.MaxValue)]
    public void WithBatchTimeout_AcceptsTheShortestAndTheLongestInterval(int milliseconds)
    {
        var builder = Builder().WithBatchTimeout(TimeSpan.FromMilliseconds(milliseconds));

        Assert.NotNull(builder);
    }

    [Fact]
    public void TheSharedSettings_KeepTheChecksOfTheTypedBuilder()
    {
        var builder = Builder();

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithMaxInFlightBatches(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithTokenRefreshThreshold(TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithWriteTransport((WriteTransportPolicy)99));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithReceiveCapacity(maxBytes: 0));
    }

    [Fact]
    public async Task WithClientCredentials_GivesTheBuilderATokenProvider_AndReturnsTheBuilder()
    {
        var builder = Builder();

        var returned = builder.WithClientCredentials("http://idp.invalid/token", "id", "secret", scope: null);

        Assert.Same(builder, returned);
        await using var client = returned.Build();
        Assert.True(client.Channel.HasTokenProvider);
    }
}
