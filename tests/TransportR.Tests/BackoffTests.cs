using TransportR.Client.Internal;

namespace TransportR.Tests;

public class BackoffTests
{
    [Fact]
    public void Ceiling_DoublesFromTheFirstPauseAndStopsAtTheMaximum()
    {
        var first = TimeSpan.FromSeconds(2);
        var max = TimeSpan.FromSeconds(15);

        var ceilings = Enumerable.Range(1, 6).Select(rank => Backoff.Ceiling(first, max, rank)).ToArray();

        Assert.Equal(
            [TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(4), TimeSpan.FromSeconds(8), max, max, max],
            ceilings);
    }

    [Fact]
    public void Ceiling_AgreesWithTheRenewalRetry()
    {
        var first = TimeSpan.FromSeconds(1);
        var max = TimeSpan.FromSeconds(30);

        var seconds = Enumerable.Range(1, 8).Select(rank => Backoff.Ceiling(first, max, rank).TotalSeconds).ToArray();

        Assert.Equal([1, 2, 4, 8, 16, 30, 30, 30], seconds);
    }

    [Theory]
    [InlineData(7)]
    [InlineData(64)]
    [InlineData(1_000)]
    [InlineData(int.MaxValue)]
    public void Ceiling_NeverExceedsTheMaximum_HoweverLongTheEpisode(int rank)
    {
        var max = TimeSpan.FromSeconds(15);

        Assert.Equal(max, Backoff.Ceiling(TimeSpan.FromSeconds(2), max, rank));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Ceiling_RefusesARankBeforeTheFirstPause(int rank)
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => Backoff.Ceiling(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(15), rank));
    }

    [Theory]
    [InlineData(0.0, 1.0)]
    [InlineData(0.5, 1.5)]
    [InlineData(0.999999, 1.999999)]
    public void Jittered_RunsFromHalfToWholeOfTheCeiling(double draw, double expectedSeconds)
    {
        var jittered = Backoff.Jittered(TimeSpan.FromSeconds(2), draw);

        Assert.Equal(expectedSeconds, jittered.TotalSeconds, precision: 3);
    }

    [Fact]
    public void Jittered_DrawnAtRandom_StaysWithinHalfAndWholeAndVaries()
    {
        var ceiling = TimeSpan.FromSeconds(15);

        var pauses = Enumerable.Range(0, 1_000).Select(_ => Backoff.Jittered(ceiling)).ToArray();

        Assert.All(pauses, pause => Assert.InRange(pause, ceiling / 2, ceiling));
        Assert.True(pauses.Distinct().Count() > 1, "every pause was the same");
    }
}
