namespace TransportR.Client.Internal;

/// <summary>
/// The pause between two attempts at the same thing: its ceiling doubles with each attempt that fails,
/// up to a maximum, and the pause itself is drawn from the half of that ceiling to the whole. Each
/// caller keeps its own first pause and maximum.
/// </summary>
/// <remarks>
/// The draw is why a fleet cut off together, by a hub or an identity provider that went down, does not
/// come back in step.
/// </remarks>
internal static class Backoff
{
    /// <summary>
    /// The ceiling of the pause that follows the <paramref name="rank"/>th failure of an episode, the
    /// first being 1: <paramref name="first"/> for the first, doubled for each after it, never above
    /// <paramref name="max"/>.
    /// </summary>
    public static TimeSpan Ceiling(TimeSpan first, TimeSpan max, int rank)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(rank, 1);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(first, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThan(max, first);

        var ceiling = first;
        for (var step = 1; step < rank && ceiling < max; step++)
        {
            ceiling *= 2;
        }

        return ceiling < max ? ceiling : max;
    }

    /// <summary>A pause drawn at random from half of <paramref name="ceiling"/> to the whole of it.</summary>
    public static TimeSpan Jittered(TimeSpan ceiling) => Jittered(ceiling, Random.Shared.NextDouble());

    /// <summary><see cref="Jittered(TimeSpan)"/> for a given <paramref name="draw"/>, in [0, 1).</summary>
    public static TimeSpan Jittered(TimeSpan ceiling, double draw) => ceiling * (0.5 + draw * 0.5);
}
