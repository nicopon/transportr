namespace TransportR.Hub.Controllers;

/// <summary>
/// The heartbeat interval a client announces when it opens a stream. The hub judges the stream's
/// silence on it, so the client's choice is held to a range: too short and every stream costs a
/// probe a few milliseconds apart, too long and a stream cut in silence is never taken as cut.
/// </summary>
internal static class AnnouncedHeartbeat
{
    /// <summary>
    /// The shortest interval accepted. No setting backs it: it is under the interval a client
    /// announces by default, and above the one that would make a stream a flood of probes.
    /// </summary>
    public static readonly TimeSpan Shortest = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Whether <paramref name="heartbeatMs"/> is an interval the hub accepts. The longest is below
    /// half of <paramref name="inactivityTimeout"/>, so that two silent intervals, which is what cuts
    /// a stream, still end before the inactivity sweep ends the transfer.
    /// </summary>
    public static bool TryAccept(int? heartbeatMs, TimeSpan inactivityTimeout, out TimeSpan interval)
    {
        interval = default;
        if (heartbeatMs is not { } announced) return false;

        var candidate = TimeSpan.FromMilliseconds(announced);
        if (candidate < Shortest || candidate >= inactivityTimeout / 2) return false;

        interval = candidate;
        return true;
    }
}
