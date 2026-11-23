using System.Diagnostics;

namespace TransportR.Hub;

/// <summary>
/// Built-in diagnostics identifiers for the TransportR hub (<see cref="System.Diagnostics.Metrics.Meter"/>
/// and <see cref="System.Diagnostics.ActivitySource"/>).
/// </summary>
public static class TransportRHubDiagnostics
{
    /// <summary>
    /// Name of the hub <see cref="System.Diagnostics.Metrics.Meter"/>.
    /// </summary>
    public const string MeterName = "TransportR.Hub";

    /// <summary>
    /// Name of the hub <see cref="System.Diagnostics.ActivitySource"/>.
    /// </summary>
    public const string ActivitySourceName = "TransportR.Hub";

    internal static readonly ActivitySource ActivitySource = new(ActivitySourceName);
}
