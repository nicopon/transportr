using System.Diagnostics;

namespace TransportR.Client;

/// <summary>
/// Built-in diagnostics identifiers for TransportR clients (<see cref="System.Diagnostics.Metrics.Meter"/>
/// and <see cref="System.Diagnostics.ActivitySource"/>).
/// </summary>
public static class TransportRClientDiagnostics
{
    /// <summary>
    /// Name of the client <see cref="System.Diagnostics.Metrics.Meter"/>.
    /// </summary>
    public const string MeterName = "TransportR.Client";

    /// <summary>
    /// Name of the client <see cref="System.Diagnostics.ActivitySource"/>.
    /// </summary>
    public const string ActivitySourceName = "TransportR.Client";

    internal static readonly ActivitySource ActivitySource = new(ActivitySourceName);
}
