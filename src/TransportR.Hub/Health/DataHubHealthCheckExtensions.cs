using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using TransportR.Hub.Health;

namespace TransportR.Hub;

/// <summary>
/// Health check registration extensions for the TransportR hub.
/// </summary>
public static class DataHubHealthCheckExtensions
{
    /// <summary>
    /// Adds a readiness health check for the TransportR hub that probes <see cref="IStateStore"/>
    /// and reports <see cref="HealthStatus.Unhealthy"/> while the hub is draining.
    /// </summary>
    public static IHealthChecksBuilder AddDataHub(
        this IHealthChecksBuilder builder,
        string name = "transportr_hub",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null,
        TimeSpan? timeout = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.AddCheck<DataHubHealthCheck>(name, failureStatus, tags, timeout);
    }
}
