namespace TransportR.Hub.Configuration;

/// <summary>
/// Options for affinity detection and handoff.
/// </summary>
public class AffinityOptions
{
    /// <summary>
    /// Ordered list of known affinity cookie names to search for.
    /// The first matching cookie will be used.
    /// </summary>
    public List<string> KnownCookieNames { get; set; } = new()
    {
        "affinity",            // Azure Container Apps
        "ARRAffinity",         // Azure App Service
        "ARRAffinitySameSite", // Azure App Service (SameSite variant)
        "AWSALB",              // AWS ALB
        "AWSALBCORS",          // AWS ALB CORS
        "SERVERID",            // HAProxy default
        "route",               // Nginx sticky (common)
    };

    /// <summary>
    /// Custom cookie name to use instead of auto-detection.
    /// If set, overrides the KnownCookieNames list.
    /// </summary>
    public string? CustomCookieName { get; set; }

    /// <summary>
    /// Whether affinity handoff is enabled.
    /// When enabled, InitTransfer will redirect sender to receiver's replica.
    /// </summary>
    public bool Enabled { get; set; } = false;
}
