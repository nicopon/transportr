namespace TransportR.Hub;

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
    /// Whether affinity handoff is enabled: InitTransfer then redirects the sender to the receiver's
    /// replica. Only <c>UseAffinityHandoff()</c> turns it on, so the callback it takes cannot turn it
    /// off again.
    /// </summary>
    internal bool Enabled { get; set; } = false;
}
