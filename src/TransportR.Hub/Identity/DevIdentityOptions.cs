namespace TransportR.Hub;

/// <summary>
/// Options for development mode identity provider.
/// </summary>
public class DevIdentityOptions
{
    /// <summary>
    /// Default groups to assign if client doesn't provide any.
    /// </summary>
    public List<string> DefaultGroups { get; set; } = new();

    /// <summary>
    /// If true, allows connections without clientId (generates one automatically).
    /// </summary>
    public bool AllowAnonymous { get; set; } = false;
}
