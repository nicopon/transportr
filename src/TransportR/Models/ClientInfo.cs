namespace TransportR.Models;

/// <summary>
/// Information about a connected client.
/// </summary>
public class ClientInfo
{
    /// <summary>
    /// Gets or sets the unique client identifier.
    /// </summary>
    public Guid ClientId { get; set; }

    /// <summary>
    /// Gets or sets the groups this client belongs to.
    /// </summary>
    public HashSet<string> Groups { get; set; } = [];
}
