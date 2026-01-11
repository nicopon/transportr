namespace TransportR.FlowControl;

/// <summary>
/// Configuration options for the flow control service.
/// </summary>
public class FlowControlOptions
{
    /// <summary>
    /// Dictionary mapping group names to their access configuration.
    /// </summary>
    /// <remarks>
    /// Key: group name, Value: access configuration with CanSendTo list.
    /// Use "*" in CanSendTo to allow sending to all groups.
    /// </remarks>
    public Dictionary<string, GroupAccess> Groups { get; set; } = new();
}

/// <summary>
/// Defines the access permissions for a group.
/// </summary>
public class GroupAccess
{
    /// <summary>
    /// List of group names this group can send data to.
    /// Use "*" to allow sending to all groups.
    /// </summary>
    public List<string> CanSendTo { get; set; } = new();
}
