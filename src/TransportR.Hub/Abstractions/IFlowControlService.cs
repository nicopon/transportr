namespace TransportR.Hub;

/// <summary>
/// Service for controlling data flow permissions between groups.
/// </summary>
/// <remarks>
/// Implement this interface to define custom authorization rules for
/// which groups can send data to other groups.
/// </remarks>
public interface IFlowControlService
{
    /// <summary>
    /// Determines if any source group is allowed to send data to any target group.
    /// </summary>
    /// <param name="sourceGroups">The groups of the sending client.</param>
    /// <param name="targetGroups">The groups of the receiving client.</param>
    /// <returns>True if the transfer is allowed, false otherwise.</returns>
    bool CanSendTo(IReadOnlySet<string> sourceGroups, IReadOnlySet<string> targetGroups);
}

