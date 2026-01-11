using Microsoft.Extensions.Logging;
using TransportR.Interfaces;

namespace TransportR.FlowControl;

/// <summary>
/// Default implementation of flow control using a configurable group access dictionary.
/// </summary>
public class DefaultFlowControlService : IFlowControlService
{
    private readonly FlowControlOptions _options;
    private readonly ILogger<DefaultFlowControlService>? _logger;

    /// <summary>
    /// Initializes a new instance of the DefaultFlowControlService.
    /// </summary>
    /// <param name="options">The flow control options defining allowed flows.</param>
    /// <param name="logger">Optional logger for warnings.</param>
    public DefaultFlowControlService(FlowControlOptions options, ILogger<DefaultFlowControlService>? logger = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _logger = logger;
        
        // Log warning if wildcards are detected
        foreach (var (group, access) in _options.Groups)
        {
            if (access.CanSendTo.Contains("*"))
            {
                _logger?.LogWarning("Group '{Group}' has wildcard CanSendTo - can send to ALL groups", group);
            }
        }
    }

    /// <inheritdoc />
    public bool CanSendTo(IReadOnlySet<string> sourceGroups, IReadOnlySet<string> targetGroups)
    {
        if (sourceGroups.Count == 0 || targetGroups.Count == 0)
            return false;

        // Check if source groups can send to ALL target groups
        // Each target group must be reachable by at least one source group
        foreach (var targetGroup in targetGroups)
        {
            bool canReachThisTarget = sourceGroups.Any(sourceGroup =>
            {
                if (!_options.Groups.TryGetValue(sourceGroup, out var access))
                    return false;
                
                // Wildcard covers any target
                if (access.CanSendTo.Contains("*"))
                    return true;
                    
                return access.CanSendTo.Contains(targetGroup, StringComparer.OrdinalIgnoreCase);
            });
            
            if (!canReachThisTarget)
            {
                _logger?.LogDebug("FlowControl denied: No source group can reach target group '{TargetGroup}'", targetGroup);
                return false; // At least one target group is not reachable
            }
        }
        
        return true; // All target groups are reachable
    }
}
