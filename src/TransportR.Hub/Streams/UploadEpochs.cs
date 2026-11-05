using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;

namespace TransportR.Hub.Streams;

/// <summary>
/// The epoch each write stream on this hub is on, by bridge key. An epoch leaves when the bridge is
/// done with its stream, and a newer epoch of the same stream ends the one it replaces.
/// </summary>
/// <remarks>
/// Local to the instance, as the bridges are: behind several hubs, a segment that reaches an instance
/// other than its epoch's finds nothing here and is refused, and the client opens a new epoch.
/// </remarks>
internal sealed class UploadEpochs
{
    private readonly ConcurrentDictionary<string, UploadEpoch> _current = new();
    private readonly TimeProvider _time;
    private readonly ILogger<UploadEpochs> _logger;

    public UploadEpochs(TimeProvider timeProvider, ILogger<UploadEpochs> logger)
    {
        _time = timeProvider;
        _logger = logger;
    }

    public bool TryGet(string bridgeKey, Guid epochId, [NotNullWhen(true)] out UploadEpoch? epoch)
    {
        if (_current.TryGetValue(bridgeKey, out var current) && current.Id == epochId)
        {
            epoch = current;
            return true;
        }

        epoch = null;
        return false;
    }

    /// <summary>
    /// Opens an epoch and hands its stream to <paramref name="handle"/>, which copies from it until it
    /// ends; the epoch's token cancels it. The epoch it replaces, if any, is ended.
    /// </summary>
    public UploadEpoch Open(
        string bridgeKey,
        Guid epochId,
        string? subject,
        TimeSpan heartbeatInterval,
        Func<Stream, CancellationToken, Task> handle)
    {
        var epoch = new UploadEpoch(epochId, bridgeKey, subject, heartbeatInterval, _time, _logger);

        UploadEpoch? replaced = null;
        _current.AddOrUpdate(bridgeKey, epoch, (_, previous) =>
        {
            replaced = previous;
            return epoch;
        });
        replaced?.Abandon();

        epoch.Start(
            async (input, token) =>
            {
                try
                {
                    await handle(input, token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception ex)
                {
                    // An IOException is a peer's connection failing, which the server has already
                    // reported as a warning; anything else is a fault of the hub and keeps its trace.
                    if (ex is not IOException)
                    {
                        _logger.LogError(ex, "Error in the write stream of {BridgeKey}", bridgeKey);
                    }
                }
            },
            () => _current.TryRemove(KeyValuePair.Create(bridgeKey, epoch)));

        return epoch;
    }
}
