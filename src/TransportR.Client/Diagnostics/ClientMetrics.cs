using System.Diagnostics;
using System.Diagnostics.Metrics;
using TransportR.Client.Models;

namespace TransportR.Client.Diagnostics;

internal static class ClientMetrics
{
    private static readonly Meter Meter = new(TransportRClientDiagnostics.MeterName);

    private static readonly UpDownCounter<int> TransfersActive = Meter.CreateUpDownCounter<int>(
        "transportr.client.transfers.active",
        unit: "{transfer}",
        description: "Number of active transfer sessions on clients.");

    private static readonly Counter<long> TransfersCompleted = Meter.CreateCounter<long>(
        "transportr.client.transfers.completed",
        unit: "{transfer}",
        description: "Total client transfer sessions ended, tagged by direction and outcome.");

    private static readonly Counter<long> BytesTransferred = Meter.CreateCounter<long>(
        "transportr.client.bytes.transferred",
        unit: "By",
        description: "Total payload bytes transferred by clients, tagged by direction.");

    private static readonly Counter<long> Reconnections = Meter.CreateCounter<long>(
        "transportr.client.reconnections",
        unit: "{reconnection}",
        description: "Total stream reconnection episodes initiated by client sessions.");

    private static readonly Counter<long> WriteTransportFallbacks = Meter.CreateCounter<long>(
        "transportr.client.write_transport.fallbacks",
        unit: "{fallback}",
        description: "Total write-stream fallbacks from WebSocket to HTTP segments.");

    private static string DirectionTag(bool isSender) => isSender ? "sending" : "receiving";

    public static void TransferStarted(bool isSender) =>
        TransfersActive.Add(1, new KeyValuePair<string, object?>("transportr.direction", DirectionTag(isSender)));

    public static void TransferEnded(bool isSender, SessionState finalState, TransferFailureCategory? failureCategory)
    {
        var direction = DirectionTag(isSender);
        TransfersActive.Add(-1, new KeyValuePair<string, object?>("transportr.direction", direction));

        var outcome = finalState switch
        {
            SessionState.Completed => "completed",
            SessionState.Failed => "failed",
            _ => "abandoned"
        };

        var tags = new TagList
        {
            { "transportr.direction", direction },
            { "transportr.outcome", outcome }
        };
        if (failureCategory is { } category)
        {
            tags.Add("transportr.failure_category", category.ToString());
        }

        TransfersCompleted.Add(1, tags);
    }

    public static void RecordBytesTransferred(bool isSender, long bytes)
    {
        if (bytes > 0)
        {
            BytesTransferred.Add(bytes, new KeyValuePair<string, object?>("transportr.direction", DirectionTag(isSender)));
        }
    }

    public static void RecordReconnection(bool isSender) =>
        Reconnections.Add(1, new KeyValuePair<string, object?>("transportr.direction", DirectionTag(isSender)));

    public static void RecordWriteTransportFallback() =>
        WriteTransportFallbacks.Add(1);
}
