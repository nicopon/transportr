using System.Diagnostics;
using System.Diagnostics.Metrics;
using TransportR.Hub.Interfaces;
using TransportR.Hub.Services;

namespace TransportR.Hub.Diagnostics;

internal static class TransferEndReasons
{
    public const string Completed = "completed";
    public const string Inactivity = "inactivity";
    public const string Stalled = "stalled";
    public const string BridgeTimeout = "bridge_timeout";
    public const string AccessRevoked = "access_revoked";
    public const string Abort = "abort";
    public const string Host = "host";
}

internal sealed class HubMetrics : IDisposable
{
    private readonly Meter _meter;
    private readonly Counter<long> _bytesRelayed;
    private readonly Counter<long> _writeStreamsOpened;
    private readonly Counter<long> _transfersEnded;

    public HubMetrics(ClientStreams streams, ITransferManager transferManager, IMeterFactory? meterFactory = null)
    {
        _meter = meterFactory?.Create(TransportRHubDiagnostics.MeterName)
            ?? new Meter(TransportRHubDiagnostics.MeterName);

        _meter.CreateObservableGauge(
            "transportr.hub.clients.connected",
            () => streams.Count,
            unit: "{client}",
            description: "Number of clients currently holding an open event stream on this hub instance.");

        _meter.CreateObservableGauge(
            "transportr.hub.transfers.active",
            () => transferManager.ActiveCount,
            unit: "{transfer}",
            description: "Number of active transfers tracked by this hub instance.");

        _bytesRelayed = _meter.CreateCounter<long>(
            "transportr.hub.bytes.relayed",
            unit: "By",
            description: "Total payload bytes relayed across active data streams on this hub instance.");

        _writeStreamsOpened = _meter.CreateCounter<long>(
            "transportr.hub.write_streams.opened",
            unit: "{stream}",
            description: "Total write streams opened on the hub, tagged by transport and connection type.");

        _transfersEnded = _meter.CreateCounter<long>(
            "transportr.hub.transfers.ended",
            unit: "{transfer}",
            description: "Total transfers retired or terminated on this hub instance, tagged by end reason.");
    }

    public void RecordBytesRelayed(long bytes)
    {
        if (bytes > 0)
        {
            _bytesRelayed.Add(bytes);
        }
    }

    public void RecordWriteStreamOpened(ConnectionType type, WriteTransport transport)
    {
        var tags = new TagList
        {
            { "transportr.transport", transport == WriteTransport.WebSocket ? "websocket" : "segments" },
            { "transportr.connection_type", type == ConnectionType.Data ? "data" : "control" },
        };
        _writeStreamsOpened.Add(1, tags);
    }

    public void RecordTransferEnded(string reason)
    {
        _transfersEnded.Add(1, new KeyValuePair<string, object?>("transportr.end_reason", reason));
    }

    public void Dispose() => _meter.Dispose();
}
