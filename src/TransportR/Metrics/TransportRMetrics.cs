using System.Diagnostics.Metrics;

namespace TransportR.Metrics;

/// <summary>
/// Provides OpenTelemetry-compatible metrics for TransportR operations.
/// </summary>
/// <remarks>
/// These metrics can be collected by any OpenTelemetry-compatible metrics collector.
/// The meter name is "TransportR" and can be subscribed to via:
/// <code>
/// builder.Services.AddOpenTelemetry()
///     .WithMetrics(metrics => metrics.AddMeter("TransportR"));
/// </code>
/// </remarks>
public static class TransportRMetrics
{
    /// <summary>
    /// The meter instance for all TransportR metrics.
    /// </summary>
    public static readonly Meter Meter = new("TransportR", "1.0.0");

    /// <summary>
    /// Counter for total bytes sent (compressed) across all transfers.
    /// </summary>
    public static readonly Counter<long> BytesSent = Meter.CreateCounter<long>(
        "transportr.bytes.sent",
        unit: "bytes",
        description: "Total compressed bytes sent");

    /// <summary>
    /// Counter for total bytes received (compressed) across all transfers.
    /// </summary>
    public static readonly Counter<long> BytesReceived = Meter.CreateCounter<long>(
        "transportr.bytes.received",
        unit: "bytes",
        description: "Total compressed bytes received");

    /// <summary>
    /// Counter for total batches sent across all transfers.
    /// </summary>
    public static readonly Counter<int> BatchesSent = Meter.CreateCounter<int>(
        "transportr.batches.sent",
        unit: "batches",
        description: "Total batches sent");

    /// <summary>
    /// Counter for total batches received across all transfers.
    /// </summary>
    public static readonly Counter<int> BatchesReceived = Meter.CreateCounter<int>(
        "transportr.batches.received",
        unit: "batches",
        description: "Total batches received");

    /// <summary>
    /// Counter for ACK messages sent.
    /// </summary>
    public static readonly Counter<int> AcksSent = Meter.CreateCounter<int>(
        "transportr.acks.sent",
        description: "Total ACK messages sent");

    /// <summary>
    /// Counter for ACK messages received.
    /// </summary>
    public static readonly Counter<int> AcksReceived = Meter.CreateCounter<int>(
        "transportr.acks.received",
        description: "Total ACK messages received");

    /// <summary>
    /// Counter for NACK messages sent.
    /// </summary>
    public static readonly Counter<int> NacksSent = Meter.CreateCounter<int>(
        "transportr.nacks.sent",
        description: "Total NACK messages sent");

    /// <summary>
    /// Counter for NACK messages received.
    /// </summary>
    public static readonly Counter<int> NacksReceived = Meter.CreateCounter<int>(
        "transportr.nacks.received",
        description: "Total NACK messages received");

    /// <summary>
    /// Counter for retry attempts.
    /// </summary>
    public static readonly Counter<int> RetryAttempts = Meter.CreateCounter<int>(
        "transportr.retries",
        description: "Total batch retry attempts");

    /// <summary>
    /// Counter for total items processed (sent or received).
    /// </summary>
    public static readonly Counter<long> ItemsProcessed = Meter.CreateCounter<long>(
        "transportr.items.processed",
        unit: "items",
        description: "Total items processed");
}
