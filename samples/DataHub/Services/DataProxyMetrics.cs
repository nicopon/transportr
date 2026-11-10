using System.Diagnostics.Metrics;

namespace DataHub.Services;

public class DataProxyMetrics
{
    public const string MeterName = "DataProxy.Metrics";
    private readonly Meter _meter;

    public Counter<long> ItemsProcessed { get; }
    public Counter<long> BytesTransferred { get; }
    public Histogram<double> BatchThroughput { get; }

    public DataProxyMetrics(IMeterFactory meterFactory)
    {
        _meter = meterFactory.Create(MeterName);

        ItemsProcessed = _meter.CreateCounter<long>(
            "dataproxy.items.processed",
            description: "Total number of items processed");

        BytesTransferred = _meter.CreateCounter<long>(
            "dataproxy.bytes.transferred",
            unit: "By",
            description: "Total bytes transferred");

        BatchThroughput = _meter.CreateHistogram<double>(
            "dataproxy.batch.throughput",
            unit: "items/s",
            description: "Throughput of processed batches");
    }
}
