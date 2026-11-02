namespace DataHub.Services;

public class ProgressMonitorService
{
    private readonly ILogger<ProgressMonitorService> _logger;
    private readonly DataProxyMetrics _metrics;
    private TransferProgress? _hub;
    private readonly IConfiguration _configuration;
    private readonly bool _showProgress;

    public ProgressMonitorService(ILogger<ProgressMonitorService> logger, DataProxyMetrics metrics, IConfiguration configuration)
    {
        _logger = logger;
        _metrics = metrics;
        _configuration = configuration;
        _showProgress = _configuration.GetValue<bool>("ShowProgress", false);
    }

    public void OnBytesTransferred(long bytes)
    {
        // The whole hub's throughput, timed from its first bytes: the monitor is told nothing per transfer.
        var tracker = LazyInitializer.EnsureInitialized(ref _hub, () => new TransferProgress
        {
            StartTime = DateTime.UtcNow,
            LastLogTime = DateTime.UtcNow
        });

        Interlocked.Add(ref tracker.TotalCompressedBytes, bytes);
        
        // OpenTelemetry Metrics
        _metrics.BytesTransferred.Add(bytes);
        
        var now = DateTime.UtcNow;
        if ((now - tracker.LastLogTime).TotalMilliseconds >= 1000)
        {
            lock (tracker)
            {
                if ((now - tracker.LastLogTime).TotalMilliseconds >= 1000)
                {
                    LogProgress(tracker);
                    tracker.LastLogTime = now;
                }
            }
        }
    }

    private void LogProgress(TransferProgress tracker)
    {
        var duration = (DateTime.UtcNow - tracker.StartTime).TotalSeconds;
        var totalMb = tracker.TotalCompressedBytes / 1024.0 / 1024.0;
        var throughputMb = duration > 0 ? totalMb / duration : 0;
        
        var msg = string.Format("HUB THROUGHPUT: {0:F2} MB total ({1:F2} MB/s)", totalMb, throughputMb);
            
        if (_showProgress)
        {
            Console.WriteLine(msg);
        }
        _logger.LogInformation(msg);
    }

    private class TransferProgress
    {
        public DateTime StartTime { get; set; }
        public DateTime LastLogTime { get; set; }
        public long TotalCompressedBytes;
    }
}
