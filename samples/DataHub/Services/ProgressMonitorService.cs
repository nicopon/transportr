using System.Collections.Concurrent;

namespace DataHub.Services;

public class ProgressMonitorService
{
    private readonly ILogger<ProgressMonitorService> _logger;
    private readonly DataProxyMetrics _metrics;
    private readonly ConcurrentDictionary<string, TransferProgress> _transfers = new();
    private readonly IConfiguration _configuration;
    private readonly bool _showProgress;

    public ProgressMonitorService(ILogger<ProgressMonitorService> logger, DataProxyMetrics metrics, IConfiguration configuration)
    {
        _logger = logger;
        _metrics = metrics;
        _configuration = configuration;
        _showProgress = _configuration.GetValue<bool>("ShowProgress", false);
    }

    public void InitializeTransfer(string transferId)
    {
        var tracker = new TransferProgress
        {
            StartTime = DateTime.UtcNow,
            LastLogTime = DateTime.UtcNow
        };
        _transfers[transferId] = tracker;
        
        var msg = $"PROGRESS: [{transferId}] STARTED";
        if (_showProgress)
        {
            Console.WriteLine(msg);
        }
        _logger.LogInformation(msg);
    }

    public void OnBytesTransferred(long bytes)
    {
        // Global tracker for Hub throughput monitoring
        var tracker = _transfers.GetOrAdd("Global", _ => new TransferProgress 
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
                    LogProgress("Global", tracker);
                    tracker.LastLogTime = now;
                }
            }
        }
    }

    private void LogProgress(string transferId, TransferProgress tracker)
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
