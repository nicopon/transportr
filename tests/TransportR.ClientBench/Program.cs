using System.Diagnostics;
using System.Globalization;
using System.Runtime;
using TransportR.ClientBench;

const long Mebibyte = 1024 * 1024;

var options = Options.Parse(args, out var usageError);
if (options is null)
{
    if (args is ["--help"] or ["-h"])
    {
        Console.WriteLine(Options.Usage);
        return 0;
    }

    Console.Error.WriteLine(usageError);
    Console.Error.WriteLine(Options.Usage);
    return 2;
}

Workload workload = options.Mode == "bytes"
    ? new ByteWorkload(options.Seed, options.BatchBytes)
    : new ItemWorkload(options.Seed, options.BatchItems);

Console.WriteLine("# TransportR.ClientBench");
Console.WriteLine("# unit: ms of CPU of this process (Process.TotalProcessorTime) per MiB of batch body carried;");
Console.WriteLine("#       a MiB is 1,048,576 bytes of body, the 30-byte chunk header of each batch left out");
Console.WriteLine($"# run: mode={options.Mode} megabytes={options.Megabytes} warmup_megabytes={options.WarmupMegabytes} seed={options.Seed} "
    + $"batch={(options.Mode == "bytes" ? options.BatchBytes + " bytes" : options.BatchItems + " items")} window={options.SendWindow}");
Console.WriteLine($"# gc: server={GCSettings.IsServerGC} concurrent={ConfiguredConcurrentGc()} latency={GCSettings.LatencyMode} "
    + $"cores={Environment.ProcessorCount} runtime={Environment.Version}");

using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(options.TimeoutSeconds));
try
{
    await workload.ConnectAsync(options.HubUrl, options.SendWindow, timeout.Token);

    if (options.WarmupMegabytes > 0)
        await workload.TransferAsync(options.WarmupMegabytes * Mebibyte, timeout.Token);

    GC.Collect();
    GC.WaitForPendingFinalizers();
    GC.Collect();

    var gen0 = GC.CollectionCount(0);
    var gen1 = GC.CollectionCount(1);
    var gen2 = GC.CollectionCount(2);
    var cpuBefore = ProcessCpu();

    var bodyBytes = await workload.TransferAsync(options.Megabytes * Mebibyte, timeout.Token);

    var cpu = ProcessCpu() - cpuBefore;
    var collections = (GC.CollectionCount(0) - gen0, GC.CollectionCount(1) - gen1, GC.CollectionCount(2) - gen2);

    await workload.DisposeAsync();

    var megabytes = (double)bodyBytes / Mebibyte;
    var cpuMs = cpu.TotalMilliseconds;
    var inv = CultureInfo.InvariantCulture;
    Console.WriteLine($"# gc collections during the measure (information, not the measurement): gen0={collections.Item1} gen1={collections.Item2} gen2={collections.Item3}");
    Console.WriteLine(string.Create(inv,
        $"RESULT mode={options.Mode} cpu_ms={cpuMs:F1} mb={megabytes:F2} ms_per_mb={cpuMs / megabytes:F4} seed={options.Seed}"));
    return 0;
}
catch (Exception ex)
{
    Console.Error.WriteLine($"FAILED: {ex.GetType().Name}: {ex.Message}");
    return 1;
}

static TimeSpan ProcessCpu()
{
    using var process = Process.GetCurrentProcess();
    return process.TotalProcessorTime;
}

static string ConfiguredConcurrentGc()
    => AppContext.TryGetSwitch("System.GC.Concurrent", out var enabled) ? enabled.ToString() : "default";
