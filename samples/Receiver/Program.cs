using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using TransportR.Abstractions;
using Shared.Models;
using Serilog;
using Receiver.Configuration;
using TransportR.Interfaces;
using TransportR.Client;
using TransportR.Auth.ClientCredentials; // Extension
using TransportR.Serialization.MessagePack; // Extension
using TransportR.Serialization.Protobuf; // Extension
using TransportR.Serialization.Json; // Extension

// Note: BatchTimeoutMs is not used by the Receiver, but required by constructor.
var batchTimeoutMs = 1000; // Default value, as it's not used by the receiver.



// Build configuration
var configuration = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
    .AddEnvironmentVariables() // Allows override via RECEIVER__BATCHSIZE, etc.
    .Build();

// Options binding
var receiverOptions = new ReceiverOptions();
configuration.GetSection("Receiver").Bind(receiverOptions);

// Serilog configuration
var logFile = Path.Combine(receiverOptions.OutputDirectory, "receiver.log");
Log.Logger = new LoggerConfiguration()
    .ReadFrom.Configuration(configuration)
    .WriteTo.File(logFile, rollingInterval: RollingInterval.Day)
    .WriteTo.Console()
    .CreateLogger();

using var loggerFactory = LoggerFactory.Create(builder =>
{
    builder.AddSerilog(dispose: true);
});

// We'll use a main logger with explicit category "Receiver" to ensure it matches the log level filter
var logger = loggerFactory.CreateLogger("Receiver");

logger.LogInformation("=== Receiver - Starting ===");
logger.LogInformation("Configuration:");
logger.LogInformation("  - Output Directory: {OutputDirectory}", receiverOptions.OutputDirectory);
logger.LogInformation("  - Hub URL: {HubUrl}", receiverOptions.HubUrl);
logger.LogInformation("  - Group: {Group}", receiverOptions.Group);

var group = receiverOptions.Group;
var showProgress = configuration.GetValue<bool>("ShowProgress", false);

// Initialization
int expectedTransfers = 1; // Default
if (Environment.GetEnvironmentVariable("RECEIVER__PARALLELISM") != null)
{
    int.TryParse(Environment.GetEnvironmentVariable("RECEIVER__PARALLELISM"), out expectedTransfers);
}
logger.LogInformation("Expected transfers: {Count}", expectedTransfers);


// URL from configuration
var hubBaseUrl = receiverOptions.HubUrl;

// Optional: Override dataUrl logic removed as per new architecture


// Token provider setup
if (receiverOptions.Auth?.IsConfigured == true)
{
     logger.LogInformation("Auth: Automatic token acquisition enabled (ClientId={ClientId})", receiverOptions.Auth.ClientId);
}
else if (!string.IsNullOrEmpty(receiverOptions.AccessToken))
{
    logger.LogInformation("Auth: Using static access token");
}
else
{
    logger.LogInformation("Auth: Disabled");
}

// The byte-stream mode has no Order, no serializer and no batch size, so it runs apart from the item mode.
if (receiverOptions.ByteStreamBatchBytes is { } byteStreamBatchBytes)
{
    await RunByteStreamAsync(receiverOptions, byteStreamBatchBytes, expectedTransfers, loggerFactory, logger, showProgress);
    return;
}

// Single Client Instance
var clientBuilder = new TransportRClientBuilder<Order>()
    .WithUrl(hubBaseUrl)
    .WithBatchSize(receiverOptions.BatchSize)
    .WithBatchTimeout(batchTimeoutMs)
    .WithLoggerFactory(loggerFactory)
    .WithBatchTimeout(batchTimeoutMs)
    .WithLoggerFactory(loggerFactory);

// Receive capacity: unbounded unless a scenario asks for one.
if (receiverOptions.ReceiveMaxItems is not null || receiverOptions.ReceiveMaxBytes is not null)
{
    logger.LogInformation("Receive capacity: {Items} items / {Bytes} wire bytes",
        receiverOptions.ReceiveMaxItems?.ToString() ?? "unbounded",
        receiverOptions.ReceiveMaxBytes?.ToString() ?? "unbounded");
    clientBuilder.WithReceiveCapacity(receiverOptions.ReceiveMaxItems, receiverOptions.ReceiveMaxBytes);
}

if (receiverOptions.WriteTransport is { } writeTransport)
{
    clientBuilder.WithWriteTransport(writeTransport);
    logger.LogInformation("Write transport policy: {Policy}", writeTransport);
}

if (receiverOptions.ConsumeItemsPerSecond > 0)
{
    logger.LogInformation("Consumer throttled to {Rate} items/s", receiverOptions.ConsumeItemsPerSecond);
}

// Serializer Selection (Environment Variable or Config)
var serializerType = configuration["SERIALIZER_TYPE"] ?? "MessagePack";

 if (serializerType.Equals("Protobuf", StringComparison.OrdinalIgnoreCase))
{
    var levelStr = configuration["PROTOBUF_COMPRESSION_LEVEL"] ?? "-1";
    int.TryParse(levelStr, out int compressionLevel);

    logger.LogInformation("Serializer: Protobuf + Zstd (Level {Level})", compressionLevel);
    clientBuilder.WithProtobufSerialization(compressionLevel);
}
else if (serializerType.Equals("Json", StringComparison.OrdinalIgnoreCase))
{
    var levelStr = configuration["JSON_COMPRESSION_LEVEL"] ?? "-1";
    int.TryParse(levelStr, out int compressionLevel);

    logger.LogInformation("Serializer: System.Text.Json + Zstd (Level {Level})", compressionLevel);
    clientBuilder.WithJsonSerialization(compressionLevel: compressionLevel);
}
else
{
    logger.LogInformation("Serializer: MessagePack + Lz4 (Default)");
    clientBuilder.WithMessagePackSerialization(useAnnotations: true);
}
    
if (receiverOptions.Auth?.IsConfigured == true)
{
    clientBuilder.WithClientCredentials(
        receiverOptions.Auth.TokenEndpoint,
        receiverOptions.Auth.ClientId,
        receiverOptions.Auth.ClientSecret,
        receiverOptions.Auth.Scope,
        loggerFactory);
}
else if (!string.IsNullOrEmpty(receiverOptions.AccessToken))
{
    clientBuilder.WithAccessToken(receiverOptions.AccessToken);
}

var broker = clientBuilder.Build();

// Handler for shutdown
var cts = new CancellationTokenSource();
Console.CancelKeyPress += (sender, e) =>
{
    e.Cancel = true;
    cts.Cancel();
    logger.LogInformation("Shutdown requested...");
};

// Countdown for completion
var completionSignal = new System.Threading.CountdownEvent(expectedTransfers);

try
{
    await broker.ConnectAsync(new[] { group }, cts.Token);
    logger.LogInformation("Client registered (ID={ClientId}, Group={Group}). Waiting for {Count} streams...", broker.ClientId, group, expectedTransfers);

    broker.OnTransferStarted += async (Receive<Order> transfer) => 
    {
        // Run specific transfer logic in background
        _ = Task.Run(async () => 
        {
            try
            {
                // The handler owns the handle: nothing else will release the session behind it,
                // and the scope has to close before the signal below, or the count the main flow
                // then reads would race the release it is there to check.
                await using (transfer)
                {
                    logger.LogInformation("[Transfer {TransferId}] Starting reception (BatchSize: {BatchSize})", transfer.TransferId, transfer.BatchSize);
                    await ProcessStreamAsync(transfer, broker, receiverOptions.OutputDirectory, showProgress, receiverOptions.ConsumeItemsPerSecond, cts.Token, logger);

                    // ProcessStreamAsync's loop already waits for the shutdown handshake before
                    // returning (and throws into the same catch below if it fails), so this now
                    // resolves at once; harmless to leave.
                    await transfer.WaitForCompletionAsync(cts.Token);

                    logger.LogInformation("[Transfer {TransferId}] Transfer completed.", transfer.TransferId);
                }
            }
            catch (Exception ex)
            {
                logger.LogCritical(ex, "[Transfer {TransferId}] Critical error", transfer.TransferId);
            }
                try { completionSignal.Signal(); } catch (Exception ex) { logger.LogError(ex, "Error signalling"); }
                logger.LogInformation("[Transfer {TransferId}] Signalled.", transfer.TransferId);

        });
        await Task.CompletedTask;
    };

    // Wait for all operations to complete
    await Task.Run(() => completionSignal.Wait(cts.Token));
    logger.LogInformation("All expected transfers completed.");

    ReportLiveSessions("RECEIVER", broker, logger);
}
catch (OperationCanceledException) { }
catch (Exception ex)
{
    logger.LogCritical(ex, "Fatal error");
    Console.Error.WriteLine($"[RECEIVER] FATAL ERROR: {ex}");
}
finally 
{
    await broker.DisposeAsync();
    Console.WriteLine("[RECEIVER] !!! TERMINATED !!!");
}

// Every released handle takes its session out of the client's registry, so a non-zero count here
// is a transfer the client is still holding — the leak this reports is invisible in RSS, which a
// buffered transfer's own footprint dwarfs.
static void ReportLiveSessions(string role, IDataClient client, Microsoft.Extensions.Logging.ILogger logger)
{
    var live = client.GetTransfers().Count;
    var msg = $"[{role}][SESSIONS] {live} live after completion";
    logger.LogInformation(msg);
    Console.WriteLine(msg);
}

static async Task RunByteStreamAsync(
    ReceiverOptions options,
    int batchBytes,
    int expectedTransfers,
    ILoggerFactory loggerFactory,
    Microsoft.Extensions.Logging.ILogger logger,
    bool showProgress)
{
    if (options.ConsumeItemsPerSecond > 0)
    {
        logger.LogError("A byte stream has no items to pace: ConsumeItemsPerSecond does not apply to the byte-stream mode.");
        Environment.ExitCode = 1;
        return;
    }

    // A byte stream has no items to bound: the setting fails instead of being ignored.
    if (options.ReceiveMaxItems is not null)
    {
        logger.LogError("A byte stream has no items to bound: ReceiveMaxItems does not apply to the byte-stream mode.");
        Environment.ExitCode = 1;
        return;
    }

    var clientBuilder = new ByteStreamClientBuilder(batchBytes)
        .WithUrl(options.HubUrl)
        .WithLoggerFactory(loggerFactory);
    logger.LogInformation("Byte-stream mode");

    if (options.ReceiveMaxBytes is not null)
    {
        logger.LogInformation("Receive capacity: {Bytes} wire bytes", options.ReceiveMaxBytes);
        clientBuilder.WithReceiveCapacity(options.ReceiveMaxBytes.Value);
    }

    if (options.WriteTransport is { } writeTransport)
    {
        clientBuilder.WithWriteTransport(writeTransport);
        logger.LogInformation("Write transport policy: {Policy}", writeTransport);
    }

    if (options.Auth?.IsConfigured == true)
    {
        clientBuilder.WithClientCredentials(
            options.Auth.TokenEndpoint,
            options.Auth.ClientId,
            options.Auth.ClientSecret,
            options.Auth.Scope,
            loggerFactory);
    }
    else if (!string.IsNullOrEmpty(options.AccessToken))
    {
        clientBuilder.WithAccessToken(options.AccessToken);
    }

    var broker = clientBuilder.Build();

    var cts = new CancellationTokenSource();
    Console.CancelKeyPress += (sender, e) =>
    {
        e.Cancel = true;
        cts.Cancel();
        logger.LogInformation("Shutdown requested...");
    };

    var completionSignal = new CountdownEvent(expectedTransfers);

    try
    {
        await broker.ConnectAsync(new[] { options.Group }, cts.Token);
        logger.LogInformation("Client registered (ID={ClientId}, Group={Group}). Waiting for {Count} streams...", broker.ClientId, options.Group, expectedTransfers);

        broker.OnTransferStarted += async (ByteReceive transfer) =>
        {
            _ = Task.Run(async () =>
            {
                try
                {
                    // The handler owns the handle, and its scope closes before the signal below, so the
                    // session count the main flow reads does not race the release it is there to check.
                    await using (transfer)
                    {
                        logger.LogInformation("[Transfer {TransferId}] Starting reception", transfer.TransferId);
                        var written = await WriteByteStreamAsync(transfer, options.OutputDirectory, cts.Token, logger);

                        var finalInfo = broker.GetTransfers().FirstOrDefault(t => t.TransferId == transfer.TransferId);
                        var finalStatsMsg = $"[RECEIVER][STATS] Transfer {transfer.TransferId[..8]} complete: {written:N0} bytes | {finalInfo?.AcksSent ?? 0} ACKs sent";
                        logger.LogInformation(finalStatsMsg);
                        if (showProgress)
                            Console.WriteLine(finalStatsMsg);

                        logger.LogInformation("[Transfer {TransferId}] Transfer completed.", transfer.TransferId);
                    }
                }
                catch (Exception ex)
                {
                    logger.LogCritical(ex, "[Transfer {TransferId}] Critical error", transfer.TransferId);
                }
                try { completionSignal.Signal(); } catch (Exception ex) { logger.LogError(ex, "Error signalling"); }
                logger.LogInformation("[Transfer {TransferId}] Signalled.", transfer.TransferId);
            });
            await Task.CompletedTask;
        };

        await Task.Run(() => completionSignal.Wait(cts.Token));
        logger.LogInformation("All expected transfers completed.");

        ReportLiveSessions("RECEIVER", broker, logger);
    }
    catch (OperationCanceledException) { }
    catch (Exception ex)
    {
        logger.LogCritical(ex, "Fatal error");
        Console.Error.WriteLine($"[RECEIVER] FATAL ERROR: {ex}");
    }
    finally
    {
        await broker.DisposeAsync();
        Console.WriteLine("[RECEIVER] !!! TERMINATED !!!");
    }
}

static async Task<long> WriteByteStreamAsync(ByteReceive transfer, string outputDirectory, CancellationToken cancellationToken, Microsoft.Extensions.Logging.ILogger logger)
{
    var binFile = Path.Combine(outputDirectory, $"orders_{transfer.TransferId}.bin");
    logger.LogInformation("[Transfer {TransferId}] Creating file: {Path}", transfer.TransferId, binFile);

    await using var file = new FileStream(
        binFile, FileMode.Create, FileAccess.Write, FileShare.None,
        bufferSize: 1, FileOptions.Asynchronous);
    try
    {
        await transfer.CopyToAsync(file, cancellationToken);
    }
    catch (Exception ex)
    {
        if (!cancellationToken.IsCancellationRequested)
            logger.LogError(ex, "[Transfer {TransferId}] Error during stream", transfer.TransferId);
    }

    await file.FlushAsync(CancellationToken.None);
    return file.Length;
}

static async Task<long> ProcessStreamAsync(Receive<Order> transfer, IDataClient broker, string outputDirectory, bool showProgress, int consumeItemsPerSecond, CancellationToken cancellationToken, Microsoft.Extensions.Logging.ILogger logger)
{
    var csvFile = Path.Combine(outputDirectory, $"orders_{transfer.TransferId}.csv"); 
    using var writer = new StreamWriter(csvFile, false);

    int itemsReceived = 0;
    logger.LogInformation("[Transfer {TransferId}] Creating CSV file: {Path}", transfer.TransferId, csvFile);

    var startTime = DateTime.UtcNow;
    var lastStatsTime = DateTime.UtcNow;
    var lastItemsReceived = 0;
    DateTime? pacingOrigin = null;

    try
    {
        var stream = transfer.ReceiveAsync(cancellationToken);
        
        await foreach (var order in stream.WithCancellation(cancellationToken))
        {
            itemsReceived++;

            // Pace against the elapsed time rather than sleeping per item: a per-item sleep is
            // floored by the timer's resolution, which throttles far harder than asked and makes
            // the configured rate a fiction. Sleeping only once the debt is worth a timer tick
            // keeps the long-run rate on target.
            //
            // The clock starts on the first item, not on the handshake: anchoring it earlier
            // banks the setup time as credit and lets the first seconds run unthrottled.
            if (consumeItemsPerSecond > 0)
            {
                pacingOrigin ??= DateTime.UtcNow;
                var due = pacingOrigin.Value.AddSeconds((double)itemsReceived / consumeItemsPerSecond);
                var debt = due - DateTime.UtcNow;
                if (debt > TimeSpan.FromMilliseconds(15)) await Task.Delay(debt, cancellationToken);
            }

            await writer.WriteLineAsync($"{order.OrderId},{order.OrderDate:O},{order.CustomerName},{order.CustomerEmail},{order.ShippingAddress},{order.BillingAddress},{order.TotalAmount},{order.TaxAmount},{order.DiscountAmount},{order.Currency},{order.PaymentMethod},{order.Status},{order.ShippingPriority},{order.TrackingNumber},{order.Notes}");
            
            if (itemsReceived % 20000 == 0 && !showProgress)
            {
                logger.LogInformation("[Transfer {TransferId}] Received {ItemsReceived} items...", transfer.TransferId, itemsReceived);
            }

            if ((DateTime.UtcNow - lastStatsTime).TotalSeconds >= 5)
            {
                var now = DateTime.UtcNow;
                var duration = (now - lastStatsTime).TotalSeconds;
                var deltaItems = itemsReceived - lastItemsReceived;
                var throughput = deltaItems / duration;
                
                var transfers = broker.GetTransfers();
                var currentTransfer = transfers.FirstOrDefault(t => t.TransferId == transfer.TransferId);
                
                var statsMsg = $"[RECEIVER][STATS] Transfer {transfer.TransferId[..8]}: {itemsReceived:N0} items | {currentTransfer?.AcksSent ?? 0} ACKs sent | {throughput:N0} items/s";
                if (showProgress)
                    Console.WriteLine(statsMsg);
                logger.LogInformation(statsMsg);

                lastStatsTime = now;
                lastItemsReceived = itemsReceived;
            }
        }
    }
    catch (Exception ex)
    {
        if (!cancellationToken.IsCancellationRequested)
            logger.LogError(ex, "[Transfer {TransferId}] Error during stream", transfer.TransferId);
    }
    
    var totalDuration = (DateTime.UtcNow - startTime).TotalSeconds;
    var finalThroughput = totalDuration > 0 ? itemsReceived / totalDuration : 0;
    var finalInfo = broker.GetTransfers().FirstOrDefault(t => t.TransferId == transfer.TransferId);

    var finalStatsMsg = $"[RECEIVER][STATS] Transfer {transfer.TransferId[..8]} complete: {itemsReceived:N0} items | {finalInfo?.AcksSent ?? 0} ACKs sent | {finalThroughput:N0} items/s";
    if (showProgress)
        Console.WriteLine(finalStatsMsg);
    logger.LogInformation(finalStatsMsg);
    
    return itemsReceived;
}
