using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Shared.Models;
using Serilog;
using Sender.Configuration;
using Sender.Services;
using TransportR;
using TransportR.Abstractions;
using TransportR.Client.SignalR.Services;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Client.SignalR;
using TransportR.Auth.ClientCredentials; // Extension
using TransportR.Serialization.MessagePack; // Extension
using TransportR.Serialization.Protobuf; // Extension
using TransportR.Serialization.Json; // Extension

// Configuration builder
var configuration = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
    .AddEnvironmentVariables()
    .Build();

// Options binding
var senderOptions = new SenderOptions();
configuration.GetSection("Sender").Bind(senderOptions);

// Serilog configuration
var logFile = Path.Combine(senderOptions.OutputDirectory, "sender.log");
Log.Logger = new LoggerConfiguration()
    .ReadFrom.Configuration(configuration)
    .WriteTo.File(logFile, rollingInterval: RollingInterval.Day)
    .CreateLogger();

using var loggerFactory = LoggerFactory.Create(builder =>
{
    builder.AddSerilog(dispose: true);
});

var logger = loggerFactory.CreateLogger<Program>();

logger.LogInformation("=== Sender - Starting ===");

logger.LogInformation("Configuration:");
logger.LogInformation("  - Source File: {SourceFilePath}", senderOptions.SourceFilePath);
logger.LogInformation("  - Hub URL: {HubUrl}", senderOptions.HubUrl);
logger.LogInformation("  - Batch Size: {BatchSize}", senderOptions.BatchSize);
logger.LogInformation("  - Group: {Group}", senderOptions.Group);

var sourceFilePath = senderOptions.SourceFilePath;
var hubUrl = senderOptions.HubUrl;
var batchSize = senderOptions.BatchSize;
var batchTimeoutMs = senderOptions.BatchTimeoutMs;
var group = senderOptions.Group;
var showProgress = configuration.GetValue<bool>("ShowProgress", false);

if (!File.Exists(sourceFilePath))
{
    logger.LogError("Source file not found: {SourceFilePath}", sourceFilePath);
    return;
}

// Services
// Serialization internally managed via builder

// Initialization
int parallelism = 1; // Default
if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("SENDER__PARALLELISM")))
{
    int.TryParse(Environment.GetEnvironmentVariable("SENDER__PARALLELISM"), out parallelism);
}
logger.LogInformation("Parallelism configured: {Parallelism}", parallelism);

// URL from configuration
var hubBaseUrl = hubUrl;

// Token provider setup
if (senderOptions.Auth?.IsConfigured == true)
{
     logger.LogInformation("Auth: Client Credentials enabled (ClientId={ClientId})", senderOptions.Auth.ClientId);
}
else if (!string.IsNullOrEmpty(senderOptions.AccessToken))
{
    logger.LogInformation("Auth: Using static access token");
}
else
{
    logger.LogInformation("Auth: Disabled");
}

// Single Client
var clientBuilder = new TransportRClientBuilder<Order>()
    .WithUrl(hubBaseUrl)
    .WithBatchSize(batchSize)
    .WithBatchTimeout(batchTimeoutMs)
    .WithLoggerFactory(loggerFactory);

if (senderOptions.MaxInFlightBatches is { } maxInFlight)
{
    clientBuilder.WithMaxInFlightBatches(maxInFlight);
    logger.LogInformation("Send window: {Max} batches in flight", maxInFlight);
}
else
{
    logger.LogInformation("Send window: unbounded");
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

if (senderOptions.Auth?.IsConfigured == true)
{
    clientBuilder.WithClientCredentials(
        senderOptions.Auth.TokenEndpoint,
        senderOptions.Auth.ClientId,
        senderOptions.Auth.ClientSecret,
        senderOptions.Auth.Scope,
        loggerFactory);
}
else if (!string.IsNullOrEmpty(senderOptions.AccessToken))
{
    clientBuilder.WithAccessToken(senderOptions.AccessToken);
}

var broker = clientBuilder.Build();

try
{
    // 1. Connect Client
    logger.LogInformation("Connecting client...");
    await broker.ConnectAsync(new[] { group });

    // 2. Discover Receivers (GetReceiversAsync returns only authorized receivers)
    List<ClientInfo> receivers = new();
    
    logger.LogInformation("Searching for authorized receivers...");
    int retry = 0;
    while(receivers.Count == 0 && retry < 10)
    {
            receivers = await broker.GetReceiversAsync();
            if (receivers.Count == 0) 
            {
                logger.LogWarning("No authorized receiver found. Waiting...");
                await Task.Delay(2000);
                retry++;
            }
    }
    
    if (receivers.Count == 0) throw new Exception("Timeout: No authorized receiver found.");
    logger.LogInformation("Found {Count} authorized receivers.", receivers.Count);
    
    // 3. Start Parallel Sending Tasks
    var tasks = new List<Task>();
    
    for (int i = 0; i < parallelism; i++)
    {
        var target = receivers[i % receivers.Count]; // Round robin
        
        // We need a separate scope/service for each stream to be safe
        var service = new OrderFileReader(sourceFilePath, loggerFactory.CreateLogger<OrderFileReader>());

        tasks.Add(Task.Run(async () => 
        {
                Send<Order>? transfer = null;
                try 
                {
                    transfer = await broker.InitTransferAsync(target.ClientId, batchSize);
                    logger.LogInformation("Sender -> Receiver {ReceiverId} [Transfer: {TransferId}]", target.ClientId, transfer.TransferId);
                    
                    var (count, startTime) = await SendOrdersAsync(transfer, service, broker, showProgress, logger, CancellationToken.None);

                    // CompleteAsync returning is the receiver's word that it has everything; the
                    // counters below are stats, not the verdict. It throws when it is not.
                    await transfer.CompleteAsync();
                    
                    var totalDuration = (DateTime.UtcNow - startTime).TotalSeconds;
                    var finalThroughput = totalDuration > 0 ? count / totalDuration : 0;
                    var finalInfo = broker.GetTransfers().FirstOrDefault(t => t.TransferId == transfer.TransferId);
                    
                    var statsMsg = $"[SENDER][STATS] Transfer {transfer.TransferId[..8]} complete: {count:N0} items | {finalInfo?.AcksReceived ?? 0} ACKs received | {finalThroughput:N0} items/s";
                    logger.LogInformation(statsMsg);

                    if (showProgress)
                        Console.WriteLine(statsMsg);
                }
                catch (TransferFailedException ex)
                {
                    logger.LogError(ex, "Transfer {TransferId} failed", ex.TransferId);
                    Console.Error.WriteLine($"[SENDER] TRANSFER FAILED: {ex.Message}");
                }
                catch(Exception ex)
                {
                    logger.LogError(ex, "Error in send task");
                }
                finally
                {
                    if (transfer != null) await transfer.DisposeAsync();
                }
        }));
    }
    
    await Task.WhenAll(tasks);

    ReportLiveSessions("SENDER", broker, logger);
}
catch (Exception ex)
{
    logger.LogCritical(ex, "Fatal error");
    Console.Error.WriteLine($"[SENDER] FATAL ERROR: {ex}");
}
finally
{
    await broker.DisposeAsync();
    logger.LogInformation("Sender shutdown.");
    Console.WriteLine("[SENDER] !!! TERMINATED !!!");
}

logger.LogInformation("=== Sender - Completed ===");

// Every released handle takes its session out of the client's registry, so a non-zero count here
// is a transfer the client is still holding.
static void ReportLiveSessions(string role, IDataClient client, Microsoft.Extensions.Logging.ILogger logger)
{
    var live = client.GetTransfers().Count;
    var msg = $"[{role}][SESSIONS] {live} live after completion";
    logger.LogInformation(msg);
    Console.WriteLine(msg);
}

static async Task<(int Count, DateTime StartTime)> SendOrdersAsync(
    Send<Order> transfer, 
    OrderFileReader dataService,
    SignalRDataClient<Order> broker,
    bool showProgress,
    Microsoft.Extensions.Logging.ILogger logger,
    CancellationToken cancellationToken)
{
    logger.LogInformation("Starting order transmission for transfer {TransferId}", transfer.TransferId);

    var startTime = DateTime.UtcNow;
    var lastStatsTime = DateTime.UtcNow;
    var lastItemsSent = 0;

    try
    {
        var ordersStream = dataService.GetOrdersStreamAsync(cancellationToken);

        int count = 0;
        await foreach (var order in ordersStream.WithCancellation(cancellationToken))
        {
            await transfer.SendAsync(order, cancellationToken);
            count++;
            
            // Periodic stats every 5 seconds
            if (showProgress && (DateTime.UtcNow - lastStatsTime).TotalSeconds >= 5)
            {
                var transfers = broker.GetTransfers();
                var info = transfers.FirstOrDefault(t => t.TransferId == transfer.TransferId);
                if (info != null)
                {
                    var duration = (DateTime.UtcNow - lastStatsTime).TotalSeconds;
                    var throughput = (count - lastItemsSent) / duration;
                    var progressMsg = $"[SENDER][STATS] Transfer {transfer.TransferId[..8]}: {count:N0} items | {info.AcksReceived} ACKs received | {throughput:N0} items/s";
                    Console.WriteLine(progressMsg);
                    logger.LogInformation(progressMsg);
                }
                lastStatsTime = DateTime.UtcNow;
                lastItemsSent = count;
            }
            else if (count % 100000 == 0)
            {
                logger.LogInformation("[Transfer {TransferId}] Sent {Count} items...", transfer.TransferId, count);
            }
        }

        return (count, startTime);
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "Error during order transmission for transfer {TransferId}", transfer.TransferId);
        throw;
    }
}
