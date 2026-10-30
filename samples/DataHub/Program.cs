using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Tokens;
using DataHub.Services;
using DataHub.Middleware;
using DataHub.Adapters;
using Microsoft.AspNetCore.ResponseCompression;
using OpenTelemetry.Metrics;
using TransportR.Hub;
using TransportR.FlowControl;
using TransportR.Interfaces;
using TransportR.Auth.SimpleIdp;
using Serilog;
using TransportR.Storage.Redis;

var builder = WebApplication.CreateBuilder(args);

// Disable Kestrel Limits to prevent stalls on slow connections (e.g. Control Stream ACKs)
builder.WebHost.ConfigureKestrel(options =>
{
    options.Limits.MinRequestBodyDataRate = null;
    options.Limits.MinResponseDataRate = null;
    options.Limits.MaxRequestBodySize = null;
    options.Limits.KeepAliveTimeout = TimeSpan.FromMinutes(10);
    options.Limits.RequestHeadersTimeout = TimeSpan.FromMinutes(10);
});

// OIDC authentication configuration (optional, disabled by default)
var enableAuth = builder.Configuration.GetValue<bool>("EnableAuth", false);
var enableEmbeddedIdp = builder.Configuration.GetValue<bool>("EnableEmbeddedIdp", false);
var hubPort = builder.Configuration.GetValue<int>("HUB_PORT", 5252);
var hubUrl = $"http://localhost:{hubPort}";
var idpUrl = enableEmbeddedIdp ? hubUrl : (builder.Configuration["Oidc:Authority"] ?? "http://localhost:5100");

// Embedded IDP Configuration (optional)
if (enableEmbeddedIdp)
{
    builder.Services.AddSimpleIdp(options =>
    {
        options.AddClient("sender", "sender-secret", "senders");
        options.AddClient("receiver", "receiver-secret", "receivers");
    });
    Console.WriteLine("Embedded IDP: ENABLED (SimpleIdp on this host)");
}

if (enableAuth)
{
    builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
        .AddJwtBearer(options =>
        {
            options.Authority = idpUrl;
            options.RequireHttpsMetadata = false; // Allow HTTP for development (samples only)
            
            options.TokenValidationParameters = new TokenValidationParameters
            {
                ValidateIssuer = true,
                // OpenIddict issues tokens with issuer without trailing slash, but sometimes with
                ValidIssuers = new[] { idpUrl, idpUrl.TrimEnd('/') + "/" },
                ValidateAudience = false, // OpenIddict client_credentials doesn't set audience
                ValidateLifetime = true,
                ValidateIssuerSigningKey = true
            };
        });

    builder.Services.AddAuthorization();
    Console.WriteLine($"Authentication: ENABLED (IDP: {idpUrl})");
}
else
{
    Console.WriteLine("Authentication: DISABLED (Dev Mode)");
}

// CORS Configuration
builder.Services.AddControllers();
builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy.AllowAnyHeader()
              .AllowAnyMethod()
              .AllowCredentials()
              .SetIsOriginAllowed(_ => true); // For development
    });
});

// Services
builder.Services.AddSingleton<ProgressMonitorService>();
builder.Services.AddSingleton<DataProxyMetrics>();
builder.Services.AddSingleton<TransportR.Interfaces.IHubProgressMonitor, ProgressMonitorAdapter>();

// Register Serializer (required before AddDataHub)


// Add DataHub Library Services with identity provider configuration
var dataHubBuilder = builder.Services.AddDataHub();

// Configure State Store (InMemory vs TestProxy)
// Configure State Store (InMemory vs TestProxy vs Redis)
var testProxyUrl = Environment.GetEnvironmentVariable("USE_TEST_PROXY");
var redisConn = Environment.GetEnvironmentVariable("USE_REDIS");

if (!string.IsNullOrEmpty(redisConn))
{
    Console.WriteLine($"StateStore: Redis ({redisConn}) + Affinity Handoff");
    dataHubBuilder.UseRedisStore(redisConn);
    dataHubBuilder.UseAffinityHandoff();
}
else if (!string.IsNullOrEmpty(testProxyUrl))
{
    Console.WriteLine($"StateStore: TestProxy ({testProxyUrl}) + Affinity Handoff");
    dataHubBuilder.UseTestProxyStore(testProxyUrl);
    dataHubBuilder.UseAffinityHandoff();
}
else
{
    Console.WriteLine("StateStore: InMemory (Default)");
}

if (enableAuth)
{
    // Production mode: Use JWT identity provider (registered by default)
    dataHubBuilder.Build();
}
else
{
    // Development mode: Trust client-provided identity
    dataHubBuilder.UseDevMode(options =>
    {
        options.DefaultGroups = new List<string> { "dev" };
        options.AllowAnonymous = false;
    }).Build();
}

// Flow Control Configuration (environment-driven)
var flowControlMode = Environment.GetEnvironmentVariable("FLOWCONTROL_MODE") ?? "default";

if (flowControlMode.Equals("none", StringComparison.OrdinalIgnoreCase))
{
    // No flow control - all transfers allowed
    Console.WriteLine("FlowControl: DISABLED (all transfers allowed)");
}
else
{
    FlowControlOptions flowControlOptions;
    
    if (flowControlMode.Equals("wildcard", StringComparison.OrdinalIgnoreCase))
    {
        // Wildcard mode - test group can send to everyone
        flowControlOptions = new FlowControlOptions
        {
            Groups = new Dictionary<string, GroupAccess>
            {
                ["test"] = new GroupAccess { CanSendTo = new List<string> { "*" } },
                ["senders"] = new GroupAccess { CanSendTo = new List<string> { "receivers" } },
                ["receivers"] = new GroupAccess { CanSendTo = new List<string>() }
            }
        };
        Console.WriteLine("FlowControl: WILDCARD (test -> *)");
    }
    else
    {
        // Default mode - senders can send to receivers
        flowControlOptions = new FlowControlOptions
        {
            Groups = new Dictionary<string, GroupAccess>
            {
                ["senders"] = new GroupAccess { CanSendTo = new List<string> { "receivers" } },
                ["receivers"] = new GroupAccess { CanSendTo = new List<string>() }
            }
        };
        Console.WriteLine("FlowControl: DEFAULT (senders -> receivers)");
    }
    
    builder.Services.AddSingleton(flowControlOptions);
    builder.Services.AddSingleton<IFlowControlService, DefaultFlowControlService>();
}

// OpenTelemetry Configuration
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics =>
    {
        metrics.AddMeter(DataProxyMetrics.MeterName);
    });

// Response compression (Gzip)
builder.Services.AddResponseCompression(opts =>
{
    opts.MimeTypes = ResponseCompressionDefaults.MimeTypes;
});

// Serilog configuration
var outputDir = Environment.GetEnvironmentVariable("OUTPUT_DIR") ?? "output";
Directory.CreateDirectory(outputDir);
var logFile = Path.Combine(outputDir, "hub.log");

Log.Logger = new LoggerConfiguration()
    .ReadFrom.Configuration(builder.Configuration)
    .WriteTo.File(logFile, rollingInterval: RollingInterval.Day)
    .CreateLogger();

builder.Host.UseSerilog();

var app = builder.Build();

// Use response compression
app.UseResponseCompression();

// Middleware for operation identifiers
app.UseMiddleware<TransferIdMiddleware>();

// CORS
app.UseCors();

// Authentication (if enabled)
if (enableAuth)
{
    app.UseAuthentication();
    app.UseAuthorization();
}

app.MapControllers(); // Maps local controllers (if any)

// Map Embedded IDP endpoints if enabled
if (enableEmbeddedIdp)
{
    app.MapSimpleIdp();
}

app.MapDataHub(requireAuth: enableAuth); // Maps DataHub endpoints (Hub + Controllers)

// Health endpoint to check service is online
app.MapGet("/health", () => new
{
    status = "healthy",
    timestamp = DateTime.UtcNow,
    authEnabled = enableAuth
});

// Information endpoint
app.MapGet("/", () => new
{
    service = "TransportR Hub (SignalR)",
    version = "1.0.0",
    healthEndpoint = "/health",
    authenticationEnabled = enableAuth
});

var logger = app.Services.GetRequiredService<ILogger<Program>>();
logger.LogInformation("=== TransportR Hub started (SignalR) ===");
logger.LogInformation("Identity Mode: {Mode}", enableAuth ? "JWT (Authentication)" : "Dev (Client-Provided)");

app.Run();
