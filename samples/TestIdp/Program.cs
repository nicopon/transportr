using TransportR.Auth.SimpleIdp;

var builder = WebApplication.CreateBuilder(args);

// Load IDP settings from configuration
var idpSection = builder.Configuration.GetSection("Idp");
var clients = idpSection.GetSection("Clients").GetChildren();

// Configure SimpleIdp from appsettings.json
builder.Services.AddSimpleIdp(options =>
{
    foreach (var clientSection in clients)
    {
        var clientId = clientSection["ClientId"] ?? "";
        var secret = clientSection["Secret"] ?? "";
        var displayName = clientSection["DisplayName"];
        var scopes = clientSection.GetSection("Scopes").Get<string[]>() ?? Array.Empty<string>();

        options.AddClientWithScopes(clientId, secret, scopes, displayName);
    }
});

var app = builder.Build();

app.UseRouting();

// Map SimpleIdp token endpoint (controller registered by AddSimpleIdp)
app.MapSimpleIdp();

// Health check
app.MapGet("/health", () => new { status = "healthy", service = "TestIdp" });

// Info endpoint
app.MapGet("/", (SimpleIdpOptions options) => new
{
    service = "TransportR Test Identity Provider (using SimpleIdp)",
    tokenEndpoint = "/connect/token",
    grantTypes = new[] { "client_credentials" },
    clients = options.Clients.Select(c => new
    {
        clientId = c.ClientId,
        scopes = c.GetEffectiveScopes()
    })
});

var idpOptions = app.Services.GetRequiredService<SimpleIdpOptions>();
Console.WriteLine("=== TransportR Test IDP (using SimpleIdp) ===");
Console.WriteLine("Token Endpoint: /connect/token");
Console.WriteLine($"Clients: {string.Join(", ", idpOptions.Clients.Select(c => c.ClientId))}");

app.Run();
