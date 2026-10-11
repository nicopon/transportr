using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpenIddict.Abstractions;
using OpenIddict.Server.AspNetCore;
using System.Security.Claims;
using static OpenIddict.Abstractions.OpenIddictConstants;

namespace TransportR.Auth.SimpleIdp;

/// <summary>
/// Extension methods for adding SimpleIdp to an ASP.NET Core application.
/// </summary>
public static class SimpleIdpExtensions
{
    /// <summary>
    /// Adds the embedded SimpleIdp authentication server.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Configuration action for SimpleIdp options.</param>
    public static IServiceCollection AddSimpleIdp(
        this IServiceCollection services,
        Action<SimpleIdpOptions> configure)
    {
        var options = new SimpleIdpOptions();
        configure(options);
        services.AddSingleton(options);

        // Add DbContext for OpenIddict (in-memory)
        services.AddDbContext<SimpleIdpDbContext>(opt =>
        {
            opt.UseInMemoryDatabase("SimpleIdpDb");
            opt.UseOpenIddict();
        });

        // Add MVC controllers (for token endpoint)
        services.AddControllers()
            .AddApplicationPart(typeof(SimpleIdpExtensions).Assembly);

        // Configure OpenIddict
        services.AddOpenIddict()
            .AddCore(opt =>
            {
                opt.UseEntityFrameworkCore()
                   .UseDbContext<SimpleIdpDbContext>();
            })
            .AddServer(opt =>
            {
                opt.SetTokenEndpointUris("connect/token");
                opt.AllowClientCredentialsFlow();
                
                // Register all scopes from configured clients
                var allScopes = options.Clients
                    .SelectMany(c => c.GetEffectiveScopes())
                    .Distinct()
                    .ToArray();
                opt.RegisterScopes(allScopes);

                opt.AddDevelopmentEncryptionCertificate()
                   .AddDevelopmentSigningCertificate()
                   .DisableAccessTokenEncryption(); // Produce signed JWS tokens for standard JWT validation
                   
                opt.UseAspNetCore()
                   .EnableTokenEndpointPassthrough()
                   .DisableTransportSecurityRequirement(); // Allow HTTP for development
                   
                // Set token lifetime
                opt.SetAccessTokenLifetime(options.TokenLifetime);
            })
            .AddValidation(opt =>
            {
                opt.UseLocalServer();
                opt.UseAspNetCore();
            });

        // Add hosted service to seed clients on startup
        services.AddHostedService<SimpleIdpClientSeeder>();

        return services;
    }

    /// <summary>
    /// Maps the SimpleIdp token endpoint controller.
    /// </summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    public static IEndpointRouteBuilder MapSimpleIdp(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapControllers();
        return endpoints;
    }
}

/// <summary>
/// DbContext for OpenIddict entities.
/// </summary>
public class SimpleIdpDbContext : DbContext
{
    public SimpleIdpDbContext(DbContextOptions<SimpleIdpDbContext> options) : base(options) { }
}

/// <summary>
/// Token endpoint controller for OpenIddict.
/// </summary>
[ApiController]
public class SimpleIdpTokenController : ControllerBase
{
    private readonly IOpenIddictApplicationManager _applicationManager;

    public SimpleIdpTokenController(IOpenIddictApplicationManager applicationManager)
    {
        _applicationManager = applicationManager;
    }

    [HttpPost("~/connect/token")]
    public async Task<IActionResult> Exchange()
    {
        var request = Microsoft.AspNetCore.OpenIddictServerAspNetCoreHelpers.GetOpenIddictServerRequest(HttpContext) 
            ?? throw new InvalidOperationException("The OpenID Connect request cannot be retrieved.");

        if (request.IsClientCredentialsGrantType())
        {
            var application = await _applicationManager.FindByClientIdAsync(request.ClientId!);
            if (application is null)
            {
                return BadRequest("Client not found");
            }

            var identity = new ClaimsIdentity(
                authenticationType: OpenIddictServerAspNetCoreDefaults.AuthenticationScheme,
                nameType: Claims.Name,
                roleType: Claims.Role);

            identity.SetClaim(Claims.Subject, request.ClientId);
            identity.SetClaim(Claims.Name, await _applicationManager.GetDisplayNameAsync(application));

            identity.SetScopes(request.GetScopes());
            identity.SetDestinations(claim => new[] { Destinations.AccessToken });

            return SignIn(new ClaimsPrincipal(identity), OpenIddictServerAspNetCoreDefaults.AuthenticationScheme);
        }

        return BadRequest("Only client_credentials grant type is supported");
    }
}

/// <summary>
/// Hosted service that seeds clients on application startup.
/// </summary>
public class SimpleIdpClientSeeder : IHostedService
{
    private readonly IServiceProvider _serviceProvider;

    public SimpleIdpClientSeeder(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await using var scope = _serviceProvider.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<SimpleIdpDbContext>();
        await context.Database.EnsureCreatedAsync(cancellationToken);

        var manager = scope.ServiceProvider.GetRequiredService<IOpenIddictApplicationManager>();
        var options = scope.ServiceProvider.GetRequiredService<SimpleIdpOptions>();

        foreach (var client in options.Clients)
        {
            var existing = await manager.FindByClientIdAsync(client.ClientId, cancellationToken);
            if (existing != null)
            {
                await manager.DeleteAsync(existing, cancellationToken);
            }

            // Get effective scopes (respects raw Scopes or Groups)
            var scopes = client.GetEffectiveScopes().ToList();

            var descriptor = new OpenIddictApplicationDescriptor
            {
                ClientId = client.ClientId,
                ClientSecret = client.Secret,
                DisplayName = client.DisplayName ?? client.ClientId
            };
            descriptor.Permissions.Add(OpenIddictConstants.Permissions.Endpoints.Token);
            descriptor.Permissions.Add(OpenIddictConstants.Permissions.GrantTypes.ClientCredentials);
            foreach (var scopeValue in scopes)
            {
                descriptor.Permissions.Add(OpenIddictConstants.Permissions.Prefixes.Scope + scopeValue);
            }
            await manager.CreateAsync(descriptor, cancellationToken);
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
