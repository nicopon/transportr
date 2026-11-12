using System.Security.Claims;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TransportR.Hub;

namespace TransportR.Tests.Infrastructure;

/// <summary>
/// A hub host with <c>MapDataHub(requireAuth: true)</c> and a stub scheme that attests the subject,
/// the groups and the expiry a caller puts in its token.
/// </summary>
/// <remarks>
/// The stub stands in for the IdP and the JwtBearer middleware, not for the authorization decision:
/// the point of these tests is what the hub does with an attested identity. Like JwtBearer with
/// <c>ValidateLifetime</c> it refuses a token whose <c>exp</c> has passed, with no clock skew, so a
/// test that outlives its token meets a host that refuses it rather than one that never looks.
/// </remarks>
public static class AuthenticatedHubServerFactory
{
    public const string Scheme = "Stub";

    /// <summary>The group the one-argument <c>Stub &lt;subject&gt;</c> form attests.</summary>
    public const string DefaultGroup = "test";

    /// <summary>
    /// An in-memory host.
    /// <paramref name="configureServices"/> runs once the hub is built, so what it registers replaces
    /// the hub's own (the identity provider, the state store). <paramref name="mapHub"/> maps the hub
    /// in place of <c>MapDataHub(requireAuth: true)</c>.
    /// </summary>
    public static IHost CreateHost(
        Action<DataHubBuilder>? configureHub = null,
        Action<IServiceCollection>? configureServices = null,
        Action<IEndpointRouteBuilder>? mapHub = null)
    {
        return new HostBuilder()
            .ConfigureWebHost(webBuilder => Configure(webBuilder.UseTestServer(), configureHub, configureServices, mapHub))
            .Start();
    }

    /// <summary>
    /// A Kestrel host on the loopback interface, so WebSockets is a transport a client can use, and a
    /// connection keeps the principal it was opened with. <paramref name="configureServices"/> is as
    /// for <see cref="CreateHost"/>.
    /// </summary>
    public static async Task<LoopbackHost> StartLoopbackAsync(Action<DataHubBuilder>? configureHub = null, Action<IServiceCollection>? configureServices = null)
    {
        var host = new HostBuilder()
            .ConfigureWebHost(webBuilder => Configure(webBuilder.UseKestrel().UseUrls("http://127.0.0.1:0"), configureHub, configureServices))
            .Build();

        await host.StartAsync();

        var baseUrl = host.Services.GetRequiredService<IServer>()
            .Features.Get<IServerAddressesFeature>()!.Addresses.First();
        return new LoopbackHost(host, baseUrl);
    }

    private static void Configure(
        IWebHostBuilder webBuilder,
        Action<DataHubBuilder>? configureHub,
        Action<IServiceCollection>? configureServices = null,
        Action<IEndpointRouteBuilder>? mapHub = null)
    {
        webBuilder
            .ConfigureServices(services =>
            {
                services.AddLogging(logging => logging.SetMinimumLevel(LogLevel.Warning));

                services.AddAuthentication(Scheme)
                        .AddScheme<AuthenticationSchemeOptions, StubAuthHandler>(Scheme, _ => { });
                services.AddAuthorization();

                var builder = services.AddDataHub();
                configureHub?.Invoke(builder);
                builder.Build();
                configureServices?.Invoke(services);
            })
            .Configure(app =>
            {
                app.UseRouting();
                app.UseAuthentication();
                app.UseAuthorization();
                app.UseEndpoints(mapHub ?? (endpoints => endpoints.MapDataHub(requireAuth: true)));
            });
    }

    private sealed class StubAuthHandler : AuthenticationHandler<AuthenticationSchemeOptions>
    {
        public StubAuthHandler(
            IOptionsMonitor<AuthenticationSchemeOptions> options,
            ILoggerFactory logger,
            UrlEncoder encoder)
            : base(options, logger, encoder)
        {
        }

        protected override Task<AuthenticateResult> HandleAuthenticateAsync()
        {
            var header = Request.Headers.Authorization.ToString();
            string? credential = null;
            foreach (var prefix in new[] { AuthenticatedHubServerFactory.Scheme + " ", "Bearer " })
            {
                if (header.StartsWith(prefix, StringComparison.Ordinal))
                {
                    credential = header[prefix.Length..];
                    break;
                }
            }

            if (credential == null)
            {
                return Task.FromResult(AuthenticateResult.NoResult());
            }

            // A JWT-shaped credential carries its own claims. Anything else is the subject itself,
            // the form the tests that need nothing more than an attested caller use.
            var claims = credential.Count(c => c == '.') >= 2
                ? ReadToken(credential)
                : ReadSubject(credential);

            if (claims == null)
            {
                return Task.FromResult(AuthenticateResult.Fail("The token is expired."));
            }

            var identity = new ClaimsIdentity(claims, AuthenticatedHubServerFactory.Scheme);
            var ticket = new AuthenticationTicket(new ClaimsPrincipal(identity), AuthenticatedHubServerFactory.Scheme);
            return Task.FromResult(AuthenticateResult.Success(ticket));
        }

        private static List<Claim> ReadSubject(string subject)
        {
            var claims = new List<Claim> { new("scope", "transportr:group:" + AuthenticatedHubServerFactory.DefaultGroup) };
            if (subject.Length > 0)
            {
                claims.Add(new Claim("sub", subject));
            }

            return claims;
        }

        /// <returns>The token's claims, or <c>null</c> when it has expired.</returns>
        private static List<Claim>? ReadToken(string token)
        {
            using var payload = JsonDocument.Parse(StubJwt.DecodePayload(token));
            var root = payload.RootElement;

            var claims = new List<Claim>();
            if (root.TryGetProperty("exp", out var exp))
            {
                if (exp.GetInt64() <= DateTimeOffset.UtcNow.ToUnixTimeSeconds())
                {
                    return null;
                }

                claims.Add(new Claim("exp", exp.GetInt64().ToString()));
            }

            if (root.TryGetProperty("sub", out var sub))
            {
                claims.Add(new Claim("sub", sub.GetString()!));
            }

            // One claim per scope, which is what the JwtBearer handler makes of a scope array.
            if (root.TryGetProperty("scope", out var scopes))
            {
                claims.AddRange(scopes.EnumerateArray().Select(s => new Claim("scope", s.GetString()!)));
            }

            return claims;
        }
    }
}

/// <summary>
/// Unsigned tokens in the JWT layout, which is what a client reads its own expiry from
/// (<c>JwtHelper.ExtractExpiry</c>) and what the stub scheme reads its claims from.
/// </summary>
public static class StubJwt
{
    public static string Create(string subject, DateTime expiresUtc, params string[] groups)
    {
        var payload = JsonSerializer.Serialize(new Dictionary<string, object>
        {
            ["sub"] = subject,
            ["exp"] = new DateTimeOffset(expiresUtc, TimeSpan.Zero).ToUnixTimeSeconds(),
            ["scope"] = groups.Select(g => "transportr:group:" + g).ToArray(),
        });

        return $"{Encode("{\"alg\":\"none\",\"typ\":\"JWT\"}")}.{Encode(payload)}.signature";
    }

    /// <summary>
    /// The same token with a segment too many: the stub still reads it, and
    /// <c>JwtHelper.ExtractExpiry</c> cannot, which is what an encrypted token looks like to a client.
    /// </summary>
    public static string CreateUnreadableByClients(string subject, DateTime expiresUtc, params string[] groups) =>
        Create(subject, expiresUtc, groups) + ".encrypted";

    internal static string DecodePayload(string token)
    {
        var payload = token.Split('.')[1].Replace('-', '+').Replace('_', '/');
        payload = payload.PadRight(payload.Length + (4 - payload.Length % 4) % 4, '=');
        return Encoding.UTF8.GetString(Convert.FromBase64String(payload));
    }

    private static string Encode(string json) =>
        Convert.ToBase64String(Encoding.UTF8.GetBytes(json)).TrimEnd('=').Replace('+', '-').Replace('/', '_');
}
