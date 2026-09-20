using System.Security.Claims;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TransportR.Hub.SignalR;
using TransportR.Interfaces;

namespace TransportR.Tests.Infrastructure;

/// <summary>
/// A hub host with <c>MapDataHub(requireAuth: true)</c> and a stub scheme that attests whatever
/// subject the caller puts in the Authorization header.
/// </summary>
/// <remarks>
/// The stub stands in for the IdP, not for the authorization decision: the point of these tests is
/// what the hub does with an attested identity, so the token format is deliberately trivial.
/// </remarks>
public static class AuthenticatedHubServerFactory
{
    public const string Scheme = "Stub";

    public static IHost CreateHost()
    {
        return new HostBuilder()
            .ConfigureWebHost(webBuilder =>
            {
                webBuilder
                    .UseTestServer()
                    .ConfigureServices(services =>
                    {
                        services.AddLogging(logging => logging.SetMinimumLevel(LogLevel.Warning));

                        services.AddAuthentication(Scheme)
                                .AddScheme<AuthenticationSchemeOptions, StubAuthHandler>(Scheme, _ => { });
                        services.AddAuthorization();

                        services.AddSingleton<IHubProgressMonitor, SilentProgressMonitor>();
                        services.AddDataHub().Build();
                    })
                    .Configure(app =>
                    {
                        app.UseRouting();
                        app.UseAuthentication();
                        app.UseAuthorization();
                        app.UseEndpoints(endpoints => endpoints.MapDataHub(requireAuth: true));
                    });
            })
            .Start();
    }

    private sealed class SilentProgressMonitor : IHubProgressMonitor
    {
        public void InitializeTransfer(string transferId) { }

        public void OnBytesTransferred(long bytes) { }

        public void CompleteTransfer(string transferId) { }
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
            var prefix = AuthenticatedHubServerFactory.Scheme + " ";
            var header = Request.Headers.Authorization.ToString();
            if (!header.StartsWith(prefix, StringComparison.Ordinal))
            {
                return Task.FromResult(AuthenticateResult.NoResult());
            }

            var subject = header[prefix.Length..];
            var claims = new List<Claim> { new("scope", "transportr:group:test") };
            if (subject.Length > 0)
            {
                claims.Add(new Claim("sub", subject));
            }

            var identity = new ClaimsIdentity(claims, AuthenticatedHubServerFactory.Scheme);
            var ticket = new AuthenticationTicket(new ClaimsPrincipal(identity), AuthenticatedHubServerFactory.Scheme);
            return Task.FromResult(AuthenticateResult.Success(ticket));
        }
    }
}
