using System.Security.Claims;
using Microsoft.Extensions.Logging;
using TransportR.Hub;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// Groups read from the <c>scope</c> claim, in both shapes an issuer may give it.
/// </summary>
public class JwtIdentityProviderTests
{
    private static ClaimsPrincipal Principal(params Claim[] claims)
        => new(new ClaimsIdentity([new Claim("sub", "client-a"), .. claims], authenticationType: "Bearer"));

    private static IReadOnlySet<string> GroupsOf(ClaimsPrincipal user)
        => new JwtIdentityProvider().ResolveIdentity(user, providedClientId: null, providedGroups: null)!.Groups;

    [Fact]
    public void A_Space_Delimited_Scope_Yields_One_Group_Per_Scope()
    {
        // The shape OpenIddict issues: one claim, every scope in it. Read whole, this was a single
        // group named "senders extra".
        var groups = GroupsOf(Principal(new Claim("scope", "transportr:group:senders transportr:group:extra")));

        Assert.Equal(new HashSet<string> { "senders", "extra" }, groups);
    }

    [Fact]
    public void Scopes_Of_Other_Applications_Do_Not_Hide_The_Groups()
    {
        // Read whole, a value that did not start with the prefix yielded no group at all.
        var groups = GroupsOf(Principal(new Claim("scope", "openid profile transportr:group:senders")));

        Assert.Equal(new HashSet<string> { "senders" }, groups);
    }

    [Fact]
    public void A_Scope_Array_Yields_One_Group_Per_Element()
    {
        // The shape the JwtBearer handler makes of a JSON array: one claim per element.
        var groups = GroupsOf(Principal(
            new Claim("scope", "transportr:group:senders"),
            new Claim("scope", "transportr:group:receivers")));

        Assert.Equal(new HashSet<string> { "senders", "receivers" }, groups);
    }

    [Fact]
    public void A_Prefix_Elsewhere_Than_At_The_Start_Is_Not_A_Group()
    {
        var groups = GroupsOf(Principal(new Claim("scope", "other:transportr:group:senders")));

        Assert.Empty(groups);
    }

    [Fact]
    public void The_Resolved_Identity_Is_Logged_At_Debug_Only()
    {
        // Resolved by every control request and every segment: at Information it was a line per request.
        var logs = new LogRecorder();
        var provider = new JwtIdentityProvider(logs.Factory.CreateLogger<JwtIdentityProvider>());

        provider.ResolveIdentity(Principal(new Claim("scope", "transportr:group:senders")), providedClientId: null, providedGroups: null);

        Assert.True(logs.Has(LogLevel.Debug, "Resolved identity"));
        Assert.DoesNotContain(logs.MessagesFrom(LogLevel.Information), message => message.Contains("Resolved identity"));
    }
}
