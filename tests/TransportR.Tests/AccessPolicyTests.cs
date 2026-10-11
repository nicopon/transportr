using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using TransportR.Hub.SignalR.Access;
using TransportR.Hub.SignalR.Configuration;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// The one decision every surface of the hub asks. Time is the policy's own clock, so that an
/// expiry is a matter of moving it.
/// </summary>
public class AccessPolicyTests
{
    private static readonly DateTimeOffset Start = new(2030, 1, 1, 12, 0, 0, TimeSpan.Zero);

    private sealed class Rules(Func<IReadOnlySet<string>, IReadOnlySet<string>, bool> canSendTo) : IFlowControlService
    {
        public bool CanSendTo(IReadOnlySet<string> sourceGroups, IReadOnlySet<string> targetGroups) =>
            canSendTo(sourceGroups, targetGroups);
    }

    private static AccessPolicy PolicyAt(
        FakeTimeProvider clock, int marginSeconds = 0, IFlowControlService? flowControl = null) =>
        new(Options.Create(new AccessOptions { TokenExpiryMarginSeconds = marginSeconds }), clock, flowControl);

    private static ClientRegistration Client(DateTimeOffset? expiry, params string[] groups) =>
        new(Guid.NewGuid(), "connection", groups.ToHashSet(), null, expiry?.UtcDateTime, Start.UtcDateTime, Subject: "subject");

    [Fact]
    public void Token_IsGoodUntilItsExpiry_AndNotAfter()
    {
        var clock = new FakeTimeProvider(Start);
        var policy = PolicyAt(clock);
        var client = Client(Start.AddSeconds(10), "g");

        Assert.Equal(AccessVerdict.Allowed, policy.Evaluate(client));

        clock.Advance(TimeSpan.FromSeconds(10));
        Assert.Equal(AccessVerdict.Allowed, policy.Evaluate(client));

        clock.Advance(TimeSpan.FromMilliseconds(1));
        Assert.Equal(AccessVerdict.TokenExpired, policy.Evaluate(client));
    }

    [Fact]
    public void Margin_DelaysTheVerdictByExactlyItsLength()
    {
        var clock = new FakeTimeProvider(Start);
        var policy = PolicyAt(clock, marginSeconds: 30);
        var client = Client(Start, "g");

        clock.Advance(TimeSpan.FromSeconds(30));
        Assert.Equal(AccessVerdict.Allowed, policy.Evaluate(client));

        clock.Advance(TimeSpan.FromMilliseconds(1));
        Assert.Equal(AccessVerdict.TokenExpired, policy.Evaluate(client));
    }

    [Fact]
    public void TokenWithoutExpiry_NeverExpires()
    {
        var clock = new FakeTimeProvider(Start);
        var client = Client(expiry: null, "g");

        clock.Advance(TimeSpan.FromDays(3650));

        Assert.Equal(AccessVerdict.Allowed, PolicyAt(clock).Evaluate(client));
    }

    [Fact]
    public void ClientWithoutGroups_HoldsNoRight_WhetherOrNotAServiceDecides()
    {
        var clock = new FakeTimeProvider(Start);
        var client = Client(Start.AddHours(1));

        Assert.Equal(AccessVerdict.NoGroups, PolicyAt(clock).Evaluate(client));
        Assert.Equal(AccessVerdict.NoGroups, PolicyAt(clock, flowControl: new Rules((_, _) => true)).Evaluate(client));
    }

    [Fact]
    public void ExpiredToken_IsReportedBeforeMissingGroups()
    {
        var clock = new FakeTimeProvider(Start);
        var client = Client(Start.AddSeconds(-1));

        Assert.Equal(AccessVerdict.TokenExpired, PolicyAt(clock).Evaluate(client));
    }

    [Fact]
    public void Pair_WithoutAFlowControlService_IsAllowedWhenBothAreGood()
    {
        var clock = new FakeTimeProvider(Start);

        var verdict = PolicyAt(clock).Evaluate(Client(Start.AddHours(1), "a"), Client(Start.AddHours(1), "b"));

        Assert.Equal(AccessVerdict.Allowed, verdict);
    }

    [Fact]
    public void Pair_FollowsTheFlowControlService()
    {
        var clock = new FakeTimeProvider(Start);
        var policy = PolicyAt(clock, flowControl: new Rules((source, _) => source.Contains("senders")));

        Assert.Equal(AccessVerdict.Allowed,
            policy.Evaluate(Client(Start.AddHours(1), "senders"), Client(Start.AddHours(1), "receivers")));
        Assert.Equal(AccessVerdict.NotAuthorized,
            policy.Evaluate(Client(Start.AddHours(1), "receivers"), Client(Start.AddHours(1), "senders")));
    }

    [Fact]
    public void Pair_NamesTheFirstThingThatIsWrong_SourceBeforeTarget()
    {
        var clock = new FakeTimeProvider(Start);
        var policy = PolicyAt(clock, flowControl: new Rules((_, _) => false));
        var good = Client(Start.AddHours(1), "g");

        Assert.Equal(AccessVerdict.TokenExpired, policy.Evaluate(Client(Start.AddSeconds(-1), "g"), good));
        Assert.Equal(AccessVerdict.TokenExpired, policy.Evaluate(good, Client(Start.AddSeconds(-1), "g")));
        Assert.Equal(AccessVerdict.NoGroups, policy.Evaluate(good, Client(Start.AddHours(1))));
        Assert.Equal(AccessVerdict.NotAuthorized, policy.Evaluate(good, Client(Start.AddHours(1), "g")));
    }

    [Fact]
    public void Discovery_KeepsATargetWhoseTokenHasExpired_ButNotOneWithoutGroups()
    {
        var clock = new FakeTimeProvider(Start);
        var policy = PolicyAt(clock);
        var caller = Client(Start.AddHours(1), "g");

        Assert.Equal(AccessVerdict.Allowed, policy.MayReach(caller, Client(Start.AddSeconds(-1), "g")));
        Assert.Equal(AccessVerdict.NoGroups, policy.MayReach(caller, Client(Start.AddHours(1))));
    }

    [Theory]
    [InlineData("a", "a", AccessVerdict.Allowed)]
    [InlineData("a", "b", AccessVerdict.WrongSubject)]
    [InlineData("a", "A", AccessVerdict.WrongSubject)]
    [InlineData(null, null, AccessVerdict.Allowed)]
    [InlineData(null, "a", AccessVerdict.WrongSubject)]
    [InlineData("a", null, AccessVerdict.WrongSubject)]
    public void Identities_AreTheSameOnlyWhenTheSubjectsAreEqual(string? registered, string? presented, AccessVerdict expected)
    {
        Assert.Equal(expected, AccessPolicy.SameIdentity(registered, presented));
    }

    [Fact]
    public void NegativeMargin_IsRefusedWhenTheHostStarts()
    {
        var failure = Record.Exception(() => AuthenticatedHubServerFactory.CreateHost(
            hub => hub.Services.Configure<AccessOptions>(options => options.TokenExpiryMarginSeconds = -1)));

        Assert.NotNull(failure);
        Assert.Contains("TokenExpiryMarginSeconds", failure.ToString());
    }
}
