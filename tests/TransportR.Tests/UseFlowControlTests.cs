using Microsoft.Extensions.DependencyInjection;
using TransportR.Hub;

namespace TransportR.Tests;

/// <summary>
/// <c>DataHubBuilder.UseFlowControl</c>: the rules a host declares reach the service that judges
/// them, what the host gets wrong is refused at the call, and a service of the host's own still wins.
/// </summary>
public class UseFlowControlTests
{
    private static IReadOnlySet<string> Groups(params string[] names) => new HashSet<string>(names);

    [Fact]
    public void TheRulesTheHostDeclares_AreTheOnesTheResolvedServiceApplies()
    {
        var services = new ServiceCollection();

        services.AddDataHub().UseFlowControl(rules =>
        {
            rules.Groups["senders"] = new GroupAccess { CanSendTo = ["receivers"] };
            rules.Groups["receivers"] = new GroupAccess { CanSendTo = [] };
        });

        using var provider = services.BuildServiceProvider();
        var flowControl = provider.GetRequiredService<IFlowControlService>();
        Assert.IsType<DefaultFlowControlService>(flowControl);
        Assert.True(flowControl.CanSendTo(Groups("senders"), Groups("receivers")));
        Assert.False(flowControl.CanSendTo(Groups("receivers"), Groups("senders")));
    }

    [Fact]
    public void TheOptionsRegistered_AreTheInstanceTheHostConfigured()
    {
        var services = new ServiceCollection();

        services.AddDataHub().UseFlowControl(rules =>
            rules.Groups["admins"] = new GroupAccess { CanSendTo = ["*"] });

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<FlowControlOptions>();
        Assert.Equal("admins", Assert.Single(options.Groups.Keys));
    }

    [Fact]
    public void ANullConfigure_IsRefused()
    {
        var builder = new ServiceCollection().AddDataHub();

        Assert.Throws<ArgumentNullException>(() => builder.UseFlowControl(null!));
    }

    [Fact]
    public void NoGroupDeclared_IsRefused_AndRegistersNothing()
    {
        var services = new ServiceCollection();
        var builder = services.AddDataHub();

        Assert.Throws<ArgumentException>(() => builder.UseFlowControl(_ => { }));

        Assert.DoesNotContain(services, d => d.ServiceType == typeof(IFlowControlService));
        Assert.DoesNotContain(services, d => d.ServiceType == typeof(FlowControlOptions));
    }

    [Fact]
    public void AGroupsDictionaryTheHostNulledOrEmptied_IsRefused()
    {
        var builder = new ServiceCollection().AddDataHub();

        Assert.Throws<ArgumentException>(() => builder.UseFlowControl(rules => rules.Groups = null!));
        Assert.Throws<ArgumentException>(() => builder.UseFlowControl(rules => rules.Groups = new()));
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t")]
    public void ABlankGroupName_IsRefused(string name)
    {
        var builder = new ServiceCollection().AddDataHub();

        Assert.Throws<ArgumentException>(() => builder.UseFlowControl(rules =>
        {
            rules.Groups["senders"] = new GroupAccess { CanSendTo = ["receivers"] };
            rules.Groups[name] = new GroupAccess { CanSendTo = ["receivers"] };
        }));
    }

    [Fact]
    public void ANullGroupAccess_IsRefused_NamingTheGroup()
    {
        var builder = new ServiceCollection().AddDataHub();

        var refused = Assert.Throws<ArgumentException>(() => builder.UseFlowControl(rules =>
        {
            rules.Groups["senders"] = new GroupAccess { CanSendTo = ["receivers"] };
            rules.Groups["ghosts"] = null!;
        }));

        Assert.Contains("ghosts", refused.Message);
        Assert.DoesNotContain("senders", refused.Message);
    }

    [Fact]
    public void ANullCanSendTo_IsRefused_NamingTheGroup()
    {
        var builder = new ServiceCollection().AddDataHub();

        var refused = Assert.Throws<ArgumentException>(() => builder.UseFlowControl(rules =>
        {
            rules.Groups["senders"] = new GroupAccess { CanSendTo = ["receivers"] };
            rules.Groups["ghosts"] = new GroupAccess { CanSendTo = null! };
        }));

        Assert.Contains("ghosts", refused.Message);
        Assert.DoesNotContain("senders", refused.Message);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void ABlankTargetInCanSendTo_IsRefused_NamingTheGroup(string? target)
    {
        var builder = new ServiceCollection().AddDataHub();

        var refused = Assert.Throws<ArgumentException>(() => builder.UseFlowControl(rules =>
        {
            rules.Groups["senders"] = new GroupAccess { CanSendTo = ["receivers"] };
            rules.Groups["relays"] = new GroupAccess { CanSendTo = ["receivers", target!] };
        }));

        Assert.Contains("relays", refused.Message);
        Assert.DoesNotContain("senders", refused.Message);
    }

    [Fact]
    public void AGroupThatSendsToNobody_IsAValidRule()
    {
        var services = new ServiceCollection();

        services.AddDataHub().UseFlowControl(rules =>
            rules.Groups["receivers"] = new GroupAccess { CanSendTo = [] });

        using var provider = services.BuildServiceProvider();
        Assert.False(provider.GetRequiredService<IFlowControlService>()
            .CanSendTo(Groups("receivers"), Groups("receivers")));
    }

    [Fact]
    public void AHostServiceRegisteredAfter_IsTheOneResolved()
    {
        var services = new ServiceCollection();

        services.AddDataHub().UseFlowControl(rules =>
            rules.Groups["senders"] = new GroupAccess { CanSendTo = ["receivers"] });
        services.AddSingleton<IFlowControlService, HostFlowControl>();

        using var provider = services.BuildServiceProvider();
        Assert.IsType<HostFlowControl>(provider.GetRequiredService<IFlowControlService>());
    }

    [Fact]
    public void AHostServiceRegisteredBefore_IsReplacedByTheRules()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IFlowControlService, HostFlowControl>();

        services.AddDataHub().UseFlowControl(rules =>
            rules.Groups["senders"] = new GroupAccess { CanSendTo = ["receivers"] });

        using var provider = services.BuildServiceProvider();
        Assert.IsType<DefaultFlowControlService>(provider.GetRequiredService<IFlowControlService>());
    }

    private sealed class HostFlowControl : IFlowControlService
    {
        public bool CanSendTo(IReadOnlySet<string> sourceGroups, IReadOnlySet<string> targetGroups) => true;
    }
}
