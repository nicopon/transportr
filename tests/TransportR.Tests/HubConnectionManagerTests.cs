using Microsoft.AspNetCore.TestHost;
using TransportR.Client.SignalR.Internal;
using TransportR.Client.SignalR.Configuration;
using TransportR.Tests.Infrastructure;
using Microsoft.Extensions.Logging.Abstractions;

namespace TransportR.Tests;

public class HubConnectionManagerTests
{
    [Fact]
    public async Task AddRegistrationCallback_ComposesWithInitialize_RatherThanReplacingIt()
    {
        var options = new ClientOptions();
        await using var manager = new HubConnectionManager("http://unused", options, NullLogger.Instance);

        var calls = new List<string>();
        manager.AddRegistrationCallback(_ => calls.Add("app"));
        manager.Initialize(_ => calls.Add("transportr"));

        // A single field, overwritten by whichever call came last, is exactly the bug this
        // guards: the application's own registration and TransportR's handshake registration
        // (SignalRTransferSignaling) both have to end up on the connection.
        Assert.Equal(["app", "transportr"], calls);
    }

    [Fact]
    public async Task HandleAffinityRedirectAsync_ReappliesEveryRegisteredCallback()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        var options = new ClientOptions();
        await using var manager = new HubConnectionManager(
            TestHubServerFactory.HubUrl,
            options,
            NullLogger.Instance,
            configureHttpConnection: o => o.HttpMessageHandlerFactory = _ => handler);

        var appCalls = 0;
        var transportRCalls = 0;
        manager.AddRegistrationCallback(_ => appCalls++);
        manager.Initialize(_ => transportRCalls++);

        Assert.Equal(1, appCalls);
        Assert.Equal(1, transportRCalls);

        await manager.HandleAffinityRedirectAsync("test-cookie", "token-1", () => Task.CompletedTask);

        Assert.Equal(2, appCalls);
        Assert.Equal(2, transportRCalls);
    }

    [Fact]
    public async Task GetAccessTokenAsync_ShouldCacheToken()
    {
        // Arrange
        var options = new ClientOptions();
        int callCount = 0;
        Func<Task<string>> provider = () => {
            callCount++;
            return Task.FromResult($"token_{callCount}");
        };
        
        await using var manager = new HubConnectionManager("http://hub", options, NullLogger.Instance, initialAccessToken: null, tokenProvider: provider);
        
        // Act 1: Fetch (initial)
        var token1 = await manager.GetAccessTokenAsync(forceRefresh: false);
        
        // Assert 1
        Assert.Equal("token_1", token1);
        Assert.Equal(1, callCount);
        
        // Act 2: Fetch again (should be cached)
        var token2 = await manager.GetAccessTokenAsync(forceRefresh: false);
        
        // Assert 2
        Assert.Equal("token_1", token2);
        Assert.Equal(1, callCount); // Still 1
    }
    
    [Fact]
    public async Task GetAccessTokenAsync_ShouldForceRefresh()
    {
        // Arrange
        var options = new ClientOptions();
        int callCount = 0;
        Func<Task<string>> provider = () => {
            callCount++;
            return Task.FromResult($"token_{callCount}");
        };
        
        await using var manager = new HubConnectionManager("http://hub", options, NullLogger.Instance, initialAccessToken: null, tokenProvider: provider);
        
        // Fill cache
        await manager.GetAccessTokenAsync(false);
        Assert.Equal(1, callCount);
        
        // Act: Force Refresh
        var token = await manager.GetAccessTokenAsync(forceRefresh: true);
        
        // Assert
        Assert.Equal("token_2", token);
        Assert.Equal(2, callCount);
    }

    [Fact]
    public async Task DisposeAsync_CalledTwice_IsNotAnError()
    {
        var manager = new HubConnectionManager("http://unused", new ClientOptions(), NullLogger.Instance);

        await manager.DisposeAsync();

        // The convention is the sessions' (SessionDisposalTests): a second release is a defensive
        // call, not a caller error. The second Cancel() on the disposed lifetime source threw.
        await manager.DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_AfterConnecting_CalledTwice_IsNotAnError()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        var manager = new HubConnectionManager(
            TestHubServerFactory.HubUrl,
            new ClientOptions(),
            NullLogger.Instance,
            configureHttpConnection: o => o.HttpMessageHandlerFactory = _ => handler);
        manager.Initialize(_ => { });
        await manager.Connection.StartAsync();

        await manager.DisposeAsync();
        await manager.DisposeAsync();
    }
}
