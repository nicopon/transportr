using Xunit;
using TransportR.Client.SignalR.Internal;
using TransportR.Client.SignalR.Configuration;
using Microsoft.Extensions.Logging.Abstractions;

namespace TransportR.Tests;

public class HubConnectionManagerTests
{
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
}
