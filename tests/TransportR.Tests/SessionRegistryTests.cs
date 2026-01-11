using Xunit;
using TransportR.Client.SignalR.Internal;
using TransportR.Client.SignalR.Sessions;
using TransportR.Tests.TestHelpers;

namespace TransportR.Tests;

public class SessionRegistryTests
{
    private readonly StubSessionContext _stubContext = new();
    
    [Fact]
    public async Task GetOrAdd_ShouldAddNewSession()
    {
        await using var registry = new SessionRegistry<string>(_stubContext.Logger);
        var transferId = "t1";
        
        var session = registry.GetOrAdd(transferId, id => 
            new DataSession<string>(_stubContext, id, 100, "http://dummy")); 
            
        Assert.NotNull(session);
        Assert.Equal(transferId, session.TransferId);
        Assert.Equal(1, registry.Count);
    }
    
    [Fact]
    public async Task TryGetSession_ShouldReturnExisting()
    {
        await using var registry = new SessionRegistry<string>(_stubContext.Logger);
        var transferId = "t2";
        
        registry.GetOrAdd(transferId, id => new DataSession<string>(_stubContext, id, 100, "http://dummy"));
        
        var found = registry.TryGetSession(transferId, out var session);
        
        Assert.True(found);
        Assert.NotNull(session);
        Assert.Equal(transferId, session.TransferId);
    }
    
    [Fact]
    public async Task TryRemove_ShouldRemoveSession()
    {
        await using var registry = new SessionRegistry<string>(_stubContext.Logger);
        var transferId = "t3";
        registry.GetOrAdd(transferId, id => new DataSession<string>(_stubContext, id, 100, "http://dummy"));
        
        var removed = registry.TryRemove(transferId, out var session);
        
        Assert.True(removed);
        Assert.NotNull(session);
        Assert.Equal(0, registry.Count);
    }
    
    [Fact]
    public async Task DisposeAsync_ShouldDisposeSessions()
    {
        var registry = new SessionRegistry<string>(_stubContext.Logger);
        registry.GetOrAdd("s1", id => new DataSession<string>(_stubContext, id, 100, "http://dummy"));
        registry.GetOrAdd("s2", id => new DataSession<string>(_stubContext, id, 100, "http://dummy"));
        
        await registry.DisposeAsync();
        
        // Disposing SessionRegistry calls DisposeAsync on all sessions.
        // DataSession.DisposeAsync calls Context.RemoveSession.
        // So RemoveSessionStub should be called twice.
        
        Assert.Equal(2, _stubContext.RemoveSessionCount);
        Assert.Equal(0, registry.Count);
    }
}
