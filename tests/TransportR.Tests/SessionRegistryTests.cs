using TransportR.Client.Internal;
using TransportR.Client.Sessions;
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
            _stubContext.NewTransferSession(id, 100, "http://dummy", affinity: null)); 
            
        Assert.NotNull(session);
        Assert.Equal(transferId, session.Session.TransferId);
        Assert.Equal(1, registry.Count);
    }
    
    [Fact]
    public async Task TryGetSession_ShouldReturnExisting()
    {
        await using var registry = new SessionRegistry<string>(_stubContext.Logger);
        var transferId = "t2";
        
        registry.GetOrAdd(transferId, id => _stubContext.NewTransferSession(id, 100, "http://dummy", affinity: null));
        
        var found = registry.TryGetSession(transferId, out var session);
        
        Assert.True(found);
        Assert.NotNull(session);
        Assert.Equal(transferId, session.Session.TransferId);
    }
    
    [Fact]
    public async Task TryRemove_ShouldRemoveSession()
    {
        await using var registry = new SessionRegistry<string>(_stubContext.Logger);
        var transferId = "t3";
        registry.GetOrAdd(transferId, id => _stubContext.NewTransferSession(id, 100, "http://dummy", affinity: null));
        
        var removed = registry.TryRemove(transferId, out var session);
        
        Assert.True(removed);
        Assert.NotNull(session);
        Assert.Equal(0, registry.Count);
    }
    
    [Fact]
    public async Task DisposeAsync_ShouldDisposeSessions()
    {
        var registry = new SessionRegistry<string>(_stubContext.Logger);
        registry.GetOrAdd("s1", id => _stubContext.NewTransferSession(id, 100, "http://dummy", affinity: null));
        registry.GetOrAdd("s2", id => _stubContext.NewTransferSession(id, 100, "http://dummy", affinity: null));
        
        await registry.DisposeAsync();
        
        // Disposing SessionRegistry calls DisposeAsync on all sessions.
        // DataSession.DisposeAsync calls Context.RemoveSession.
        // So RemoveSessionStub should be called twice.
        
        Assert.Equal(2, _stubContext.RemoveSessionCount);
        Assert.Equal(0, registry.Count);
    }
}
