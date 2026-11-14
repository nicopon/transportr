using TransportR.Services;

namespace TransportR.Tests;

/// <summary>
/// Unit tests for BridgeManager.
/// </summary>
public class BridgeManagerTests
{
    [Fact]
    public void GetOrCreate_CreatesNewBridge()
    {
        // Arrange
        var manager = new BridgeManager(Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance);

        // Act
        var bridge = manager.GetOrCreate("transfer-1", ConnectionType.Data);

        // Assert
        Assert.NotNull(bridge);
        Assert.Equal("transfer-1:Data", bridge.BridgeKey);
    }

    [Fact]
    public void GetOrCreate_ReturnsSameBridgeForSameKey()
    {
        // Arrange
        var manager = new BridgeManager(Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance);

        // Act
        var bridge1 = manager.GetOrCreate("transfer-1", ConnectionType.Data);
        var bridge2 = manager.GetOrCreate("transfer-1", ConnectionType.Data);

        // Assert
        Assert.Same(bridge1, bridge2);
    }

    [Fact]
    public void TryRemove_RemovesBridge()
    {
        // Arrange
        var manager = new BridgeManager(Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance);
        manager.GetOrCreate("transfer-1", ConnectionType.Data);

        // Act
        var removed = manager.TryRemove("transfer-1:Data", out _);

        // Assert
        Assert.True(removed);
        Assert.Empty(manager.Bridges);
    }

    [Fact]
    public void TryRemove_ReturnsFalseForNonExistent()
    {
        // Arrange
        var manager = new BridgeManager(Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance);

        // Act
        var removed = manager.TryRemove("non-existent", out _);

        // Assert
        Assert.False(removed);
    }

    [Fact]
    public void Bridges_ReturnsAllBridges()
    {
        // Arrange
        var manager = new BridgeManager(Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance);
        manager.GetOrCreate("transfer-1", ConnectionType.Data);
        manager.GetOrCreate("transfer-2", ConnectionType.Control);

        // Act
        var bridges = manager.Bridges;

        // Assert
        Assert.Equal(2, bridges.Count);
        Assert.Contains("transfer-1:Data", bridges.Keys);
        Assert.Contains("transfer-2:Control", bridges.Keys);
    }
}
