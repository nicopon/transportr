using TransportR.Models;
using TransportR.Services;

namespace TransportR.Tests;

/// <summary>
/// Unit tests for TransferManager (session and participant management).
/// </summary>
public class TransferManagerTests
{
    // ========== Participant & Session Tests ==========

    [Fact]
    public async Task RegisterParticipants_ThenGetParticipants_ReturnsParticipants()
    {
        // Arrange
        var stateStore = new InMemoryStateStore();
        var manager = new TransferManager(stateStore);
        var senderId = Guid.NewGuid();
        var receiverId = Guid.NewGuid();

        // Act
        await manager.RegisterParticipantsAsync("transfer-1", senderId, receiverId);
        
        // Assert - Check Manager Memory
        var result = await manager.GetParticipantsAsync("transfer-1");
        Assert.NotNull(result);
        Assert.Equal(senderId, result.SenderId);
        Assert.Equal(receiverId, result.ReceiverId);
        
        // Assert - Check State Store (Persistence)
        var session = await stateStore.GetSessionAsync("transfer-1");
        Assert.NotNull(session);
        Assert.Equal(senderId, session.SenderId);
    }

    [Fact]
    public async Task GetParticipants_NotFound_ReturnsNull()
    {
        var manager = new TransferManager(new InMemoryStateStore());
        Assert.Null(await manager.GetParticipantsAsync("non-existent"));
    }
    
    [Fact]
    public async Task GetParticipants_FromStore_RestoresMemoryState()
    {
        // Arrange
        var stateStore = new InMemoryStateStore();
        var manager = new TransferManager(stateStore);
        var transferId = "transfer-ha";
        
        // Simulate a session existing in the store (from another node)
        // but NOT in the manager's memory
        var session = new TransferSession(transferId, Guid.NewGuid(), Guid.NewGuid(), DateTime.UtcNow, DateTime.UtcNow.AddMinutes(5));
        await stateStore.SaveSessionAsync(session);

        // Act
        // Logic: GetParticipantsAsync should check store if memory miss
        var result = await manager.GetParticipantsAsync(transferId);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(session.SenderId, result.SenderId);
        
        // Ensure it hydrated memory for subsequent calls
        // We can verify this by checking if GetTransfer returns a state object now
        Assert.NotNull(manager.GetTransfer(transferId));
    }

    // ========== Stats Tests ==========

    [Fact]
    public async Task UpdateSenderStats_CreatesTransferState()
    {
        // Arrange
        var manager = new TransferManager(new InMemoryStateStore());

        // Act
        await manager.UpdateSenderStatsAsync("transfer-s", 1000, 5);
        var state = manager.GetTransfer("transfer-s");

        // Assert
        Assert.NotNull(state);
        Assert.Equal(1000, state.ItemsSent);
        Assert.Equal(5, state.AcksReceived);
    }

    [Fact]
    public async Task UpdateReceiverStats_CreatesTransferState()
    {
        // Arrange
        var manager = new TransferManager(new InMemoryStateStore());

        // Act
        await manager.UpdateReceiverStatsAsync("transfer-r", 500, 3);
        var state = manager.GetTransfer("transfer-r");

        // Assert
        Assert.NotNull(state);
        Assert.Equal(500, state.ItemsReceived);
        Assert.Equal(3, state.AcksSent);
    }

    // ========== Cleanup Tests ==========

    [Fact]
    public async Task RemoveTransfer_ClearsMemoryAndStore()
    {
        // Arrange
        var stateStore = new InMemoryStateStore();
        var manager = new TransferManager(stateStore);
        var id = "transfer-del";
        
        await manager.RegisterParticipantsAsync(id, Guid.NewGuid(), Guid.NewGuid());
        await manager.UpdateSenderStatsAsync(id, 100, 1);

        // Act
        await manager.RemoveTransferAsync(id);

        // Assert
        Assert.Null(await manager.GetParticipantsAsync(id));
        Assert.Null(manager.GetTransfer(id));
        Assert.Null(await stateStore.GetSessionAsync(id));
    }
    
    [Fact]
    public async Task RemoveTransfer_OfATransferThatNeverReported_ClearsMemoryAndStore()
    {
        // The state a handshake that failed leaves behind: participants and the state registered with
        // them, and no statistics.
        var stateStore = new InMemoryStateStore();
        var manager = new TransferManager(stateStore);
        var id = "transfer-unreg";

        await manager.RegisterParticipantsAsync(id, Guid.NewGuid(), Guid.NewGuid());

        await manager.RemoveTransferAsync(id);

        Assert.Null(await manager.GetParticipantsAsync(id));
        Assert.Null(manager.GetTransfer(id));
        Assert.Null(await stateStore.GetSessionAsync(id));
    }

    [Fact]
    public async Task RegisterParticipants_StartsTheTransferState()
    {
        // The inactivity sweep walks the states. One that appears only with the first statistics
        // report leaves a transfer that ends sooner with nothing for the sweep to find.
        var manager = new TransferManager(new InMemoryStateStore());

        await manager.RegisterParticipantsAsync("transfer-1", Guid.NewGuid(), Guid.NewGuid());

        Assert.NotNull(manager.GetTransfer("transfer-1"));
        Assert.Single(manager.GetAllTransfers());
    }
}
