using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using TransportR.Configuration;
using TransportR.Interfaces;
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
    public async Task Track_OfATransferRegisteredElsewhere_HoldsItFromTheStore()
    {
        // A replica that takes over the streams of a transfer another one registered, after a failover.
        var stateStore = new InMemoryStateStore();
        var manager = new TransferManager(stateStore);
        var transferId = "transfer-ha";
        var session = new TransferSession(transferId, Guid.NewGuid(), Guid.NewGuid(), DateTime.UtcNow, DateTime.UtcNow.AddMinutes(5));
        await stateStore.SaveSessionAsync(session);

        var state = await manager.TrackAsync(transferId);

        Assert.NotNull(state);
        Assert.Same(state, manager.GetTransfer(transferId));
        Assert.Equal(session.SenderId, manager.GetAllParticipants()[transferId].SenderId);
    }

    // ========== Stats Tests ==========

    [Fact]
    public async Task UpdateSenderStats_RecordsOnAHeldTransfer()
    {
        var manager = new TransferManager(new InMemoryStateStore());
        await manager.RegisterParticipantsAsync("transfer-s", Guid.NewGuid(), Guid.NewGuid());

        await manager.UpdateSenderStatsAsync("transfer-s", 1000, 5);
        var state = manager.GetTransfer("transfer-s");

        Assert.NotNull(state);
        Assert.Equal(1000, state.ItemsSent);
        Assert.Equal(5, state.AcksReceived);
    }

    [Fact]
    public async Task UpdateReceiverStats_RecordsOnAHeldTransfer()
    {
        var manager = new TransferManager(new InMemoryStateStore());
        await manager.RegisterParticipantsAsync("transfer-r", Guid.NewGuid(), Guid.NewGuid());

        await manager.UpdateReceiverStatsAsync("transfer-r", 500, 3);
        var state = manager.GetTransfer("transfer-r");

        Assert.NotNull(state);
        Assert.Equal(500, state.ItemsReceived);
        Assert.Equal(3, state.AcksSent);
    }

    [Fact]
    public async Task Heartbeats_OfATransferNotHeld_StartNoState()
    {
        // A heartbeat that outlives its transfer: a state started here would be the sweep's to end.
        var manager = new TransferManager(new InMemoryStateStore());

        await manager.UpdateSenderStatsAsync("transfer-gone", 1000, 5);
        await manager.UpdateReceiverStatsAsync("transfer-gone", 500, 3);

        Assert.Empty(manager.GetAllTransfers());
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

    [Fact]
    public async Task AReplicaThatOnlyReadsATransfer_DoesNotEndItForTheSilenceThatFollows()
    {
        // A replica that refuses a misrouted request, or checks whether the hub still holds a transfer,
        // reads it from the shared store. It hears no heartbeat of a transfer it does not carry, so a
        // read that started a state there would have its inactivity sweep delete the shared session.
        var clock = new FakeTimeProvider();
        var store = new InMemoryStateStore(timeProvider: clock);
        var replicaA = new TransferManager(store, clock);
        var replicaB = new TransferManager(store, clock);
        var terminator = new SignallingTerminator(new TransferTerminator(
            replicaB, new BridgeManager(NullLoggerFactory.Instance), store, new SilentNotifier(), NullLogger<TransferTerminator>.Instance));
        var config = new DataPlaneConfiguration();
        using var cleanup = new TransferCleanupService(
            replicaB, terminator, Options.Create(config), NullLogger<TransferCleanupService>.Instance, clock);

        await replicaA.RegisterParticipantsAsync("transfer-1", Guid.NewGuid(), Guid.NewGuid());
        Assert.NotNull(await replicaB.GetParticipantsAsync("transfer-1"));

        // A transfer replica B does carry, silent as well: its end says the sweep has run past the timeout.
        await replicaB.RegisterParticipantsAsync("witness", Guid.NewGuid(), Guid.NewGuid());
        await cleanup.StartAsync(CancellationToken.None);

        var step = TimeSpan.FromSeconds(10);
        var limit = TimeSpan.FromSeconds(config.TransferInactivityTimeoutSeconds * 3);
        for (var moved = TimeSpan.Zero; moved < limit && !terminator.WitnessEnded.Task.IsCompleted; moved += step)
        {
            clock.Advance(step);
            await Task.WhenAny(terminator.WitnessEnded.Task, Task.Delay(100));
        }

        // Stopping waits for the sweep in progress, which may reach transfer-1 after the witness.
        await cleanup.StopAsync(CancellationToken.None);

        Assert.True(terminator.WitnessEnded.Task.IsCompleted, "the sweep never ran past the inactivity timeout");
        Assert.NotNull(await store.GetSessionAsync("transfer-1"));
    }

    private sealed class SignallingTerminator(ITransferTerminator inner) : ITransferTerminator
    {
        public TaskCompletionSource WitnessEnded { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task TerminateAsync(string transferId, CancellationToken cancellationToken = default)
        {
            await inner.TerminateAsync(transferId, cancellationToken);
            if (transferId == "witness") WitnessEnded.TrySetResult();
        }

        public Task RetireAsync(string transferId) => inner.RetireAsync(transferId);
    }

    private sealed class SilentNotifier : ITransferNotifier
    {
        public Task NotifyCancelTransferAsync(string connectionId, string transferId, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
    }
}
