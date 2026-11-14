using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Hub;
using TransportR.Hub.Interfaces;
using TransportR.Hub.Services;
using TransportR.Hub.Streams;

namespace TransportR.Tests;

public class TransferTerminatorTests
{
    private readonly InMemoryStateStore _stateStore = new();
    private readonly TransferManager _transferManager;
    private readonly BridgeManager _bridgeManager = new(NullLoggerFactory.Instance);
    private readonly RecordingNotifier _notifier = new();
    private readonly UploadEpochs _epochs = new(TimeProvider.System, NullLogger<UploadEpochs>.Instance);
    private readonly TransferTerminator _terminator;

    public TransferTerminatorTests()
    {
        _transferManager = new TransferManager(_stateStore);
        _terminator = new TransferTerminator(
            _transferManager, _bridgeManager, _stateStore, _notifier, _epochs, NullLogger<TransferTerminator>.Instance);
    }

    [Fact]
    public async Task TerminateAsync_RemovesStateAndBridges_AndNotifiesBothParticipants()
    {
        var senderId = Guid.NewGuid();
        var receiverId = Guid.NewGuid();
        const string transferId = "transfer-1";

        await _stateStore.SaveClientAsync(new ClientRegistration(senderId, "conn-sender", ["senders"], null, null, DateTime.UtcNow));
        await _stateStore.SaveClientAsync(new ClientRegistration(receiverId, "conn-receiver", ["receivers"], null, null, DateTime.UtcNow));
        await _transferManager.RegisterParticipantsAsync(transferId, senderId, receiverId);

        var dataKey = BridgeManager.KeyFor(transferId, ConnectionType.Data);
        var controlKey = BridgeManager.KeyFor(transferId, ConnectionType.Control);
        _bridgeManager.GetOrCreate(transferId, ConnectionType.Data);
        _bridgeManager.GetOrCreate(transferId, ConnectionType.Control);

        await _terminator.TerminateAsync(transferId);

        Assert.Null(await _transferManager.GetParticipantsAsync(transferId));
        Assert.False(_bridgeManager.Bridges.ContainsKey(dataKey));
        Assert.False(_bridgeManager.Bridges.ContainsKey(controlKey));
        Assert.Equal(["conn-sender", "conn-receiver"], _notifier.NotifiedConnectionIds);
    }

    [Fact]
    public async Task TerminateAsync_UnknownTransfer_DoesNothingAndDoesNotThrow()
    {
        await _terminator.TerminateAsync("no-such-transfer");

        Assert.Empty(_notifier.NotifiedConnectionIds);
    }

    [Fact]
    public async Task RetireAsync_RemovesStateAndBridges_AndTellsNobody()
    {
        var senderId = Guid.NewGuid();
        var receiverId = Guid.NewGuid();
        const string transferId = "transfer-1";

        await _stateStore.SaveClientAsync(new ClientRegistration(senderId, "conn-sender", ["senders"], null, null, DateTime.UtcNow));
        await _stateStore.SaveClientAsync(new ClientRegistration(receiverId, "conn-receiver", ["receivers"], null, null, DateTime.UtcNow));
        await _transferManager.RegisterParticipantsAsync(transferId, senderId, receiverId);

        var dataKey = BridgeManager.KeyFor(transferId, ConnectionType.Data);
        var controlKey = BridgeManager.KeyFor(transferId, ConnectionType.Control);
        _bridgeManager.GetOrCreate(transferId, ConnectionType.Data);
        _bridgeManager.GetOrCreate(transferId, ConnectionType.Control);

        await _terminator.RetireAsync(transferId);

        Assert.Null(await _transferManager.GetParticipantsAsync(transferId));
        Assert.Null(_transferManager.GetTransfer(transferId));
        Assert.Null(await _stateStore.GetSessionAsync(transferId));
        Assert.Empty(_bridgeManager.Bridges);
        Assert.Empty(_notifier.NotifiedConnectionIds);
    }

    [Fact]
    public async Task RetireAsync_RemovesTheStateBeforeTheBridges()
    {
        // A stream that reopens between the two must find the transfer gone. Were the bridges to go
        // first it would be handed a new one, and wait for a peer that is not coming.
        const string transferId = "transfer-1";
        var dataKey = BridgeManager.KeyFor(transferId, ConnectionType.Data);
        var bridgesWhenTheStateWasRemoved = new List<string>();
        var transfers = new TransferManagerSpy(_transferManager, () =>
            bridgesWhenTheStateWasRemoved.AddRange(_bridgeManager.Bridges.Keys));
        var terminator = new TransferTerminator(
            transfers, _bridgeManager, _stateStore, _notifier, _epochs, NullLogger<TransferTerminator>.Instance);
        await _transferManager.RegisterParticipantsAsync(transferId, Guid.NewGuid(), Guid.NewGuid());
        _bridgeManager.GetOrCreate(transferId, ConnectionType.Data);

        await terminator.RetireAsync(transferId);

        Assert.Equal([dataKey], bridgesWhenTheStateWasRemoved);
        Assert.Empty(_bridgeManager.Bridges);
    }

    [Fact]
    public async Task RetireAsync_WhenTheStateCannotBeRemoved_DoesNotThrow_AndKeepsTheBridges()
    {
        // The bridges stay, completed, and the idle sweep finds them: a retirement is not worth
        // failing the data stream's last request over.
        const string transferId = "transfer-1";
        var dataKey = BridgeManager.KeyFor(transferId, ConnectionType.Data);
        var transfers = new TransferManagerSpy(_transferManager, () => throw new IOException("the store is down"));
        var terminator = new TransferTerminator(
            transfers, _bridgeManager, _stateStore, _notifier, _epochs, NullLogger<TransferTerminator>.Instance);
        _bridgeManager.GetOrCreate(transferId, ConnectionType.Data);

        await terminator.RetireAsync(transferId);

        Assert.True(_bridgeManager.Bridges.ContainsKey(dataKey));
    }

    private sealed class TransferManagerSpy(TransferManager inner, Action onRemove) : ITransferManager
    {
        public Task RemoveTransferAsync(string transferId)
        {
            onRemove();
            return inner.RemoveTransferAsync(transferId);
        }

        public Task RegisterParticipantsAsync(string transferId, Guid senderId, Guid receiverId) => inner.RegisterParticipantsAsync(transferId, senderId, receiverId);
        public Task<TransferParticipants?> GetParticipantsAsync(string transferId) => inner.GetParticipantsAsync(transferId);
        public Task<TransferState?> TrackAsync(string transferId) => inner.TrackAsync(transferId);
        public Task UpdateSenderStatsAsync(string transferId, long itemsSent, long acksReceived) => inner.UpdateSenderStatsAsync(transferId, itemsSent, acksReceived);
        public Task UpdateReceiverStatsAsync(string transferId, long itemsReceived, long acksSent) => inner.UpdateReceiverStatsAsync(transferId, itemsReceived, acksSent);
        public IReadOnlyDictionary<string, TransferParticipants> GetAllParticipants() => inner.GetAllParticipants();
        public TransferState? GetTransfer(string transferId) => inner.GetTransfer(transferId);
        public IEnumerable<TransferState> GetAllTransfers() => inner.GetAllTransfers();
        public Task KeepAliveAsync(string transferId) => inner.KeepAliveAsync(transferId);
    }

    private class RecordingNotifier : ITransferNotifier
    {
        public List<string> NotifiedConnectionIds { get; } = new();

        public Task NotifyCancelTransferAsync(string connectionId, string transferId, CancellationToken cancellationToken = default)
        {
            NotifiedConnectionIds.Add(connectionId);
            return Task.CompletedTask;
        }
    }
}
