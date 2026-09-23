using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Services;
using Xunit;

namespace TransportR.Tests;

public class TransferTerminatorTests
{
    private readonly InMemoryStateStore _stateStore = new();
    private readonly TransferManager _transferManager;
    private readonly BridgeManager _bridgeManager = new(NullLoggerFactory.Instance);
    private readonly RecordingNotifier _notifier = new();
    private readonly TransferTerminator _terminator;

    public TransferTerminatorTests()
    {
        _transferManager = new TransferManager(_stateStore);
        _terminator = new TransferTerminator(
            _transferManager, _bridgeManager, _stateStore, _notifier, NullLogger<TransferTerminator>.Instance);
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

        var dataKey = IBridgeManager.KeyFor(transferId, ConnectionType.Data);
        var controlKey = IBridgeManager.KeyFor(transferId, ConnectionType.Control);
        _bridgeManager.GetOrCreate(dataKey);
        _bridgeManager.GetOrCreate(controlKey);

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
