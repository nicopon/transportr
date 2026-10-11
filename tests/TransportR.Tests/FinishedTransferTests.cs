using Microsoft.Extensions.DependencyInjection;
using TransportR.Abstractions;
using TransportR.Client.SignalR.Configuration;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// What the hub still holds of a transfer that ended well. The ending is a data-plane exchange, so
/// no message tells the hub the transfer is over, and nothing on that path removes it: until a
/// sweep happens to judge it, the access reconciler keeps judging a transfer that is done, and a
/// bridge keeps its place. On a socket, because a stream's ending is what is under test.
/// </summary>
public class FinishedTransferTests
{
    [Fact]
    public async Task ATransferThatEndedWell_IsNoLongerHeldByTheHub()
    {
        await using var host = await TestHubServerFactory.StartLoopbackAsync();

        // Long enough for a heartbeat to report on it, so that the inactivity sweep knows it.
        var transferId = await TransferToTheEndAsync(host, o => o.HeartbeatInterval = TimeSpan.FromMilliseconds(100), lasting: TimeSpan.FromMilliseconds(500));

        await AssertForgottenAsync(host, transferId);
    }

    [Fact]
    public async Task ATransferThatEndedBeforeItsFirstHeartbeat_IsNoLongerHeldByTheHub()
    {
        // The inactivity sweep only knows a transfer that reported statistics, and the first report
        // comes a heartbeat after it started. A transfer shorter than that has no state for the
        // sweep to see, so nothing but a verdict on its participants could ever remove it.
        await using var host = await TestHubServerFactory.StartLoopbackAsync();

        var transferId = await TransferToTheEndAsync(host, o => o.HeartbeatInterval = TimeSpan.FromHours(1), lasting: TimeSpan.Zero);

        await AssertForgottenAsync(host, transferId);
    }

    private static async Task AssertForgottenAsync(LoopbackHost host, params string[] transferIds)
    {
        var transfers = host.Services.GetRequiredService<ITransferManager>();
        var bridges = host.Services.GetRequiredService<IBridgeManager>();

        // The inactivity sweep acts after a minute of silence; a hub that forgets the transfer when
        // it ends does so within moments.
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline && transferIds.Any(id => Held(transfers, bridges, id)))
        {
            await Task.Delay(50);
        }

        var retained = transferIds.Where(id => Held(transfers, bridges, id)).ToList();
        Assert.True(retained.Count == 0,
            $"the hub still holds {retained.Count} of {transferIds.Length} finished transfers; first: "
            + string.Join("; ", retained.Take(3).Select(id =>
                $"{id}: participants listed = {Listed(transfers, id)}, "
                + $"bridges = [{string.Join(", ", HeldBridges(bridges, id).Select(b => $"{b.Key} {b.Value.State}"))}], "
                + $"known to the inactivity sweep = {transfers.GetTransfer(id) is not null}")));
    }

    private static bool Held(ITransferManager transfers, IBridgeManager bridges, string transferId) =>
        Listed(transfers, transferId) || HeldBridges(bridges, transferId).Any();

    private static bool Listed(ITransferManager transfers, string transferId) =>
        transfers.GetAllParticipants().ContainsKey(transferId);

    private static IEnumerable<KeyValuePair<string, ConnectionBridge>> HeldBridges(IBridgeManager bridges, string transferId) =>
        bridges.Bridges.Where(b => b.Key.StartsWith(transferId + ":", StringComparison.Ordinal));

    /// <returns>The id of a transfer both ends have carried to completion and released.</returns>
    private static async Task<string> TransferToTheEndAsync(LoopbackHost host, Action<ClientOptions>? configure, TimeSpan lasting)
    {
        await using var sender = TestClients.Create(null, host.BaseUrl, configure: configure);
        await using var receiver = TestClients.Create(null, host.BaseUrl, configure: configure);

        var handle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += r => { handle.TrySetResult(r); return Task.CompletedTask; };
        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 10);
        var receive = await handle.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var consumer = Task.Run(async () =>
        {
            var items = new List<string>();
            await foreach (var item in receive.ReceiveAsync()) items.Add(item);
            await receive.WaitForCompletionAsync();
            return items;
        });

        var expected = Enumerable.Range(0, 25).Select(i => $"item-{i}").ToList();
        await send.SendAsync(expected.Take(10).ToList());
        await Task.Delay(lasting);
        await send.SendAsync(expected.Skip(10).ToList());
        await send.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(15));

        Assert.Equal(expected, await consumer.WaitAsync(TimeSpan.FromSeconds(15)));

        var transferId = send.TransferId;
        await send.DisposeAsync();
        await receive.DisposeAsync();
        return transferId;
    }
}
