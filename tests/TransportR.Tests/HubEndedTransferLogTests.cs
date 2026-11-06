using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using TransportR.Abstractions;
using TransportR.Client.Configuration;
using TransportR.Interfaces;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// What the hub says about the streams of a transfer it has just ended itself. The client cuts them
/// when it is told to cancel, which the hub sees as a rupture it should expect: a warning that the
/// client "should reconnect", for a transfer nobody is going to resume, is read by an operator as
/// a fault of the network. On a socket, because how a cut request reaches the handler is under test.
/// </summary>
public class HubEndedTransferLogTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    // How often a transfer ended by the host left a warning at the hub, measured on a loopback
    // socket before the fix: 41 of 45 with the sender writing, 35 of 45 with the receiver stuck.
    // Twelve transfers on one hub leave a clean run with a chance under 0.1%.
    private const int Transfers = 12;

    [Fact]
    public async Task ATransferTheHostEnds_WhileItsSenderIsWriting_IsNotWarnedAboutByTheHub()
    {
        var hubLogs = new LogRecorder();
        await using var host = await StartHubAsync(hubLogs);

        var ids = new List<string>();
        foreach (var batch in Enumerable.Range(0, Transfers).Chunk(4))
        {
            ids.AddRange(await Task.WhenAll(batch.Select(_ => EndATransferWhileItIsWritingAsync(host, hubLogs))));
        }

        AssertNoWarning(hubLogs, ids);
    }

    [Fact]
    public async Task ATransferTheHostEnds_WhileItsReceiverIsStuck_IsNotWarnedAboutByTheHub()
    {
        var hubLogs = new LogRecorder();
        await using var host = await StartHubAsync(hubLogs);

        var ids = new List<string>();
        foreach (var batch in Enumerable.Range(0, Transfers).Chunk(4))
        {
            ids.AddRange(await Task.WhenAll(batch.Select(_ => EndATransferWithAStuckReceiverAsync(host, hubLogs))));
        }

        AssertNoWarning(hubLogs, ids);
    }

    private static void AssertNoWarning(LogRecorder hubLogs, List<string> transferIds)
    {
        var warnings = hubLogs.MessagesFrom(LogLevel.Warning)
            .Where(m => transferIds.Any(id => m.Contains(id, StringComparison.Ordinal)))
            .Select(m => Regex.Replace(m, "[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}", "<id>"))
            .GroupBy(m => m)
            .Select(g => $"{g.Count()}x {g.Key}")
            .ToList();

        Assert.True(warnings.Count == 0,
            $"The hub warned about {transferIds.Count} transfers it had itself ended:{Environment.NewLine}{string.Join(Environment.NewLine, warnings)}");
    }

    private static async Task<string> EndATransferWhileItIsWritingAsync(LoopbackHost host, LogRecorder hubLogs)
    {
        await using var sender = TestClients.Create(null, host.BaseUrl);
        await using var receiver = TestClients.Create(null, host.BaseUrl);

        var handle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += r => { handle.TrySetResult(r); return Task.CompletedTask; };
        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 10);
        var receive = await handle.Task.WaitAsync(Bound);

        var received = 0;
        var consumer = Task.Run(async () =>
        {
            try { await foreach (var _ in receive.ReceiveAsync()) Interlocked.Increment(ref received); }
            catch (TransferFailedException) { }
        });
        var producer = Task.Run(async () =>
        {
            try
            {
                for (var i = 0; ; i++)
                {
                    await send.SendAsync($"item-{i}");
                    await Task.Delay(1);
                }
            }
            catch (TransferFailedException) { }
        });

        // The sender is mid-body when the host ends the transfer.
        while (Volatile.Read(ref received) < 100) await Task.Delay(10);

        await host.Services.GetRequiredService<ITransferTerminator>().TerminateAsync(send.TransferId);
        await Task.WhenAll(consumer, producer).WaitAsync(Bound);
        await UntilTheHubHasSeenTheStreamsEndAsync(hubLogs, send.TransferId);
        return send.TransferId;
    }

    private static async Task<string> EndATransferWithAStuckReceiverAsync(LoopbackHost host, LogRecorder hubLogs)
    {
        // A window of one batch, and a receive buffer of one item that nobody drains: the second item
        // parks the receiver's read loop and the third parks the sender.
        await using var sender = TestClients.Create(null, host.BaseUrl, configure: o => o.MaxInFlightBatches = 1);
        await using var receiver = TestClients.Create(null, host.BaseUrl, configure: o => o.ReceiveBufferCapacity = BufferCapacity.Items(1));

        var handle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += r => { handle.TrySetResult(r); return Task.CompletedTask; };
        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 1);
        var receive = await handle.Task.WaitAsync(Bound);

        await send.SendAsync("item-1");
        await send.SendAsync("item-2").WaitAsync(Bound);
        var parked = send.SendAsync("item-3");
        await Task.Delay(100);
        Assert.False(parked.IsCompleted, "item-3 should be parked on a full window.");

        await host.Services.GetRequiredService<ITransferTerminator>().TerminateAsync(send.TransferId);

        await Assert.ThrowsAsync<TransferFailedException>(() => parked.WaitAsync(Bound));
        await Assert.ThrowsAsync<TransferFailedException>(async () =>
        {
            await foreach (var _ in receive.ReceiveAsync()) { }
        }).WaitAsync(Bound);
        await UntilTheHubHasSeenTheStreamsEndAsync(hubLogs, send.TransferId);
        return send.TransferId;
    }

    /// <summary>
    /// A test that asserts the absence of a warning is green when the stream has not ended yet, so it
    /// waits for the line that says it did. The sender's data stream has to end; its control stream
    /// is given a moment more and may not (it is the receiver's, and cut on its own schedule).
    /// </summary>
    private static async Task UntilTheHubHasSeenTheStreamsEndAsync(LogRecorder hubLogs, string transferId)
    {
        await UntilAsync(() => SourceEnded(hubLogs, $"{transferId}:Data"), TimeSpan.FromSeconds(5),
            $"the hub never logged the end of the data stream of {transferId}");

        var control = DateTime.UtcNow + TimeSpan.FromSeconds(2);
        while (DateTime.UtcNow < control && !SourceEnded(hubLogs, $"{transferId}:Control"))
        {
            await Task.Delay(20);
        }
    }

    private static bool SourceEnded(LogRecorder hubLogs, string bridgeKey) =>
        hubLogs.MessagesFrom(LogLevel.Information).Any(m =>
            m.Contains(bridgeKey, StringComparison.Ordinal)
            && (m.Contains("Sender stream", StringComparison.Ordinal)
                || m.Contains("Source Disconnected", StringComparison.Ordinal)
                || m.Contains("Source cancelled", StringComparison.Ordinal)));

    private static async Task UntilAsync(Func<bool> condition, TimeSpan within, string failure)
    {
        var deadline = DateTime.UtcNow + within;
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, failure);
            await Task.Delay(20);
        }
    }

    private static Task<LoopbackHost> StartHubAsync(LogRecorder hubLogs) =>
        TestHubServerFactory.StartLoopbackAsync(services => services.AddLogging(logging =>
        {
            logging.ClearProviders();
            logging.AddProvider(hubLogs);
            logging.SetMinimumLevel(LogLevel.Information);
        }));
}
