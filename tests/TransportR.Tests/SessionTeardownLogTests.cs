using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging;
using TransportR.Abstractions;
using TransportR.Client.SignalR.Configuration;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// Letting a session go while its streams are up is how every transfer a host abandons, or a hub
/// cancels, ends on this side. The cancelled streams come back as a normal end, and what the session
/// does with that must not read as a fault in the log: the warning and the error below are the two
/// lines a host sees on every teardown.
/// </summary>
public class SessionTeardownLogTests
{
    private const string RefusedTransition = "Invalid transition";
    private const string LoopError = "Session Loop Error";

    [Fact]
    public async Task ReleasingAStreamingSession_LogsNoRefusedTransition()
    {
        var logs = await ReleaseStreamingSessionsAsync(times: 3);

        Assert.False(logs.Has(LogLevel.Warning, RefusedTransition), "Releasing a streaming session logged a refused state transition.");
    }

    [Fact]
    public async Task ReleasingAStreamingSession_LogsNoLoopError()
    {
        // The error depends on where the session loop is when the disposal cancels its token. It appeared on every release
        // measured on b9038be (60 of 60, TestServer); the repeat keeps one release that misses the window from passing.
        var logs = await ReleaseStreamingSessionsAsync(times: 12);

        Assert.False(logs.Has(LogLevel.Error, LoopError), "Releasing a streaming session logged a session loop error.");
    }

    private static async Task<LogRecorder> ReleaseStreamingSessionsAsync(int times)
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();
        var logs = new LogRecorder();

        await using var sender = TestClients.Create(handler, "http://localhost", logger: logs.Factory.CreateLogger("sender"));
        await using var receiver = TestClients.Create(handler, "http://localhost");

        var received = new System.Collections.Concurrent.ConcurrentQueue<Receive<string>>();
        receiver.OnTransferStarted += r => { received.Enqueue(r); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        for (var i = 0; i < times; i++)
        {
            var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 1);
            await send.SendAsync("item");
            await Task.Delay(300);

            await send.DisposeAsync();
            await Task.Delay(300);
        }

        foreach (var receive in received) await receive.DisposeAsync();
        return logs;
    }

    [Fact]
    public async Task ASenderThatGaveUp_LogsNoRefusedTransitionWhenItsStreamsEnd()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();
        var logs = new LogRecorder();

        await using var sender = TestClients.Create(handler, "http://localhost", logger: logs.Factory.CreateLogger("sender"),
            configure: o => { o.MaxInFlightBatches = 1; o.CompleteStallTimeout = TimeSpan.FromMilliseconds(500); });
        await using var receiver = TestClients.Create(handler, "http://localhost",
            configure: o => o.ReceiveBufferCapacity = BufferCapacity.Items(1));

        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += r => { receiveHandle.TrySetResult(r); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 1);
        await using var receive = await receiveHandle.Task;

        // Nobody enumerates: the receiver holds the second item, and the third parks the sender on its window until it gives up.
        await send.SendAsync("item-1");
        await send.SendAsync("item-2");
        await Assert.ThrowsAsync<TransferFailedException>(() => send.SendAsync("item-3").WaitAsync(TimeSpan.FromSeconds(10)));

        await send.DisposeAsync();
        await Task.Delay(500);

        Assert.False(logs.Has(LogLevel.Warning, RefusedTransition), "A session that failed logged a refused state transition when its streams ended.");
    }
}
