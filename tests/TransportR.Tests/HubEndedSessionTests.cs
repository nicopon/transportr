using System.Net;
using Microsoft.Extensions.Logging;
using TransportR.Abstractions;
using TransportR.Client.Configuration;
using TransportR.Client.Sessions;
using TransportR.Models;
using TransportR.Protocol;
using TransportR.Tests.Infrastructure;
using TransportR.Tests.TestHelpers;

namespace TransportR.Tests;

/// <summary>
/// When the hub cancels a transfer, neither end released anything. The session fails with the hub as
/// the reason, tells the peer nothing (the hub told it), and keeps whatever outcome or reason it
/// already had.
/// </summary>
public class HubEndedSessionTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task EndByHubAsync_OnAStreamingReceiver_FailsItWithTheHubAsTheReason()
    {
        var session = NewSession(new SilentHub(), out _);
        await session.StartReceiverAsync();

        await session.EndByHubAsync();

        Assert.Equal(SessionState.Failed, session.State);
        var failure = await Assert.ThrowsAsync<TransferFailedException>(() => session.WaitForCompletionAsync(CancellationToken.None).WaitAsync(Bound));
        Assert.Contains("hub ended", failure.Message);
        await Assert.ThrowsAsync<TransferFailedException>(async () =>
        {
            await foreach (var _ in session.ReceiveAsync(CancellationToken.None)) { }
        });
    }

    [Fact]
    public async Task EndByHubAsync_OnASenderParkedOnItsWindow_ReleasesItWithTheHubAsTheReason()
    {
        var session = NewSession(new SilentHub(), out _, o => o.MaxInFlightBatches = 1);
        await session.StartSenderAsync();
        await session.SendAsync("a", CancellationToken.None);
        var parked = session.SendAsync("b", CancellationToken.None);
        await Task.Delay(200);
        Assert.False(parked.IsCompleted);

        await session.EndByHubAsync();

        var failure = await Assert.ThrowsAsync<TransferFailedException>(() => parked.WaitAsync(Bound));
        Assert.Contains("hub ended", failure.Message);
    }

    [Fact]
    public async Task EndByHubAsync_OnASenderWaitingForTheCompleteAck_ReleasesItAtOnce()
    {
        // The wait for the acknowledgement is a retry interval long (5 s), and nothing woke it when the
        // session stopped: the caller learned the hub had ended the transfer at the next tick, 2.6 to
        // 4.0 s later in the three passes measured.
        var session = NewSession(new SilentHub(), out _);
        await session.StartSenderAsync();
        await session.SendAsync("a", CancellationToken.None);
        var complete = session.CompleteAsync(CancellationToken.None);
        await Task.Delay(300);
        Assert.False(complete.IsCompleted, "CompleteAsync should be waiting for an acknowledgement that never comes.");

        var clock = System.Diagnostics.Stopwatch.StartNew();
        await session.EndByHubAsync();
        var failure = await Assert.ThrowsAsync<TransferFailedException>(() => complete.WaitAsync(TimeSpan.FromSeconds(10)));
        clock.Stop();

        Assert.Contains("hub ended", failure.Message);
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(1), $"CompleteAsync took {clock.Elapsed} to report that the hub ended the transfer.");
    }

    [Fact]
    public async Task EndByHubAsync_TellsThePeerNothing_AndDoesNotSayTheTransferWasAbandoned()
    {
        var hub = new SilentHub();
        var session = NewSession(hub, out var logs);
        await session.StartSenderAsync();
        await session.SendAsync("a", CancellationToken.None);
        await Task.Delay(200);

        await session.EndByHubAsync();

        Assert.DoesNotContain(ChunkType.Abort, await hub.ChunkTypesWrittenAsync());
        Assert.False(logs.Has(LogLevel.Warning, "abandoned"), "The hub ended the transfer; the session did not abandon it.");
    }

    [Fact]
    public async Task DisposeAsync_ByTheHost_StillTellsThePeerAndSaysTheTransferWasAbandoned()
    {
        // What EndByHubAsync leaves out is what a release does: the contrast keeps the two from drifting together.
        var hub = new SilentHub();
        var session = NewSession(hub, out var logs);
        await session.StartSenderAsync();
        await session.SendAsync("a", CancellationToken.None);
        await Task.Delay(200);

        await session.DisposeAsync();

        Assert.Contains(ChunkType.Abort, await hub.ChunkTypesWrittenAsync());
        Assert.True(logs.Has(LogLevel.Warning, "abandoned"));
    }

    [Fact]
    public async Task EndByHubAsync_AfterTheStreamFailedForGood_KeepsThatReason()
    {
        var hub = new ForgetfulHub();
        var session = NewSession(hub, out _);
        await session.StartReceiverAsync();
        hub.Forget();
        await Assert.ThrowsAsync<TransferFailedException>(() => session.WaitForCompletionAsync(CancellationToken.None).WaitAsync(Bound));

        await session.EndByHubAsync();

        var failure = await Assert.ThrowsAsync<TransferFailedException>(() => session.WaitForCompletionAsync(CancellationToken.None).WaitAsync(Bound));
        Assert.DoesNotContain("hub ended", failure.Message);
        Assert.Contains("data stream failed", failure.Message);
    }

    [Fact]
    public async Task EndByHubAsync_OnAReceiverParkedOnItsFullBuffer_LogsNoStreamFailureAndNoReconnection()
    {
        // Failing the session releases the read loop parked on the buffer, and the loop ends with a
        // closed-channel error before the disposal that follows has cancelled its stream: 30 of 30
        // sessions here, 36 of 45 on a loopback socket.
        var session = NewSession(new ReceivingHub(batches: 2), out var logs,
            o => o.ReceiveBufferCapacity = BufferCapacity.Items(1));
        await session.StartReceiverAsync();

        // The first item is admitted; the second chunk is then read and waits for room.
        var deadline = DateTime.UtcNow + Bound;
        while (session.ItemsProcessed < 1 && DateTime.UtcNow < deadline) await Task.Delay(10);
        Assert.Equal(1, session.ItemsProcessed);
        await Task.Delay(200);

        await session.EndByHubAsync();

        Assert.False(logs.Has(LogLevel.Warning, "Retryable"), "A transfer the hub ended was logged as a stream to retry.");
        Assert.False(logs.Has(LogLevel.Warning, "Reconnecting"), "A transfer the hub ended was logged as a session reconnecting.");
    }

    private static DataSession<string> NewSession(
        HttpMessageHandler hub, out LogRecorder logs, Action<TransportR.Client.Configuration.ClientOptions>? configure = null)
    {
        logs = new LogRecorder();
        var context = new StubSessionContext(new HttpClient(hub), logs.Factory.CreateLogger("session"));
        // Long enough that reaching the stall bound would be a hang, not a verdict.
        context.Options.CompleteStallTimeout = TimeSpan.FromMinutes(5);
        configure?.Invoke(context.Options);
        return new DataSession<string>(context, Guid.NewGuid().ToString(), batchSize: 1, dataUrl: "http://test/data", affinity: null);
    }

    /// <summary>Takes the sender's data POST and keeps what it wrote; answers nothing else.</summary>
    private sealed class SilentHub : HttpMessageHandler
    {
        private readonly MemoryStream _body = new();

        public async Task<List<ChunkType>> ChunkTypesWrittenAsync()
        {
            var types = new List<ChunkType>();
            using var reader = new MemoryStream(_body.ToArray());
            while (await ChunkIO.ReadChunkAsync(reader) is { } chunk)
            {
                using (chunk) types.Add(chunk.Header.Type);
            }

            return types;
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request.Method == HttpMethod.Post)
            {
                try { await request.Content!.CopyToAsync(_body, cancellationToken); }
                catch (OperationCanceledException) { }
                catch (IOException) { }
            }

            await Task.Delay(Timeout.Infinite, cancellationToken);
            throw new OperationCanceledException(cancellationToken);
        }
    }

    /// <summary>Holds every request until told to forget the transfer, then answers 404 to all of them.</summary>
    private sealed class ForgetfulHub : HttpMessageHandler
    {
        private readonly TaskCompletionSource _forgotten = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Forget() => _forgotten.TrySetResult();

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            await _forgotten.Task.WaitAsync(cancellationToken);
            return HubResponse.With(HttpStatusCode.NotFound);
        }
    }
}
