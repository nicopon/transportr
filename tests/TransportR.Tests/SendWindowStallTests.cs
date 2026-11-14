using TransportR.Client;
using TransportR.Client.Batching;
using TransportR.Client.Internal;
using TransportR.Client.Sessions;
using TransportR.Models;
using TransportR.Tests.TestHelpers;

namespace TransportR.Tests;

/// <summary>
/// A producer parked on a full send window is released only by an acknowledgement. A receiver that
/// stopped early sends none, so without a bound the producer waits forever — measured from dtpipe,
/// where a consumer run with a row limit left its producer hanging until it was killed.
/// </summary>
public class SendWindowStallTests
{
    [Fact]
    public async Task SendAsync_ParkedWithNothingAcknowledged_FailsOnceTheStallTimeoutElapses()
    {
        var context = new StubSessionContext(new HttpClient(new NeverAnswers()));
        context.Options.MaxInFlightBatches = 1;
        context.Options.CompleteStallTimeout = TimeSpan.FromMilliseconds(200);

        await using var session = new TestSession(
            context, Guid.NewGuid().ToString(), batchSize: 1, dataUrl: "http://test/data", affinity: null);
        await session.StartSenderAsync();

        await session.SendAsync("a", CancellationToken.None);
        var parked = session.SendAsync("b", CancellationToken.None);

        var failure = await Assert.ThrowsAsync<TransferFailedException>(
            () => parked.WaitAsync(TimeSpan.FromSeconds(10)));

        Assert.Equal(session.TransferId, failure.TransferId);
        Assert.Equal(SessionState.Failed, session.State);
    }

    [Fact]
    public async Task CompleteAsync_LastBatchParkedWithNothingAcknowledged_FailsOnceTheStallTimeoutElapses()
    {
        // CompleteAsync hands the accumulator's partial batch off first, and that batch needs a slot
        // like any other: the producer parked on the window is CompleteAsync itself.
        var context = new StubSessionContext(new HttpClient(new NeverAnswers()));
        context.Options.MaxInFlightBatches = 1;
        context.Options.CompleteStallTimeout = TimeSpan.FromMilliseconds(200);

        await using var session = new TestSession(
            context, Guid.NewGuid().ToString(), batchSize: 2, dataUrl: "http://test/data", affinity: null);
        await session.StartSenderAsync();

        await session.SendAsync("a", CancellationToken.None);
        await session.SendAsync("b", CancellationToken.None);
        await session.SendAsync("c", CancellationToken.None);

        var failure = await Assert.ThrowsAsync<TransferFailedException>(
            () => session.CompleteAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10)));

        Assert.Equal(session.TransferId, failure.TransferId);
        Assert.Equal(SessionState.Failed, session.State);
    }

    [Fact]
    public async Task SendAsync_ParkedAndCancelled_DoesNotReportTheTransferAsFailed()
    {
        var context = new StubSessionContext(new HttpClient(new NeverAnswers()));
        context.Options.MaxInFlightBatches = 1;
        context.Options.CompleteStallTimeout = TimeSpan.FromMinutes(5);

        await using var session = new TestSession(
            context, Guid.NewGuid().ToString(), batchSize: 1, dataUrl: "http://test/data", affinity: null);
        await session.StartSenderAsync();

        await session.SendAsync("a", CancellationToken.None);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => session.SendAsync("b", cts.Token));

        // A caller who gave up says nothing about the receiver, which may still be draining.
        Assert.NotEqual(SessionState.Failed, session.State);
    }

    [Fact]
    public async Task WriteAsync_CancelledWhileItsAddStillReadsTheBuffer_ReturnsOnlyOnceTheAddIsDoneWithIt()
    {
        var context = new StubSessionContext(new HttpClient(new NeverAnswers()));
        context.Options.CompleteStallTimeout = TimeSpan.FromMinutes(5);

        await using var session = new DataSession(
            context, Guid.NewGuid().ToString(), batchSize: 1, dataUrl: "http://test/data", affinity: null,
            new ByteBatchSource(4, context.Options.BatchFlushIntervalMs, context.StreamManager),
            new ByteReceivePort(context.Options.ReceiveBufferCapacity).OpenSink);
        await session.StartSenderAsync();

        // Past its wait for a slot an add copies whatever its token says: one granted its slot as the
        // caller cancels reads the caller's buffer after the cancellation, and stops at its next wait.
        var granted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        byte[]? copied = null;
        async Task AddAsync(ReadOnlyMemory<byte> bytes, CancellationToken token)
        {
            await granted.Task;
            copied = bytes.ToArray();
            token.ThrowIfCancellationRequested();
        }

        var send = new HttpByteSend(session, AddAsync);
        byte[] buffer = [1, 2, 3, 4];
        using var cts = new CancellationTokenSource();

        var write = send.WriteAsync(buffer, cts.Token);
        // The caller takes its buffer back the moment the call returns.
        var reused = write.ContinueWith(_ => Array.Clear(buffer), TaskContinuationOptions.ExecuteSynchronously);

        await cts.CancelAsync();
        await Task.WhenAny(write, Task.Delay(300));
        granted.SetResult();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => write.WaitAsync(TimeSpan.FromSeconds(5)));
        await reused.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal([1, 2, 3, 4], copied);
    }

    /// <summary>Keeps the session loop waiting on its streams, so nothing else changes its state.</summary>
    private sealed class NeverAnswers : HttpMessageHandler
    {
        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
            throw new OperationCanceledException(cancellationToken);
        }
    }
}
