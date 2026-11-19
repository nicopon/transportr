using System.IO.Pipelines;
using System.Net;
using TransportR.Client;
using TransportR.Client.Batching;
using TransportR.Client.Internal;
using TransportR.Client.Protocol;
using TransportR.Client.Sessions;
using TransportR.Tests.TestHelpers;

namespace TransportR.Tests;

/// <summary>
/// A call that a failure unblocks reports the cause of that failure, whichever thread lets it go and
/// however late the call comes.
/// </summary>
public class FailureCauseTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task AnAbortReceivedWhileASendIsParked_FailsThatSend_WithTheAbortsReason()
    {
        var hub = new ScriptedHub();
        var context = new StubSessionContext(new HttpClient(hub));
        context.Options.MaxInFlightBatches = 1;
        context.Options.CompleteStallTimeout = TimeSpan.FromMinutes(5);
        var (port, receive) = context.CreatePorts(batchSize: 1);
        var source = new HeldRelease(port.Source);
        await using var session = new DataSession(
            context, Guid.NewGuid().ToString(), batchSize: 1, dataUrl: "http://test/data", affinity: null, source, receive.OpenSink);
        var send = new HttpSend<string>(session, port);
        await session.StartSenderAsync();

        // The first item takes the only slot and nobody acknowledges it, so the second parks.
        await send.SendAsync("a");
        var parked = send.SendAsync("b");
        await Task.Delay(TimeSpan.FromMilliseconds(300));
        Assert.False(parked.IsCompleted, "The send was expected to be parked before the Abort.");
        source.Released = parked;

        await hub.WriteAsync(ChunkType.Abort, (int)AbortReason.UndecodableBatch);

        var failure = await Assert.ThrowsAsync<TransferFailedException>(() => parked.WaitAsync(Bound));
        Assert.Equal(TransferFailureCategory.AbandonedByPeer, failure.Category);
        Assert.Equal(AbortReason.UndecodableBatch.Describe(), failure.Message);
    }

    [Fact]
    public async Task ACallAfterTheAcknowledgementTimeout_FailsWithWhatTheTimeoutFailedTheTransferWith()
    {
        var context = new StubSessionContext(new HttpClient(new ScriptedHub()));
        context.Options.MaxInFlightBatches = 1;
        context.Options.CompleteStallTimeout = TimeSpan.FromMilliseconds(200);
        await using var session = new TestSession(
            context, Guid.NewGuid().ToString(), batchSize: 1, dataUrl: "http://test/data", affinity: null);
        await session.StartSenderAsync();

        await session.SendAsync("a", CancellationToken.None);
        var stalled = await Assert.ThrowsAsync<TransferFailedException>(
            () => session.SendAsync("b", CancellationToken.None).WaitAsync(Bound));

        var later = await Assert.ThrowsAsync<TransferFailedException>(
            () => session.CompleteAsync(CancellationToken.None).WaitAsync(Bound));
        Assert.Equal(TransferFailureCategory.AcknowledgementTimedOut, stalled.Category);
        Assert.Equal(TransferFailureCategory.AcknowledgementTimedOut, later.Category);
        Assert.Equal(stalled.Message, later.Message);
    }

    [Fact]
    public async Task AReceiverThatAcceptedComplete_AndIsNeverClosed_FailsOnTheAcknowledgementTimeout()
    {
        var hub = new ScriptedHub();
        var context = new StubSessionContext(new HttpClient(hub));
        context.Options.CompleteStallTimeout = TimeSpan.FromMilliseconds(200);
        await using var session = new TestSession(
            context, Guid.NewGuid().ToString(), batchSize: 1, dataUrl: "http://test/data", affinity: null);
        await session.StartReceiverAsync();

        // No batch was sent, which a Complete naming batch -1 says.
        await hub.WriteAsync(ChunkType.Complete, -1);

        var failure = await Assert.ThrowsAsync<TransferFailedException>(
            () => session.WaitForCompletionAsync(CancellationToken.None).WaitAsync(Bound));
        Assert.Equal(TransferFailureCategory.AcknowledgementTimedOut, failure.Category);
        Assert.Contains("never finished closing", failure.Message);
    }

    /// <summary>
    /// The source of a sending session, whose release holds the thread that releases it until the call
    /// it let go has ended: what that call reports is then what the session knew when it let go, however
    /// the threads that follow would have run.
    /// </summary>
    private sealed class HeldRelease(IBatchSource inner) : IBatchSource
    {
        public Task? Released { get; set; }

        public ChunkType BatchType => inner.BatchType;
        public void Open(SemaphoreSlim? inFlightSlots, Action releaseSlot) => inner.Open(inFlightSlots, releaseSlot);
        public void StartFlushInterval() => inner.StartFlushInterval();
        public ValueTask<bool> WaitToReadAsync(CancellationToken ct) => inner.WaitToReadAsync(ct);
        public ValueTask<OutgoingBatch?> TryReadAsync(CancellationToken ct) => inner.TryReadAsync(ct);
        public ValueTask CompleteAsync(CancellationToken ct) => inner.CompleteAsync(ct);
        public void Detach() => inner.Detach();
        public void Dispose() => inner.Dispose();

        public void Abandon()
        {
            inner.Abandon();
            if (Released is { } call) SpinWait.SpinUntil(() => call.IsCompleted, Bound);
        }
    }

    /// <summary>
    /// A hub whose read stream carries the chunks the test writes, and stays open; a write stream is read
    /// and never answered.
    /// </summary>
    private sealed class ScriptedHub : HttpMessageHandler
    {
        private readonly TaskCompletionSource<(Guid TransferId, PipeWriter Writer)> _read =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task WriteAsync(ChunkType type, int value)
        {
            var (transferId, writer) = await _read.Task.WaitAsync(Bound);
            await ChunkIO.WriteControlAsync(writer.AsStream(), transferId, value, type);
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request.Method == HttpMethod.Post)
            {
                try { await request.Content!.CopyToAsync(Stream.Null, cancellationToken); }
                catch (OperationCanceledException) { }
                catch (IOException) { }

                await Task.Delay(Timeout.Infinite, cancellationToken);
            }

            var pipe = new Pipe();
            _read.TrySetResult((Guid.Parse(request.RequestUri!.Segments[^2].TrimEnd('/')), pipe.Writer));
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StreamContent(pipe.Reader.AsStream()) };
        }
    }
}
