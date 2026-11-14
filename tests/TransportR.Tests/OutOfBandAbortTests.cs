using System.Net;
using TransportR.Abstractions;
using TransportR.Client.Configuration;
using TransportR.Client.Protocol;
using TransportR.Client.Sessions;
using TransportR.Protocol;
using TransportR.Tests.TestHelpers;

namespace TransportR.Tests;

/// <summary>
/// An <c>Abort</c> queued behind data nobody reads does not reach the wire, and the hub then keeps a
/// transfer whose sender has gone. A session that gave up and could not confirm its Abort therefore
/// says so on a request of its own - and only then: an Abort that did arrive, or a transfer that
/// simply completed, never asks the hub for anything.
/// </summary>
public class OutOfBandAbortTests
{
    // DataSession waits this long for the Abort to reach the wire before it looks for another way.
    private static readonly TimeSpan AbortWriteTimeout = TimeSpan.FromSeconds(2);

    [Fact]
    public async Task DisposeAsync_WhenTheAbortStaysBehindTheData_AsksTheHubToEndTheTransfer()
    {
        var hub = new Hub(dataSegmentsConfirm: false);
        var session = await StartedSenderAsync(hub);
        await FailOnTheWindowAsync(session);

        await session.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        var request = Assert.Single(hub.AbortRequests);
        Assert.Equal(HttpMethod.Post, request.Method);
        Assert.Equal($"/data/{session.TransferId}/{WireRoutes.AbortSegment}", request.Path);
        Assert.Equal(HttpVersion.Version11, request.Version);
    }

    [Fact]
    public async Task DisposeAsync_WhenTheAbortReachedTheWire_DoesNotAskTheHub()
    {
        var hub = new Hub(dataSegmentsConfirm: true);
        var session = await StartedSenderAsync(hub);
        await FailOnTheWindowAsync(session);

        await session.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Empty(hub.AbortRequests);
    }

    [Theory]
    [InlineData(HttpStatusCode.NotFound)]
    [InlineData(HttpStatusCode.InternalServerError)]
    public async Task DisposeAsync_WhenTheHubRefusesTheRequest_StillReleasesTheSession(HttpStatusCode answer)
    {
        var hub = new Hub(dataSegmentsConfirm: false) { AbortAnswer = answer };
        var session = await StartedSenderAsync(hub);
        await FailOnTheWindowAsync(session);

        await session.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Single(hub.AbortRequests);
    }

    [Fact]
    public async Task DisposeAsync_WhenTheHubNeverAnswersTheRequest_IsStillBounded()
    {
        var hub = new Hub(dataSegmentsConfirm: false) { AbortAnswer = null };
        var session = await StartedSenderAsync(hub);
        await FailOnTheWindowAsync(session);

        var started = DateTime.UtcNow;
        await session.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        // Two bounds in a row, the wait for the Abort and the wait for the request, and nothing more.
        Assert.True(DateTime.UtcNow - started < 2 * AbortWriteTimeout + TimeSpan.FromSeconds(2),
            "The release outlasted both bounds.");
    }

    [Fact]
    public async Task DisposeAsync_WhenTheDataStreamWasCutBeforeTheAbortWasWritten_AsksTheHub()
    {
        var hub = new Hub(dataSegmentsConfirm: false) { DataSegmentsFail = true };
        var session = await StartedSenderAsync(hub);
        await FailOnTheWindowAsync(session);

        await session.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Single(hub.AbortRequests);
    }

    [Fact]
    public async Task DisposeAsync_WhenTheHubSaidItHasNoSuchTransfer_DoesNotAskIt()
    {
        // The data stream's segment is answered 404 after the sender gave up and queued its Abort: the
        // hub refused the stream, so it has no transfer left to end.
        var hub = new Hub(dataSegmentsConfirm: false) { HoldTheDataSegmentsForAnAnswer = true };
        var session = await StartedSenderAsync(hub);
        await FailOnTheWindowAsync(session);
        hub.AnswerTheDataSegments(HttpStatusCode.NotFound);
        await Task.Delay(300);

        await session.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Empty(hub.AbortRequests);
    }

    [Fact]
    public async Task DisposeAsync_AfterACompletedTransfer_NeverAsksTheHub()
    {
        // The last segment of a completed session is waited for as well, and may take as long as the
        // hub likes to answer; that is no reason to abort a transfer that succeeded.
        var hub = new Hub(dataSegmentsConfirm: false) { AcknowledgeComplete = true };
        var session = await StartedSenderAsync(hub, o => o.MaxInFlightBatches = null);

        await session.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(10));
        await session.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Empty(hub.AbortRequests);
    }

    private static async Task<TestSession> StartedSenderAsync(Hub hub, Action<ClientOptions>? configure = null)
    {
        var context = new StubSessionContext(new HttpClient(hub));
        context.Options.CompleteStallTimeout = TimeSpan.FromMilliseconds(200);
        context.Options.MaxInFlightBatches = 1;
        configure?.Invoke(context.Options);

        var session = new TestSession(context, Guid.NewGuid().ToString(), batchSize: 1, dataUrl: "http://test/data", affinity: null);
        await session.StartSenderAsync();
        return session;
    }

    private static async Task FailOnTheWindowAsync(TestSession session)
    {
        await session.SendAsync("a", CancellationToken.None);
        await Assert.ThrowsAsync<TransferFailedException>(
            () => session.SendAsync("b", CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10)));
    }

    /// <summary>
    /// The hub as a sender meets it. The segments of its data stream are either relayed, as a hub does
    /// when the stream is not full, or never answered, which is a stream whose pipe is full: what is
    /// written stays queued. The control GET is never answered.
    /// </summary>
    private sealed class Hub(bool dataSegmentsConfirm) : HttpMessageHandler
    {
        private readonly List<(HttpMethod Method, string Path, Version Version)> _abortRequests = [];

        /// <summary>How the hub answers the abort request; null means it never does.</summary>
        public HttpStatusCode? AbortAnswer { get; init; } = HttpStatusCode.NoContent;

        public bool AcknowledgeComplete { get; init; }

        /// <summary>Every segment of the data stream fails the way a cut connection does.</summary>
        public bool DataSegmentsFail { get; init; }

        /// <summary>The data stream's segments wait for <see cref="AnswerTheDataSegments"/> instead of being left pending for good.</summary>
        public bool HoldTheDataSegmentsForAnAnswer { get; init; }

        private readonly TaskCompletionSource<HttpStatusCode> _dataSegmentsAnswer = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void AnswerTheDataSegments(HttpStatusCode status) => _dataSegmentsAnswer.TrySetResult(status);

        public IReadOnlyList<(HttpMethod Method, string Path, Version Version)> AbortRequests
        {
            get { lock (_abortRequests) return [.. _abortRequests]; }
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var path = request.RequestUri!.AbsolutePath;

            if (path.EndsWith("/" + WireRoutes.AbortSegment, StringComparison.Ordinal))
            {
                lock (_abortRequests) _abortRequests.Add((request.Method, path, request.Version));

                if (AbortAnswer is { } answer)
                {
                    return HubResponse.With(answer);
                }

                await Task.Delay(Timeout.Infinite, cancellationToken);
                throw new OperationCanceledException(cancellationToken);
            }

            if (request.Method == HttpMethod.Post)
            {
                if (DataSegmentsFail)
                {
                    throw new HttpRequestException("connection reset");
                }

                if (HoldTheDataSegmentsForAnAnswer)
                {
                    return HubResponse.With(await _dataSegmentsAnswer.Task.WaitAsync(cancellationToken));
                }

                // A completed session's segments are relayed, and the last one, which ends the stream,
                // goes unanswered.
                if (dataSegmentsConfirm || (AcknowledgeComplete && !Segments.IsFinal(request)))
                {
                    return await Segments.RelayAsync(request, Stream.Null, cancellationToken);
                }

                await Task.Delay(Timeout.Infinite, cancellationToken);
            }

            if (AcknowledgeComplete)
            {
                var transferId = Guid.Parse(request.RequestUri!.Segments[^2].TrimEnd('/'));
                return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StreamContent(new RepeatedAck(transferId)) };
            }

            await Task.Delay(Timeout.Infinite, cancellationToken);
            throw new OperationCanceledException(cancellationToken);
        }
    }

    /// <summary>
    /// A CompleteAck every so often, for as long as the stream is read: the sender asks for the
    /// acknowledgement again at intervals, and one that arrives before it has asked is dropped.
    /// </summary>
    private sealed class RepeatedAck(Guid transferId) : Stream
    {
        private byte[] _pending = [];
        private int _offset;

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (_offset >= _pending.Length)
            {
                await Task.Delay(100, cancellationToken);
                using var chunk = new MemoryStream();
                await ChunkIO.WriteControlAsync(chunk, transferId, 0, ChunkType.CompleteAck, cancellationToken);
                _pending = chunk.ToArray();
                _offset = 0;
            }

            var count = Math.Min(buffer.Length, _pending.Length - _offset);
            _pending.AsMemory(_offset, count).CopyTo(buffer);
            _offset += count;
            return count;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
