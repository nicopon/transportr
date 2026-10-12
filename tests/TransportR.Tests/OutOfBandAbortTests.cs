using System.Net;
using TransportR.Abstractions;
using TransportR.Client.SignalR.Configuration;
using TransportR.Client.SignalR.Sessions;
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
        var hub = new Hub(dataPostConfirms: false);
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
        var hub = new Hub(dataPostConfirms: true);
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
        var hub = new Hub(dataPostConfirms: false) { AbortAnswer = answer };
        var session = await StartedSenderAsync(hub);
        await FailOnTheWindowAsync(session);

        await session.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Single(hub.AbortRequests);
    }

    [Fact]
    public async Task DisposeAsync_WhenTheHubNeverAnswersTheRequest_IsStillBounded()
    {
        var hub = new Hub(dataPostConfirms: false) { AbortAnswer = null };
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
        var hub = new Hub(dataPostConfirms: false) { DataPostFails = true };
        var session = await StartedSenderAsync(hub);
        await FailOnTheWindowAsync(session);

        await session.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Single(hub.AbortRequests);
    }

    [Fact]
    public async Task DisposeAsync_WhenTheHubSaidItHasNoSuchTransfer_DoesNotAskIt()
    {
        // The data POST is answered 404 after the sender gave up and queued its Abort: the hub refused
        // the stream, so it has no transfer left to end.
        var hub = new Hub(dataPostConfirms: false) { HoldTheDataPostForAnAnswer = true };
        var session = await StartedSenderAsync(hub);
        await FailOnTheWindowAsync(session);
        hub.AnswerTheDataPost(HttpStatusCode.NotFound);
        await Task.Delay(300);

        await session.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Empty(hub.AbortRequests);
    }

    [Fact]
    public async Task DisposeAsync_AfterACompletedTransfer_NeverAsksTheHub()
    {
        // The data POST of a completed session is waited for as well, and may take as long as the hub
        // likes to answer; that is no reason to abort a transfer that succeeded.
        var hub = new Hub(dataPostConfirms: false) { AcknowledgeComplete = true };
        var session = await StartedSenderAsync(hub, o => o.MaxInFlightBatches = null);

        await session.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(10));
        await session.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Empty(hub.AbortRequests);
    }

    private static async Task<DataSession<string>> StartedSenderAsync(Hub hub, Action<ClientOptions>? configure = null)
    {
        var context = new StubSessionContext(new HttpClient(hub));
        context.Options.CompleteStallTimeout = TimeSpan.FromMilliseconds(200);
        context.Options.MaxInFlightBatches = 1;
        configure?.Invoke(context.Options);

        var session = new DataSession<string>(context, Guid.NewGuid().ToString(), batchSize: 1, dataUrl: "http://test/data");
        await session.StartSenderAsync();
        return session;
    }

    private static async Task FailOnTheWindowAsync(DataSession<string> session)
    {
        await session.SendAsync("a", CancellationToken.None);
        await Assert.ThrowsAsync<TransferFailedException>(
            () => session.SendAsync("b", CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10)));
    }

    /// <summary>
    /// The hub as a sender meets it. Its data POST either reads the body to the end, as a hub does
    /// when the stream is not full, or reads nothing and never answers, which is a stream whose pipe
    /// is full: what is written stays queued. The control GET is never answered.
    /// </summary>
    private sealed class Hub(bool dataPostConfirms) : HttpMessageHandler
    {
        private readonly List<(HttpMethod Method, string Path, Version Version)> _abortRequests = [];

        /// <summary>How the hub answers the abort request; null means it never does.</summary>
        public HttpStatusCode? AbortAnswer { get; init; } = HttpStatusCode.NoContent;

        public bool AcknowledgeComplete { get; init; }

        /// <summary>The data POST fails the way a cut connection does.</summary>
        public bool DataPostFails { get; init; }

        /// <summary>The data POST waits for <see cref="AnswerTheDataPost"/> instead of being left pending for good.</summary>
        public bool HoldTheDataPostForAnAnswer { get; init; }

        private readonly TaskCompletionSource<HttpStatusCode> _dataPostAnswer = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void AnswerTheDataPost(HttpStatusCode status) => _dataPostAnswer.TrySetResult(status);

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
                    return new HttpResponseMessage(answer);
                }

                await Task.Delay(Timeout.Infinite, cancellationToken);
                throw new OperationCanceledException(cancellationToken);
            }

            if (request.Method == HttpMethod.Post)
            {
                if (DataPostFails)
                {
                    throw new HttpRequestException("connection reset");
                }

                if (HoldTheDataPostForAnAnswer)
                {
                    return new HttpResponseMessage(await _dataPostAnswer.Task.WaitAsync(cancellationToken));
                }

                if (dataPostConfirms)
                {
                    await request.Content!.CopyToAsync(Stream.Null, cancellationToken);
                    return new HttpResponseMessage(HttpStatusCode.OK);
                }

                // A completed session's body is read to its end and the POST then goes unanswered.
                if (AcknowledgeComplete)
                {
                    await request.Content!.CopyToAsync(Stream.Null, cancellationToken);
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
