using System.Net;
using TransportR.Abstractions;
using TransportR.Client.Configuration;
using TransportR.Client.Sessions;
using TransportR.Protocol;
using TransportR.Tests.TestHelpers;

namespace TransportR.Tests;

/// <summary>
/// A sender that gives up on its own stall bound ends its data stream cleanly, and a clean end is
/// what the hub relays to the receiver as a finished transfer. The Abort chunk is what tells the
/// receiver otherwise, so it has to be the last thing on the stream and it has to reach the wire.
/// </summary>
public class SenderFailureAbortTests
{
    [Fact]
    public async Task SendAsync_ParkedPastTheStallTimeout_EndsTheDataStreamWithAnAbort()
    {
        var wire = new CapturedDataStream();
        await using var session = await StartedSenderAsync(wire, batchSize: 1, o => o.MaxInFlightBatches = 1);

        await session.SendAsync("a", CancellationToken.None);
        await Assert.ThrowsAsync<TransferFailedException>(
            () => session.SendAsync("b", CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10)));

        await wire.Ended.WaitAsync(TimeSpan.FromSeconds(10));
        await AssertEndsWithAbortAsync(wire);
    }

    [Fact]
    public async Task CompleteAsync_NeverAcknowledged_EndsTheDataStreamWithAnAbort()
    {
        var wire = new CapturedDataStream();
        await using var session = await StartedSenderAsync(wire, batchSize: 10);

        await Assert.ThrowsAsync<TransferFailedException>(
            () => session.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(10)));

        await wire.Ended.WaitAsync(TimeSpan.FromSeconds(10));
        await AssertEndsWithAbortAsync(wire);
    }

    [Fact]
    public async Task DisposeAsync_AfterAStallFailure_LetsTheAbortReachTheWire()
    {
        // Every write takes longer than the gap between the failure and the disposal that follows
        // it, so an Abort that is only enqueued is cancelled before it is written.
        var wire = new CapturedDataStream(writeDelay: TimeSpan.FromMilliseconds(300));
        var session = await StartedSenderAsync(wire, batchSize: 1, o => o.MaxInFlightBatches = 1);

        await session.SendAsync("a", CancellationToken.None);
        await Assert.ThrowsAsync<TransferFailedException>(
            () => session.SendAsync("b", CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10)));

        await session.DisposeAsync();

        await AssertEndsWithAbortAsync(wire);
    }

    private static async Task<TestSession> StartedSenderAsync(
        CapturedDataStream wire, int batchSize, Action<ClientOptions>? configure = null)
    {
        var context = new StubSessionContext(new HttpClient(wire));
        context.Options.CompleteStallTimeout = TimeSpan.FromMilliseconds(200);
        configure?.Invoke(context.Options);

        var session = new TestSession(
            context, Guid.NewGuid().ToString(), batchSize, dataUrl: "http://test/data", affinity: null);
        await session.StartSenderAsync();
        return session;
    }

    private static async Task AssertEndsWithAbortAsync(CapturedDataStream wire)
    {
        var chunks = await wire.ChunksAsync();
        Assert.NotEmpty(chunks);

        var last = chunks[^1];
        Assert.Equal(ChunkType.Abort, last.Type);
        Assert.Equal((int)AbortReason.AcknowledgementTimeout, last.BatchId);
    }

    /// <summary>
    /// Relays every segment of the sender's data stream, which is what the hub does while the receiver
    /// reads, and never answers the control GET, which keeps the session loop waiting on its streams.
    /// </summary>
    private sealed class CapturedDataStream : HttpMessageHandler
    {
        private readonly MemoryStream _body = new();
        private readonly TimeSpan _writeDelay;
        private readonly TaskCompletionSource _ended = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public CapturedDataStream(TimeSpan writeDelay = default) => _writeDelay = writeDelay;

        /// <summary>Completes on the last segment, that is on a clean close.</summary>
        public Task Ended => _ended.Task;

        public async Task<List<(ChunkType Type, int BatchId)>> ChunksAsync()
        {
            var chunks = new List<(ChunkType, int)>();
            using var reader = new MemoryStream(_body.ToArray());
            while (await ChunkIO.ReadChunkAsync(reader) is { } chunk)
            {
                using (chunk)
                {
                    chunks.Add((chunk.Header.Type, chunk.Header.BatchId));
                }
            }

            return chunks;
        }

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request.Method != HttpMethod.Post)
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
                throw new OperationCanceledException(cancellationToken);
            }

            var response = await Segments.RelayAsync(request, new SlowSink(_body, _writeDelay, cancellationToken), cancellationToken);
            if (Segments.IsFinal(request)) _ended.TrySetResult();
            return response;
        }
    }

    /// <summary>A write-only stream that takes its time and gives up when its token is cancelled.</summary>
    private sealed class SlowSink : Stream
    {
        private readonly Stream _inner;
        private readonly TimeSpan _delay;
        private readonly CancellationToken _requestToken;

        public SlowSink(Stream inner, TimeSpan delay, CancellationToken requestToken)
        {
            _inner = inner;
            _delay = delay;
            _requestToken = requestToken;
        }

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _requestToken);
            if (_delay > TimeSpan.Zero)
            {
                await Task.Delay(_delay, linked.Token);
            }

            await _inner.WriteAsync(buffer, linked.Token);
        }
    }
}
