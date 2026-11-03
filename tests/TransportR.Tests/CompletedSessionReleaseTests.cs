using System.Net;
using TransportR.Client.Sessions;
using TransportR.Models;
using TransportR.Protocol;
using TransportR.Tests.TestHelpers;

namespace TransportR.Tests;

/// <summary>
/// CompleteAsync returns on the acknowledgement. The clean end of the data POST, which is how the hub
/// learns that the transfer is over and forgets it, comes after, so a sender released straight away
/// must not cancel the request before the hub has read it to its end. A receiver completes earlier
/// still, when the hub has read its acknowledgement; released straight away, it must not close its
/// data GET before the hub ends it, which would cut the sender's POST before the sender has read the
/// acknowledgement.
/// </summary>
public class CompletedSessionReleaseTests
{
    [Fact]
    public async Task DisposeAsync_AfterCompleteAsync_WaitsForTheDataPostToEnd()
    {
        var hub = new AcknowledgingHub();
        var session = new DataSession<string>(
            new StubSessionContext(new HttpClient(hub)), Guid.NewGuid().ToString(), batchSize: 10, dataUrl: "http://test/data", affinity: null);
        await session.StartSenderAsync();

        await session.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(10));

        var release = session.DisposeAsync().AsTask();

        // The hub answers the POST only after it has read the body, and it has not answered yet: a
        // release that cancels the streams instead of waiting for that is over by now.
        await Task.Delay(300);
        Assert.False(release.IsCompleted, "the session was released before the hub had answered the end of the data stream");

        hub.AnswerThePost();
        await release.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.True(hub.PostBodyEnded, "the data POST was cancelled before its body ended");
    }

    [Fact]
    public async Task DisposeAsync_OfACompletedReceiver_WaitsForTheHubToEndTheDataGet()
    {
        var hub = new CompletingHub();
        var session = new DataSession<string>(
            new StubSessionContext(new HttpClient(hub)), Guid.NewGuid().ToString(), batchSize: 10, dataUrl: "http://test/data", affinity: null);
        await session.StartReceiverAsync();

        // The enumeration ends on the outcome, which is the hub answering the control POST.
        await DrainAsync(session).WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(SessionState.Completed, session.State);

        var release = session.DisposeAsync().AsTask();

        await Task.Delay(300);
        Assert.False(hub.DataGetClosedBeforeItsEnd, "the receiver closed its data GET before the hub ended it");
        Assert.False(release.IsCompleted, "the session was released before the hub had ended the data GET");

        hub.EndTheDataGet();
        await release.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.False(hub.DataGetClosedBeforeItsEnd, "the receiver closed its data GET before the hub ended it");
    }

    [Fact]
    public async Task DisposeAsync_OfACompletedReceiver_GivesUpOnADataGetTheHubNeverEnds()
    {
        // The sender died after reading its acknowledgement: its POST never ends cleanly, and the hub
        // never ends the receiver's GET.
        var hub = new CompletingHub();
        var session = new DataSession<string>(
            new StubSessionContext(new HttpClient(hub)), Guid.NewGuid().ToString(), batchSize: 10, dataUrl: "http://test/data", affinity: null);
        await session.StartReceiverAsync();
        await DrainAsync(session).WaitAsync(TimeSpan.FromSeconds(10));

        await session.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(SessionState.Completed, session.State);
    }

    private static async Task DrainAsync(DataSession<string> session)
    {
        await foreach (var _ in session.ReceiveAsync(CancellationToken.None)) { }
    }

    /// <summary>
    /// The hub as a receiver meets it at the end of a transfer: the data GET carries a Complete for a
    /// transfer of no batch, then stays open until the test ends it, as the hub does once the sender's
    /// data POST has ended; the control POST is read to its end and answered.
    /// </summary>
    private sealed class CompletingHub : HttpMessageHandler
    {
        private readonly TaskCompletionSource _end = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private volatile bool _closedBeforeItsEnd;

        public bool DataGetClosedBeforeItsEnd => _closedBeforeItsEnd;

        public void EndTheDataGet() => _end.TrySetResult();

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request.Method == HttpMethod.Post)
            {
                await request.Content!.CopyToAsync(Stream.Null, cancellationToken);
                return new HttpResponseMessage(HttpStatusCode.OK);
            }

            var transferId = Guid.Parse(request.RequestUri!.Segments[^2].TrimEnd('/'));
            using var wire = new MemoryStream();
            await ChunkIO.WriteControlAsync(wire, transferId, -1, ChunkType.Complete, cancellationToken);
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StreamContent(new ThenOpenUntil(wire.ToArray(), _end.Task, () => _closedBeforeItsEnd = true)),
            };
        }
    }

    /// <summary>The given bytes, then nothing until <paramref name="end"/> completes, then the end of the stream.</summary>
    private sealed class ThenOpenUntil(byte[] bytes, Task end, Action closedBeforeItsEnd) : Stream
    {
        private int _offset;

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (_offset >= bytes.Length)
            {
                await end.WaitAsync(cancellationToken);
                return 0;
            }

            var count = Math.Min(buffer.Length, bytes.Length - _offset);
            bytes.AsMemory(_offset, count).CopyTo(buffer);
            _offset += count;
            return count;
        }

        protected override void Dispose(bool disposing)
        {
            if (!end.IsCompleted) closedBeforeItsEnd();
            base.Dispose(disposing);
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

    /// <summary>
    /// Reads the sender's data POST to its end and answers it when told to, which is what the hub does
    /// with the end of a stream; and acknowledges the completion on the control GET.
    /// </summary>
    private sealed class AcknowledgingHub : HttpMessageHandler
    {
        private readonly TaskCompletionSource _answer = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public volatile bool PostBodyEnded;

        public void AnswerThePost() => _answer.TrySetResult();

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request.Method == HttpMethod.Post)
            {
                await request.Content!.CopyToAsync(Stream.Null, cancellationToken);
                PostBodyEnded = true;
                await _answer.Task.WaitAsync(cancellationToken);
                return new HttpResponseMessage(HttpStatusCode.OK);
            }

            var transferId = Guid.Parse(request.RequestUri!.Segments[^2].TrimEnd('/'));
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StreamContent(new RepeatedAck(transferId)) };
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
