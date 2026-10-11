using System.Net;
using TransportR.Client.SignalR.Sessions;
using TransportR.Protocol;
using TransportR.Tests.TestHelpers;

namespace TransportR.Tests;

/// <summary>
/// CompleteAsync returns on the acknowledgement. The clean end of the data POST, which is how the hub
/// learns that the transfer is over and forgets it, comes after, so a sender released straight away
/// must not cancel the request before the hub has read it to its end.
/// </summary>
public class CompletedSessionReleaseTests
{
    [Fact]
    public async Task DisposeAsync_AfterCompleteAsync_WaitsForTheDataPostToEnd()
    {
        var hub = new AcknowledgingHub();
        var session = new DataSession<string>(
            new StubSessionContext(new HttpClient(hub)), Guid.NewGuid().ToString(), batchSize: 10, dataUrl: "http://test/data");
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
