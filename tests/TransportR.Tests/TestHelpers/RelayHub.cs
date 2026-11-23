using System.Net;
using System.Threading.Channels;
using TransportR.Client.Protocol;
using TransportR.Protocol;

namespace TransportR.Tests.TestHelpers;

/// <summary>A chunk as one end wrote it, header and payload, cut out of the segments that carried it.</summary>
/// <param name="FromSender">Written on the sender's data stream, rather than the receiver's control stream.</param>
/// <param name="Epoch">How many write streams that end opened before this one: 0 for its first.</param>
internal sealed record WireChunk(bool FromSender, int Epoch, ChunkHeader Header, byte[] Bytes)
{
    public ReadOnlySpan<byte> Payload => Bytes.AsSpan(ChunkHeader.Size);
}

/// <summary>
/// The hub between a sending and a receiving session of one process, over segments: each chunk of the
/// sender's data stream goes to the receiver's data GET, each chunk of the receiver's control stream to
/// the sender's control GET. It records what each end wrote, and a test can rewrite or drop a chunk on
/// its way, as a hub that modifies the streams would.
/// </summary>
/// <remarks>
/// A GET opened again reads on from the same relay, so a session that reconnects finds what its peer
/// wrote since. The end of the sender's data stream ends the receiver's data GET, and a data GET opened
/// after it is answered 410; the sender's control GET stays open until its session lets go of it.
/// </remarks>
internal sealed class RelayHub : HttpMessageHandler
{
    private readonly Relay _data = new(fromSender: true);
    private readonly Relay _control = new(fromSender: false);

    /// <summary>
    /// Given each chunk as its end wrote it, returns the bytes to relay in its place: none drops it.
    /// Null relays every chunk as written. Runs on the request that carried the chunk.
    /// </summary>
    public Func<WireChunk, IEnumerable<byte[]>>? Rewrite { get; set; }

    public IReadOnlyList<WireChunk> SenderWrote => _data.Written;
    public IReadOnlyList<WireChunk> ReceiverWrote => _control.Written;

    /// <summary>Relays a chunk no end wrote, as a hub that forges one would: to the sender's control GET, or the receiver's data GET.</summary>
    public void Inject(bool toSender, byte[] chunk) => (toSender ? _control : _data).Deliver(chunk);

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var path = request.RequestUri!.AbsolutePath;
        var relay = request.Headers.TryGetValues(DataPlane.ConnectionTypeHeader, out var types) && types.Single() == "Control"
            ? _control
            : _data;

        if (request.Method == HttpMethod.Post && path.EndsWith($"/{WireRoutes.SendSegment}"))
        {
            var body = await request.Content!.ReadAsByteArrayAsync(cancellationToken);
            relay.Accept(Segments.Epoch(request), body, Rewrite);
            if (Segments.IsFinal(request) && relay == _data) relay.End();
            return Segments.Answer(HttpStatusCode.OK, Segments.Offset(request) + body.Length);
        }

        if (request.Method == HttpMethod.Get && path.EndsWith($"/{WireRoutes.ReceiveSegment}"))
        {
            // As the hub answers a data stream whose source has finished.
            if (relay.Ended) return new HttpResponseMessage(HttpStatusCode.Gone);

            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StreamContent(new RelayedStream(relay.Delivered)) };
        }

        if (request.Method == HttpMethod.Post && path.EndsWith($"/{WireRoutes.AbortSegment}"))
        {
            _data.End();
            _control.End();
            return new HttpResponseMessage(HttpStatusCode.NoContent);
        }

        return new HttpResponseMessage(HttpStatusCode.NotFound);
    }

    private sealed class Relay(bool fromSender)
    {
        private readonly object _gate = new();
        private readonly List<WireChunk> _written = [];
        private readonly Dictionary<Guid, (int Epoch, List<byte> Pending)> _epochs = [];
        private readonly Channel<byte[]> _delivered = Channel.CreateUnbounded<byte[]>();

        public ChannelReader<byte[]> Delivered => _delivered.Reader;

        public IReadOnlyList<WireChunk> Written
        {
            get { lock (_gate) return [.. _written]; }
        }

        public void Accept(Guid epoch, byte[] body, Func<WireChunk, IEnumerable<byte[]>>? rewrite)
        {
            lock (_gate)
            {
                if (!_epochs.TryGetValue(epoch, out var stream))
                {
                    stream = (_epochs.Count, []);
                    _epochs[epoch] = stream;
                }

                var pending = stream.Pending;
                pending.AddRange(body);
                while (pending.Count >= ChunkHeader.Size)
                {
                    var header = ChunkHeader.ReadFrom(pending.GetRange(0, ChunkHeader.Size).ToArray());
                    var length = ChunkHeader.Size + header.PayloadLength;
                    if (pending.Count < length) break;

                    var chunk = new WireChunk(fromSender, stream.Epoch, header, pending.GetRange(0, length).ToArray());
                    pending.RemoveRange(0, length);
                    _written.Add(chunk);

                    foreach (var relayed in rewrite?.Invoke(chunk) ?? [chunk.Bytes])
                    {
                        _delivered.Writer.TryWrite(relayed);
                    }
                }
            }
        }

        public bool Ended { get; private set; }

        public void Deliver(byte[] chunk) => _delivered.Writer.TryWrite(chunk);

        public void End()
        {
            Ended = true;
            _delivered.Writer.TryComplete();
        }
    }

    /// <summary>The relayed chunks, one after the other, until the relay ends.</summary>
    private sealed class RelayedStream(ChannelReader<byte[]> chunks) : Stream
    {
        private byte[] _current = [];
        private int _offset;

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            while (_offset >= _current.Length)
            {
                if (!await chunks.WaitToReadAsync(cancellationToken)) return 0;
                if (chunks.TryRead(out var next))
                {
                    _current = next;
                    _offset = 0;
                }
            }

            var count = Math.Min(buffer.Length, _current.Length - _offset);
            _current.AsMemory(_offset, count).CopyTo(buffer);
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
