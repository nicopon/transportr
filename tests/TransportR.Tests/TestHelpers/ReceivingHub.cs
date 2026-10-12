using TransportR.Protocol;

namespace TransportR.Tests.TestHelpers;

/// <summary>
/// The hub as a receiving session meets it: its data GET hands out the batches it was given, one
/// item each, and then stays open; its control POST is read and never answered.
/// </summary>
internal sealed class ReceivingHub(int batches) : HttpMessageHandler
{
    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        if (request.Method == HttpMethod.Post)
        {
            try { await request.Content!.CopyToAsync(Stream.Null, cancellationToken); }
            catch (OperationCanceledException) { }
            catch (IOException) { }

            await Task.Delay(Timeout.Infinite, cancellationToken);
        }

        var transferId = Guid.Parse(request.RequestUri!.Segments[^2].TrimEnd('/'));
        using var wire = new MemoryStream();
        for (var batchId = 0; batchId < batches; batchId++)
        {
            // What the test serializer writes after the count the session prefixes: its own count, and a byte.
            byte[] body = [.. BitConverter.GetBytes(1), 0];
            await ChunkIO.WriteBatchAsync(wire, transferId, batchId, count: 1, body, cancellationToken);
        }

        return new HttpResponseMessage(System.Net.HttpStatusCode.OK) { Content = new StreamContent(new ThenSilence(wire.ToArray())) };
    }

    private sealed class ThenSilence(byte[] bytes) : Stream
    {
        private int _offset;

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (_offset >= bytes.Length)
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }

            var count = Math.Min(buffer.Length, bytes.Length - _offset);
            bytes.AsMemory(_offset, count).CopyTo(buffer);
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
