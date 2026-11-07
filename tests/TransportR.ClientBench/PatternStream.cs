namespace TransportR.ClientBench;

/// <summary>A read-only, non-seekable stream of <c>length</c> bytes of the repeated pattern block, held in memory.</summary>
internal sealed class PatternStream(byte[] block, long length) : Stream
{
    private long _position;

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => length;
    public override long Position
    {
        get => _position;
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

    public override int Read(Span<byte> buffer)
    {
        var count = (int)Math.Min(buffer.Length, length - _position);
        var written = 0;
        var offset = (int)(_position % block.Length);
        while (written < count)
        {
            var run = Math.Min(count - written, block.Length - offset);
            block.AsSpan(offset, run).CopyTo(buffer[written..]);
            written += run;
            offset = 0;
        }

        _position += count;
        return count;
    }

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(Read(buffer.Span));

    public override void Flush() { }
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}
