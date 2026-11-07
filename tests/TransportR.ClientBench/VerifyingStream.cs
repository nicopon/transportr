namespace TransportR.ClientBench;

/// <summary>
/// A write-only stream that checks each byte against the pattern block as it arrives and throws on the
/// first one that differs, so an altered, dropped or reordered byte fails the run instead of being counted.
/// </summary>
internal sealed class VerifyingStream(byte[] block) : Stream
{
    private long _position;

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => _position;
    public override long Position
    {
        get => _position;
        set => throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count) => Write(buffer.AsSpan(offset, count));

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        if (!Pattern.Matches(block, _position, buffer))
            throw new InvalidDataException($"The bytes received at offset {_position} differ from the bytes sent.");

        _position += buffer.Length;
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        Write(buffer.Span);
        return ValueTask.CompletedTask;
    }

    public override void Flush() { }
    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
}
