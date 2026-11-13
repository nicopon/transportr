namespace TransportR.Client.Internal;

/// <summary>
/// A write-only view of a compressing stream that fails once more than a limit has gone into it, so
/// a batch is refused by its sender at the bound its receiver holds it to once decompressed.
/// </summary>
/// <remarks>
/// Does not own <c>inner</c>: disposing this leaves it open, for the caller that created both.
/// </remarks>
internal sealed class BoundedWriteStream(Stream inner, long limit) : Stream
{
    private long _written;

    public override bool CanRead => false;

    public override bool CanSeek => false;

    public override bool CanWrite => true;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count) => Write(buffer.AsSpan(offset, count));

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        Count(buffer.Length);
        inner.Write(buffer);
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        Count(buffer.Length);
        return inner.WriteAsync(buffer, cancellationToken);
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        => WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override void Flush() => inner.Flush();

    public override Task FlushAsync(CancellationToken cancellationToken) => inner.FlushAsync(cancellationToken);

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    private void Count(int length)
    {
        _written += length;
        if (_written > limit) throw DecompressionBound.ExceededBeforeCompression(limit);
    }
}
