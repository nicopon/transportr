using System.Buffers;

namespace TransportR.Client.Internal;

internal abstract class NonSeekableTransferStream : Stream
{
    private bool _disposed;

    public override bool CanSeek => false;
    public override long Length => throw new NotSupportedException();
    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    protected bool IsDisposed => _disposed;

    public override void Flush() => EnsureNotDisposed();

    public override Task FlushAsync(CancellationToken cancellationToken)
    {
        EnsureNotDisposed();
        return cancellationToken.IsCancellationRequested
            ? Task.FromCanceled(cancellationToken)
            : Task.CompletedTask;
    }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();

    protected override void Dispose(bool disposing) => _disposed = true;

    public override ValueTask DisposeAsync()
    {
        _disposed = true;
        return ValueTask.CompletedTask;
    }

    protected void EnsureNotDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);
}

/// <summary>
/// Write-only <see cref="Stream"/> adapter over a <see cref="ByteSend"/> handle.
/// Disposing this stream does not complete or dispose the underlying handle.
/// </summary>
internal sealed class ByteSendStream : NonSeekableTransferStream
{
    private readonly ByteSend _send;

    public ByteSendStream(ByteSend send)
    {
        _send = send ?? throw new ArgumentNullException(nameof(send));
    }

    public override bool CanRead => false;
    public override bool CanWrite => !IsDisposed;

    public override void Write(byte[] buffer, int offset, int count)
    {
        ValidateBufferArguments(buffer, offset, count);
        EnsureNotDisposed();
        if (count == 0) return;

        _send.WriteAsync(buffer.AsMemory(offset, count)).GetAwaiter().GetResult();
    }

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        EnsureNotDisposed();
        if (buffer.IsEmpty) return;

        var rented = ArrayPool<byte>.Shared.Rent(buffer.Length);
        try
        {
            buffer.CopyTo(rented);
            _send.WriteAsync(rented.AsMemory(0, buffer.Length)).GetAwaiter().GetResult();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        ValidateBufferArguments(buffer, offset, count);
        EnsureNotDisposed();
        return _send.WriteAsync(buffer.AsMemory(offset, count), cancellationToken);
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        EnsureNotDisposed();
        return new ValueTask(_send.WriteAsync(buffer, cancellationToken));
    }

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}

/// <summary>
/// Read-only <see cref="Stream"/> adapter over a <see cref="ByteReceive"/> handle.
/// Disposing this stream does not dispose the underlying handle.
/// </summary>
internal sealed class ByteReceiveStream : NonSeekableTransferStream
{
    private readonly ByteReceive _receive;

    public ByteReceiveStream(ByteReceive receive)
    {
        _receive = receive ?? throw new ArgumentNullException(nameof(receive));
    }

    public override bool CanRead => !IsDisposed;
    public override bool CanWrite => false;

    public override int Read(byte[] buffer, int offset, int count)
    {
        ValidateBufferArguments(buffer, offset, count);
        EnsureNotDisposed();
        return count == 0
            ? 0
            : _receive.ReadAsync(buffer.AsMemory(offset, count)).AsTask().GetAwaiter().GetResult();
    }

    public override int Read(Span<byte> buffer)
    {
        EnsureNotDisposed();
        if (buffer.IsEmpty) return 0;

        var rented = ArrayPool<byte>.Shared.Rent(buffer.Length);
        try
        {
            var read = _receive.ReadAsync(rented.AsMemory(0, buffer.Length)).AsTask().GetAwaiter().GetResult();
            rented.AsSpan(0, read).CopyTo(buffer);
            return read;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        ValidateBufferArguments(buffer, offset, count);
        EnsureNotDisposed();
        return count == 0
            ? Task.FromResult(0)
            : _receive.ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
    }

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        EnsureNotDisposed();
        return buffer.IsEmpty
            ? ValueTask.FromResult(0)
            : _receive.ReadAsync(buffer, cancellationToken);
    }

    public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(destination);
        EnsureNotDisposed();
        return _receive.CopyToAsync(destination, cancellationToken);
    }

    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}
