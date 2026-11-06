namespace TransportR.Client.Internal;

/// <summary>
/// The write side of a data stream, whatever carries it: what is written goes out on a flush, or when
/// the buffer is full. One instance is one opening of the stream.
/// </summary>
internal abstract class WriteStream : Stream
{
    /// <summary>
    /// Sends what is left and ends the stream. Returns once the hub has closed it: for a sender's data
    /// stream, once the hub has seen the transfer through and forgotten it.
    /// </summary>
    public abstract Task CompleteAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Cancelled when the stream fails with nothing being written, which a transport that holds a
    /// connection open can do. Never cancelled by a transport that writes only when asked to.
    /// </summary>
    public virtual CancellationToken Lost => CancellationToken.None;

    /// <summary>What made the stream fail on its own; null so long as it has not.</summary>
    public virtual Exception? Failure => null;

    // Every write the client makes is asynchronous; one that blocked would hold a thread for as long as
    // the peer takes to read.
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override void Flush() => throw new NotSupportedException();

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => throw new NotSupportedException();
    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
}
