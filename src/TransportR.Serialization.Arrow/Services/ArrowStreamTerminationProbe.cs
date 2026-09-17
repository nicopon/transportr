namespace TransportR.Serialization.Arrow.Services;

/// <summary>
/// Records whether the wrapped stream ran out of bytes while the Arrow reader was still reading,
/// which is what tells a truncated IPC stream apart from a complete one.
/// </summary>
/// <remarks>
/// <para>
/// An Arrow IPC stream ends with the marker <c>ffffffff 00000000</c>. A chunk that lost its tail
/// stops on a clean message boundary, and <c>ArrowStreamReader.ReadNextRecordBatchAsync</c>
/// answers <c>null</c> for that exactly as it does for a stream that carried its marker. A reader
/// that only tests for <c>null</c> therefore hands back the rows that arrived and reports success.
/// </para>
///
/// <para>
/// <b>Measured, not assumed:</b> without this probe a chunk with its last eight bytes removed
/// decodes into a complete-looking batch and no exception at all. The test that pins it is
/// <c>ArrowBatchSerializerTests.A_Truncated_Chunk_Is_Refused</c>, and it was written red.
/// </para>
///
/// <para>
/// The distinction is visible one level down. Reading the marker consumes eight bytes that exist,
/// so a complete stream never makes the source report end-of-file, while a stream that stopped
/// early always does. This probe records that one fact and judges nothing.
/// </para>
///
/// <para>
/// This is a second implementation of a guard that also lives in dtpipe's own Arrow reader, and
/// deliberately so: the two repositories cannot reference one another, and the alternative is a
/// transport that quietly reopens a data-loss defect the consumer already closed. Both rest on the
/// same property of Arrow's reader — that it does not read past the marker — so both fail in the
/// same loud direction if a future version changes it: complete streams start being refused.
/// </para>
/// </remarks>
internal sealed class ArrowStreamTerminationProbe(Stream inner) : Stream
{
    /// <summary>True once the source reported end-of-file to a read that asked for bytes.</summary>
    public bool SourceRanDry { get; private set; }

    /// <summary>
    /// How many bytes the source actually delivered.
    /// </summary>
    /// <remarks>
    /// Zero separates the two things an unterminated stream can mean. A body that delivered
    /// nothing at all is an EMPTY chunk — the accumulator flushed with nothing in it — while a
    /// body that delivered some bytes and then stopped is a chunk that was CUT. Both end without
    /// the marker, so without this count the empty case would be reported as data loss and the
    /// transport would fail on a legitimate flush.
    /// </remarks>
    public long BytesRead { get; private set; }

    private int Observe(int requested, int received)
    {
        if (requested > 0 && received == 0) SourceRanDry = true;
        BytesRead += received;
        return received;
    }

    public override int Read(byte[] buffer, int offset, int count)
        => Observe(count, inner.Read(buffer, offset, count));

    public override int Read(Span<byte> buffer)
        => Observe(buffer.Length, inner.Read(buffer));

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct)
        => Observe(count, await inner.ReadAsync(buffer.AsMemory(offset, count), ct));

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        => Observe(buffer.Length, await inner.ReadAsync(buffer, ct));

    public override bool CanRead => inner.CanRead;
    public override bool CanSeek => inner.CanSeek;
    public override bool CanWrite => false;
    public override long Length => inner.Length;
    public override long Position { get => inner.Position; set => inner.Position = value; }
    public override void Flush() => inner.Flush();
    public override long Seek(long offset, SeekOrigin origin) => inner.Seek(offset, origin);
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}
