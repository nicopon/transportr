using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using Microsoft.Extensions.Logging;
using TransportR.Client.Protocol;
using TransportR.Client.Sessions;
using TransportR.Protocol;

namespace TransportR.Client.Internal;

/// <summary>
/// The write side of a data stream, sent in segments (see <see cref="DataPlane"/>): what is written
/// goes out on a flush, or when the buffer is full, as a request of its own. One instance is one
/// epoch, and has one segment in flight at a time.
/// </summary>
/// <remarks>
/// A flush returns once the hub has written the segment on to the peer, so a peer that reads slowly
/// slows the flushes down: that is the stream's backpressure. A segment the hub held at its bound is
/// sent again from where the hub's answer says the peer stands, in the same epoch; so is one refused
/// for an expired token, once, with a renewed one. Any other failure ends the epoch, and the session
/// opens a new one.
/// </remarks>
internal sealed class SegmentedWriteStream : WriteStream
{
    private readonly IStreamTransport _transport;
    private readonly ILogger _logger;
    private readonly Func<string, HttpRequestMessage> _newRequest;
    private readonly long? _heartbeatMs;
    private readonly Guid _epoch = Guid.NewGuid();
    private readonly byte[] _buffer;
    private int _count;

    // Where the buffer starts in the epoch's stream: everything before has reached the peer.
    private long _offset;

    /// <param name="newRequest">
    /// Builds the POST of a segment, its route and stream headers set, from the query that names it.
    /// </param>
    /// <param name="heartbeatInterval">
    /// Announced on the opening segment when the writer keeps to it, which only the sender's data
    /// stream does; null otherwise.
    /// </param>
    public SegmentedWriteStream(IStreamTransport transport, ILogger logger, Func<string, HttpRequestMessage> newRequest, int bufferSize, TimeSpan? heartbeatInterval)
    {
        _transport = transport;
        _logger = logger;
        _newRequest = newRequest;
        _buffer = new byte[bufferSize];
        _heartbeatMs = heartbeatInterval is { } interval ? (long)interval.TotalMilliseconds : null;
    }

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> source, CancellationToken cancellationToken = default)
    {
        while (!source.IsEmpty)
        {
            if (_count == _buffer.Length)
            {
                await SendAsync(final: false, cancellationToken).ConfigureAwait(false);
            }

            var count = Math.Min(source.Length, _buffer.Length - _count);
            source[..count].CopyTo(_buffer.AsMemory(_count));
            _count += count;
            source = source[count..];
        }
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override Task FlushAsync(CancellationToken cancellationToken) =>
        _count == 0 ? Task.CompletedTask : SendAsync(final: false, cancellationToken);

    public override Task CompleteAsync(CancellationToken cancellationToken) => SendAsync(final: true, cancellationToken);

    private async Task SendAsync(bool final, CancellationToken cancellationToken)
    {
        var relayed = 0;
        var refreshed = false;
        while (true)
        {
            var offset = _offset + relayed;
            using var request = _newRequest(Query(offset, final));
            request.Content = new ReadOnlyMemoryContent(_buffer.AsMemory(relayed, _count - relayed));

            var token = await _transport.GetAccessTokenAsync(refreshed).ConfigureAwait(false);
            if (!string.IsNullOrEmpty(token))
            {
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            }

            using var response = await _transport.HttpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
            switch (response.StatusCode)
            {
                case HttpStatusCode.Unauthorized when !refreshed:
                    // A refused segment took nothing: it goes again, at the same offset.
                    _logger.LogWarning("Stream Unauthorized (401). Refreshing token and retrying...");
                    refreshed = true;
                    continue;
                case HttpStatusCode.Unauthorized:
                    throw new StreamRefusedException(StreamOutcome.Retryable, "Unauthorized (401) - Check credentials");
                case HttpStatusCode.NotFound or HttpStatusCode.Gone:
                    throw new StreamRefusedException(StreamOutcome.Gone, $"Transfer Gone ({response.StatusCode})");
                case HttpStatusCode.Conflict:
                    throw new StreamRefusedException(StreamOutcome.Retryable, "The hub holds this stream no longer (409)");
            }

            response.EnsureSuccessStatusCode();
            relayed = Relayed(response, relayed);
            if (response.StatusCode == HttpStatusCode.OK && relayed == _count) break;

            // Held at the hub's bound with the peer not reading: what did not get through goes again.
            if (response.StatusCode != HttpStatusCode.Accepted)
            {
                throw new WireProtocolViolationException(
                    $"The hub answered a segment {(int)response.StatusCode} with {relayed} of its {_count} bytes relayed.");
            }
        }

        _offset += _count;
        _count = 0;
    }

    private string Query(long offset, bool final)
    {
        var query = $"{DataPlane.EpochParameter}={_epoch}&{DataPlane.OffsetParameter}={offset.ToString(CultureInfo.InvariantCulture)}";
        if (offset == 0 && _heartbeatMs is { } heartbeatMs)
        {
            query += $"&{DataPlane.HeartbeatParameter}={heartbeatMs.ToString(CultureInfo.InvariantCulture)}";
        }

        return final ? query + $"&{DataPlane.FinalParameter}=true" : query;
    }

    /// <returns>How much of the buffer the hub says has reached the peer.</returns>
    private int Relayed(HttpResponseMessage response, int before)
    {
        if (!response.Headers.TryGetValues(DataPlane.RelayedHeader, out var values)
            || !long.TryParse(values.FirstOrDefault(), NumberStyles.None, CultureInfo.InvariantCulture, out var relayed))
        {
            // A hub of an earlier version takes a segment for a whole stream and its end for the
            // transfer's: going on would only hide that.
            throw new WireProtocolViolationException(
                $"The hub answered a segment without the {DataPlane.RelayedHeader} header: it does not speak this version of the data plane.");
        }

        var covered = relayed - _offset;
        if (covered < before || covered > _count)
        {
            throw new WireProtocolViolationException(
                $"The hub says {relayed} bytes of the stream were relayed, outside the {_offset + before}..{_offset + _count} the segment covers.");
        }

        return (int)covered;
    }

}

/// <summary>The hub refused a write stream, with what the refusal means for the attempt.</summary>
internal sealed class StreamRefusedException(StreamOutcome outcome, string message) : Exception(message)
{
    public StreamOutcome Outcome { get; } = outcome;
}
