using System.Net.WebSockets;
using TransportR.Client.Protocol;
using TransportR.Client.Sessions;
using TransportR.Protocol;

namespace TransportR.Client.Internal;

/// <summary>
/// The write side of a data stream carried by a WebSocket, already opened and proven (see
/// <see cref="DataPlane"/>). A flush is one binary message, and waits for no answer: a peer that reads
/// slowly fills the socket, and the flush then blocks. That is the stream's backpressure.
/// </summary>
/// <remarks>
/// A read stays pending from start to end, because the hub only ever says one thing, a <c>Close</c>, and
/// says it whenever it has to: a <c>Close</c> nobody reads leaves the writes succeeding as if nothing had
/// happened. Owns the socket.
/// </remarks>
internal sealed class WebSocketWriteStream : WriteStream
{
    private readonly WebSocket _socket;
    private readonly byte[] _buffer;
    private readonly CancellationTokenSource _lost = new();
    private readonly Task _hubEnd;
    private int _count;
    private volatile bool _endSent;
    private volatile Exception? _failure;

    public WebSocketWriteStream(WebSocket socket, int bufferSize)
    {
        _socket = socket;
        _buffer = new byte[bufferSize];
        _hubEnd = WatchForTheHubsEndAsync();
    }

    public override CancellationToken Lost => _lost.Token;

    public override Exception? Failure => _failure;

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> source, CancellationToken cancellationToken = default)
    {
        while (!source.IsEmpty)
        {
            if (_count == _buffer.Length)
            {
                await SendBufferAsync(cancellationToken).ConfigureAwait(false);
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
        _count == 0 ? Task.CompletedTask : SendBufferAsync(cancellationToken);

    public override async Task CompleteAsync(CancellationToken cancellationToken)
    {
        if (_count > 0)
        {
            await SendBufferAsync(cancellationToken).ConfigureAwait(false);
        }

        _endSent = true;
        await _socket.CloseOutputAsync((WebSocketCloseStatus)DataPlane.EndOfStreamCloseStatus, null, cancellationToken).ConfigureAwait(false);
        await _hubEnd.WaitAsync(cancellationToken).ConfigureAwait(false);

        if (_failure is { } failure)
        {
            throw failure;
        }
    }

    private async Task SendBufferAsync(CancellationToken cancellationToken)
    {
        await _socket.SendAsync(_buffer.AsMemory(0, _count), WebSocketMessageType.Binary, endOfMessage: true, cancellationToken).ConfigureAwait(false);
        _count = 0;
    }

    private async Task WatchForTheHubsEndAsync()
    {
        var nothingExpected = new byte[16];
        try
        {
            var received = await _socket.ReceiveAsync(nothingExpected.AsMemory(), CancellationToken.None).ConfigureAwait(false);
            if (received.MessageType != WebSocketMessageType.Close)
            {
                Fail(new WireProtocolViolationException("The hub sent data on a write stream."));
            }
            else if (_socket.CloseStatus == (WebSocketCloseStatus)DataPlane.StreamOverCloseStatus)
            {
                Fail(new StreamRefusedException(StreamOutcome.Retryable, "The hub holds this stream no longer"));
            }
            else if (!_endSent || _socket.CloseStatus != (WebSocketCloseStatus)DataPlane.EndOfStreamCloseStatus)
            {
                Fail(new IOException($"The WebSocket closed with status {(int?)_socket.CloseStatus}, which does not end the stream."));
            }
        }
        catch (Exception ex)
        {
            Fail(new IOException("The WebSocket carrying the stream failed.", ex));
        }
    }

    // In this order: aborting releases a blocked write, whose caller then looks for the failure.
    private void Fail(Exception failure)
    {
        _failure = failure;
        _socket.Abort();
        _lost.Cancel();
    }

    public override async ValueTask DisposeAsync()
    {
        _socket.Abort();
        await _hubEnd.ConfigureAwait(false);
        _socket.Dispose();
        _lost.Dispose();
        await base.DisposeAsync().ConfigureAwait(false);
    }
}
