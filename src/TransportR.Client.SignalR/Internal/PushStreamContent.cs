using System.Net;

namespace TransportR.Client.SignalR.Internal;

/// <summary>
/// HttpContent that streams data via a callback for efficient HTTP POST streaming.
/// </summary>
internal class PushStreamContent : HttpContent
{
    private readonly Func<Stream, HttpContent?, CancellationToken, Task> _onStreamAvailable;

    public PushStreamContent(Func<Stream, HttpContent?, CancellationToken, Task> onStreamAvailable)
    {
        _onStreamAvailable = onStreamAvailable;
    }

    protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context)
    {
        await _onStreamAvailable(stream, this, CancellationToken.None);
    }

    protected override bool TryComputeLength(out long length)
    {
        length = -1;
        return false;
    }
}
