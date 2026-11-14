namespace TransportR.Hub.Controllers;

internal static class StreamedResponse
{
    /// <summary>
    /// The response header, <c>no</c>, that asks a reverse proxy not to buffer a response: one that
    /// does holds back the few bytes a stream can end on until its buffer fills, which a stream may
    /// never do. Nginx honours it.
    /// </summary>
    public const string ProxyBufferingHeader = "X-Accel-Buffering";
}
