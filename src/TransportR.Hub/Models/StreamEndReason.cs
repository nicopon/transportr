namespace TransportR.Hub.Models;

/// <summary>
/// Indicates how a stream ended.
/// Critical for distinguishing clean completion (domino advance) vs network failure (reconnect).
/// </summary>
internal enum StreamEndReason
{
    /// <summary>
    /// ReadAsync returned 0 (EOF). The remote party intentionally closed the stream.
    /// This is the expected signal for domino progression.
    /// </summary>
    CleanEOF,

    /// <summary>
    /// ReadAsync threw IOException/HttpRequestException.
    /// Network failure or unexpected disconnect - requires reconnect flow.
    /// </summary>
    Rupture,

    /// <summary>
    /// CancellationToken was triggered.
    /// Intentional local cancellation (e.g., timeout, user abort).
    /// </summary>
    Cancelled
}
