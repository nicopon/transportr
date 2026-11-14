namespace TransportR.Abstractions;

/// <summary>
/// Thrown when the hub refuses a request, or cannot carry it out.
/// </summary>
/// <remarks>
/// Raised on the hub by <c>ITransferInitiator.InitTransferAsync</c>, and on a client
/// by each request it makes of the hub: registering, listing receivers, opening a transfer, and
/// checking or refreshing its registration. The message is the one the hub gives a client, generic
/// on purpose; the detail is in the hub log.
/// </remarks>
public class HubRequestFailedException : Exception
{
    public HubRequestFailedException(string message) : base(message)
    {
    }

    public HubRequestFailedException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
