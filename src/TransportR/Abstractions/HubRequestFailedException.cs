namespace TransportR;

/// <summary>
/// Thrown when the hub refuses a request, or cannot carry it out.
/// </summary>
/// <remarks>
/// Raised on the hub by <c>ITransferInitiator.InitTransferAsync</c>, and on a client
/// by each request it makes of the hub: registering, listing receivers, opening a transfer, and
/// checking or refreshing its registration. The message is the one the hub gives a client, generic
/// on purpose; the detail is in the hub log. <see cref="Category"/> tells the failures apart without
/// reading the message.
/// </remarks>
public class HubRequestFailedException : Exception
{
    /// <summary>
    /// Why the request failed. A <see cref="HubRequestFailureCategory.Refused"/> says nothing of the
    /// caller's rights or of its target's.
    /// </summary>
    public HubRequestFailureCategory Category { get; }

    public HubRequestFailedException(HubRequestFailureCategory category, string message) : base(message)
    {
        Category = category;
    }

    public HubRequestFailedException(HubRequestFailureCategory category, string message, Exception innerException)
        : base(message, innerException)
    {
        Category = category;
    }
}
