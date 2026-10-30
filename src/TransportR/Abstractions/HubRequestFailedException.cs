namespace TransportR.Abstractions;

/// <summary>
/// Thrown when the hub refuses a request, or cannot carry it out.
/// </summary>
/// <remarks>
/// Raised by <see cref="Interfaces.ITransferInitiator.InitTransferAsync"/>. The message is the one a
/// client is given, generic on purpose; the detail is in the hub log.
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
