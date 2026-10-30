namespace TransportR.Interfaces;

/// <summary>
/// Specifies the type of connection for streaming.
/// </summary>
public enum ConnectionType
{
    /// <summary>
    /// Data channel for transferring batch payloads.
    /// </summary>
    Data,

    /// <summary>
    /// Control channel for ACK/Complete messages.
    /// </summary>
    Control
}
