namespace TransportR.Protocol;

/// <summary>
/// Chunk type for wire protocol.
/// </summary>
public enum ChunkType : byte
{
    Batch = 0x01,
    BatchAck = 0x02,     // Receiver acknowledges Batch receipt
    Complete = 0x03,     // Sender signals end of transfer
    CompleteAck = 0x04,  // Receiver confirms receipt of Complete
    Ping = 0x05,         // Connection probe after reconnection
    Pong = 0x06          // Response to connection probe
}
