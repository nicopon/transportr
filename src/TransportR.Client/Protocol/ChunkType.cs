namespace TransportR.Client.Protocol;

/// <summary>
/// Chunk type for wire protocol.
/// </summary>
internal enum ChunkType : byte
{
    Batch = 0x01,
    BatchAck = 0x02,     // Receiver acknowledges Batch receipt
    Complete = 0x03,     // Sender signals end of transfer
    CompleteAck = 0x04,  // Receiver confirms receipt of Complete
    Ping = 0x05,         // Connection probe after reconnection
    Pong = 0x06,         // Response to connection probe
    Abort = 0x07,        // One end is giving up; BatchId carries an AbortReason
    StreamBatch = 0x08   // Batch of a transfer without a serializer: the payload is raw bytes
}
