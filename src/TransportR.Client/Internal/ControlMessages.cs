using TransportR.Client.Protocol;

namespace TransportR.Client.Internal;

internal interface IControlMessage { }

internal record BatchControlMessage(int BatchId, BatchEntry Entry) : IControlMessage;

/// <param name="Payload">
/// What end-to-end encryption adds to a Ping, Pong, Complete or CompleteAck; <c>null</c> for none, which
/// a transfer in clear always is.
/// </param>
internal record ControlChunkMessage(ChunkType Type, int BatchId, byte[]? Payload = null) : IControlMessage;
