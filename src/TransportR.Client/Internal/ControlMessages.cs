using TransportR.Client.Protocol;

namespace TransportR.Client.Internal;

internal interface IControlMessage { }

internal record BatchControlMessage(int BatchId, BatchEntry Entry) : IControlMessage;

internal record ControlChunkMessage(ChunkType Type, int BatchId) : IControlMessage;
