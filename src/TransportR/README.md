# TransportR

Core library containing interfaces, protocols, and abstractions for the TransportR distributed data transfer solution.

## Purpose

This project defines the foundational contracts and binary protocols without implementing any network transport logic. It serves as the backbone for client and server implementations.

## Key Interfaces

### IBatchSerializer<T>

```csharp
public interface IBatchSerializer<T>
{
    Task SerializeBatchAsync(Stream destination, List<T> batch);
    Task<List<T>?> DeserializeBatchAsync(Stream source);
}
```

The item type is on the interface, not on the call. A per-call generic would claim that every
implementation carries every type, which none can and one cannot at all: a serializer built on
Apache Arrow carries record batches only, and on a generic contract it could reject anything else
only by throwing at the first batch. Typed here, the mismatch is a compile error.

### IDataSender<T> / IDataReceiver<T>

```csharp
public interface IDataSender<T> : IDataClient
{
    Task<Send<T>> InitTransferAsync(Guid targetClientId, int? batchSize = null);
    Task<List<ClientInfo>> GetReceiversAsync(CancellationToken ct = default);
    event Func<Send<T>, Task> OnSendRequested;   // a transfer the hub's host opened
}

public interface IDataReceiver<T> : IDataClient
{
    event Func<Receive<T>, Task> OnTransferStarted;
}
```

### ITransferManager

```csharp
// For unit testing hub components
public interface ITransferManager { ... }
```

## Protocols

The chunk types and the protocol flow are described in [TransportRArchitecture.md](../../TransportRArchitecture.md).

### Binary Header (v2)

30-byte fixed header using `BinaryPrimitives` (zero external dependencies). The version byte is 2
(`ChunkHeader.CurrentVersion`); a reader rejects any other value.

```
│ Version │ Type │ PayloadLen │ BatchId │ Count │ TransferId │ [Payload] │
│ 1 byte  │ 1 b  │ 4 bytes    │ 4 bytes │ 4 b   │ 16 bytes   │ variable  │
```

## Dependencies

- `Microsoft.Extensions.Hosting.Abstractions`, `Microsoft.Extensions.Logging.Abstractions`, `Microsoft.Extensions.Options` - abstractions only

