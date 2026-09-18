# TransportR

Core library containing interfaces, protocols, and abstractions for the TransportR distributed data transfer solution.

## Purpose

This project defines the foundational contracts and binary protocols without implementing any network transport logic. It serves as the backbone for client and server implementations.

## Structure

```
TransportR/
├── Abstractions/              # Base classes for transfer handles
│   ├── Transfer.cs            # Base class with TransferId
│   ├── Send.cs                # Send<T> abstract class
│   └── Receive.cs             # Receive<T> abstract class
├── Interfaces/
│   ├── IBatchSerializer.cs    # Serialization contract
│   ├── ITransportRClient.cs   # Client interfaces (IDataSender, IDataReceiver)
│   ├── ITransportRServer.cs   # Server interface
│   ├── ICommandClient.cs      # Client callback interface
│   ├── ICommandHub.cs         # Hub methods interface
│   ├── ITransferManager.cs    # Transfer lifecycle abstraction
│   └── IHubProgressMonitor.cs  # Hub-side progress monitoring interface
├── Models/
│   ├── ClientInfo.cs          # Connected client information
│   ├── ClientIdentity.cs      # Client identity from JWT
│   └── TransferInfo.cs        # Transfer statistics
├── Protocol/
│   ├── BatchProtocol.cs       # Batch serialization wrapper
│   ├── ChunkProtocol.cs       # ChunkType enum
│   ├── ChunkHeader.cs         # 30-byte binary header
│   └── ChunkIO.cs             # Async read/write operations
└── Metrics/
    └── TransportRMetrics.cs   # OpenTelemetry metrics
```

## Key Interfaces

### IBatchSerializer

```csharp
public interface IBatchSerializer
{
    Task SerializeBatchAsync<T>(Stream destination, List<T> batch);
    Task<List<T>?> DeserializeBatchAsync<T>(Stream source);
}
```

### IDataSender<T> / IDataReceiver<T>

```csharp
public interface IDataSender<T> : IDataClient
{
    Task<Send<T>> InitTransferAsync(Guid targetClientId, int? batchSize = null);
    Task<List<ClientInfo>> GetReceiversAsync(CancellationToken ct = default);
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

### Binary Header (v1)

30-byte fixed header using `BinaryPrimitives` (zero external dependencies):

```
│ Version │ Type │ PayloadLen │ BatchId │ Count │ TransferId │ [Payload] │
│ 1 byte  │ 1 b  │ 4 bytes    │ 4 bytes │ 4 b   │ 16 bytes   │ variable  │
```

## Dependencies

- `Microsoft.IO.RecyclableMemoryStream` - Memory-efficient streams
- `System.Diagnostics.DiagnosticSource` - OpenTelemetry metrics

