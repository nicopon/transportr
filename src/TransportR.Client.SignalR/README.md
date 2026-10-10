# TransportR - SignalR Client

The official SignalR client implementation for [TransportR](https://github.com/nicopon/TransportR), a high-performance data transport library for .NET.

## Features

- **Efficient Streaming**: Stream large datasets in chunks with minimal memory overhead.
- **Resilience**: Automatic reconnection, checkpoint recovery, and chaos-tested stability.
- **Flexibility**: Protocol-agnostic transport layer.
- **Internalized Batteries**: Built-in support for Client Credentials Auth and MessagePack serialization.

## Installation

```bash
dotnet add package TransportR.Client.SignalR
```

## Quick Start (Fluent API)

```csharp
using TransportR.Client.SignalR;
using Microsoft.Extensions.Logging;

// 1. Configure the Client
var client = new TransportRClientBuilder<Order>()
    .WithUrl("http://localhost:5000")
    .WithClientCredentials(
        tokenEndpoint: "http://localhost:5100/connect/token",
        clientId: "my-client",
        clientSecret: "my-secret",
        scope: "api"
    )
    .WithMessagePackSerialization() // Use built-in MessagePack support
    // .WithSerializer(new MyCustomSerializer()) // Or bring your own
    .WithBatchSize(1000)
    .WithLoggerFactory(LoggerFactory.Create(b => b.AddConsole()))
    .Build();

// 2. Connect
await client.ConnectAsync(new[] { "my-group" });

// 3. Send Data
await client.InitTransferAsync("receiver-id");
// ... send items ...

// 4. Dispose
await client.DisposeAsync();
```

## Advanced Configuration

### Authentication
You can provide a static token or a custom dynamic provider:
```csharp
.WithAccessToken("ey...") 
// OR
.WithTokenProvider(async () => await GetMyTokenAsync())
```
Only a provider is renewed (see "Token Renewal" in `SecurityArchitecture.md`): the hub closes the
connection of a client whose token it was not given a new one for, and a static token's client is closed
when it expires. `WithTokenRefreshThreshold` sets how far ahead of the expiry the renewal starts.

A host that owns the rights of its clients (its own `IClientIdentityProvider` on the hub) and changes
them while they run applies the change without waiting for a renewal:
```csharp
await client.RefreshRegistrationAsync();
```
The client registers again under the same id, with the groups its identity holds now and a token from its
provider, and returns once the hub has recorded both. Its connection and its transfers are not interrupted;
the hub ends those the new groups no longer allow. It raises `InvalidOperationException` when the client is
not connected, and the hub's refusal when the identity holds no group (the registration stays as it was).
The hub must be of the same release.

### Serialization
We recommend MessagePack for performance, but you can implement `IBatchSerializer<T>` for any
format (e.g. JSON, Protobuf).

The builder carries the item type, so a serializer that cannot carry it is not an overload that
exists: `new TransportRClientBuilder<string>().WithArrowSerialization()` does not compile, rather
than building a client that fails on its first batch.

## License

MIT
