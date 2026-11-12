# TransportR - Client

The client of [TransportR](https://github.com/nicopon/TransportR): streams typed records between .NET applications through a hub both sides connect out to.

## Features

- **Efficient Streaming**: Stream large datasets in batches, with opt-in backpressure at both ends to bound memory.
- **Resilience**: Automatic reconnection and resumption from the receiver's last batch, checked by fault-injection scenarios (see the repository's `scripts/README.md`).
- **Serializer-agnostic**: MessagePack, Protobuf, JSON, Apache Arrow, or your own `IBatchSerializer<T>`.
- **Built-in auth**: OAuth2 client credentials. MessagePack serialization ships in its own package, `TransportR.Serialization.MessagePack`.

## Installation

```bash
dotnet add package TransportR.Client
dotnet add package TransportR.Serialization.MessagePack   # or another serializer package
```

## Quick Start (Fluent API)

```csharp
using TransportR.Client;
using TransportR.Serialization.MessagePack;
using Microsoft.Extensions.Logging;

// 1. Configure the Client
var client = new TransportRClientBuilder<Order>()
    .WithUrl("http://localhost:5000")
    .WithClientCredentials(
        tokenEndpoint: "http://localhost:5100/connect/token",
        clientId: "my-client",
        clientSecret: "my-secret",
        scope: "transportr:group:senders"   // the client's groups come from these scopes
    )
    .WithMessagePackSerialization() // Use built-in MessagePack support
    // .WithSerializer(new MyCustomSerializer()) // Or bring your own
    .WithBatchSize(1000)
    .WithLoggerFactory(LoggerFactory.Create(b => b.AddConsole()))
    .Build();

// 2. Connect (with a token, groups passed here are ignored: the token's scopes decide)
await client.ConnectAsync();

// 3. Send Data
var receivers = await client.GetReceiversAsync();
await using var transfer = await client.InitTransferAsync(receivers[0].ClientId);
// ... send each item with transfer.SendAsync(item) ...
await transfer.CompleteAsync();

// 4. Dispose
await client.DisposeAsync();
```

## Moving raw bytes

A file, a pipe or any other stream of bytes needs no item type and no serializer: `ByteStreamClientBuilder` builds a
`ByteStreamClient`, whose transfers are a `ByteSend` and a `ByteReceive`. The bytes are cut into batches of `batchBytes`,
the argument of the builder, and are sent as they are: never compressed, and no boundary of the sender's calls reaches the
receiver.

```csharp
// Sender
var sender = new ByteStreamClientBuilder(batchBytes: 1 << 20)
    .WithUrl("http://localhost:5000")
    .WithMaxInFlightBatches(8)            // optional: bounds what the sender holds
    .Build();
await sender.ConnectAsync(new[] { "senders" });
var receivers = await sender.GetReceiversAsync();
await using var send = await sender.InitTransferAsync(receivers[0].ClientId);
await using var source = File.OpenRead("data.bin");
await send.CopyFromAsync(source);         // or send.WriteAsync(bytes), which copies them before it returns
await send.CompleteAsync();               // returns once the receiver has acknowledged everything

// Receiver
var receiver = new ByteStreamClientBuilder(batchBytes: 1 << 20)
    .WithUrl("http://localhost:5000")
    .WithReceiveCapacity(64 << 20)        // optional: bounds what the receiver holds, in wire bytes
    .Build();
receiver.OnTransferStarted += async transfer =>
{
    await using (transfer)
    {
        await using var file = File.Create("out.bin");
        await transfer.CopyToAsync(file); // or transfer.ReadAsync(buffer), which returns 0 once the transfer is over
    }
};
await receiver.ConnectAsync(new[] { "receivers" });
```

The builder has the options of the item builder that mean something for bytes (`WithClientCredentials`, `WithAccessToken`,
`WithTokenProvider`, `WithWriteTransport`, `WithLoggerFactory`, among others). One that means something only for items (`WithSerializer`, `WithBatchSize`, a bound in items) is not an overload that exists. Both ends
of a transfer must be `ByteStreamClient`s, or both item clients. `TransferInfo.ItemsProcessed` is 0 for a byte transfer, which has
no items; `BytesTransferred` carries the count. `WithMaxBatchBytes`, on both builders, bounds one batch on the wire,
256 MiB by default: a receiver fails the transfer on a larger chunk, and `Build()` refuses a `batchBytes` above it.

### Sizing for WAN and high-throughput streaming

On high-latency links, size the in-flight window (`WithMaxInFlightBatches`) to at least the bandwidth-delay product:
$$\text{Window} = \text{batchBytes} \times \text{MaxInFlightBatches} \ge \text{RTT} \times \text{TargetBandwidth}$$

For continuous streaming at 60–100 MB/s over a 25 ms WAN (BDP $\approx$ 1.5–2.5 MB), use 256 KB or 512 KB batches with an in-flight window of 4–8 MB (e.g. `WithMaxInFlightBatches(16)`). This keeps the pipe full during acknowledgement round-trips and matches `StreamBufferSize` (512 KB).

## Advanced Configuration

### Authentication
You can provide a static token or a custom dynamic provider:
```csharp
.WithAccessToken("ey...") 
// OR
.WithTokenProvider(async () => await GetMyTokenAsync())
```
Only a provider is renewed (see "Token Renewal" in `SecurityArchitecture.md`): the hub closes the
events stream of a client whose token it was not given a new one for, and a static token's client is closed
when it expires. `WithTokenRefreshThreshold` sets how far ahead of the expiry the renewal starts.

A host that owns the rights of its clients (its own `IClientIdentityProvider` on the hub) and changes
them while they run applies the change without waiting for a renewal:
```csharp
await client.RefreshRegistrationAsync();
```
The client registers again under the same id, with the groups its identity holds now and a token from its
provider, and returns once the hub has recorded both. It stays reachable and its transfers are not
interrupted; the hub ends those the new groups no longer allow. It raises `InvalidOperationException` when the client is
not connected or its lost connection cannot reach the hub, and the hub's refusal when the identity holds no group,
connection lost or not (the registration stays as it was).
The hub must be of the same release.

`IsRegisteredAsync()` asks the hub whether it still knows the client. It answers `false` when the hub has
forgotten it (a restart, an eviction); the next presence heartbeat registers it again, and
`RefreshRegistrationAsync()` does so at once.

### Write transport
A write stream (the sender's data, the receiver's acknowledgements) goes over a WebSocket where the path to the hub
carries one, and over short POST requests (segments) otherwise:
```csharp
.WithWriteTransport(WriteTransportPolicy.PreferWebSocket) // the default
```
`PreferWebSocket` falls back on segments for the rest of the transfer, with a logged warning, when the path does not
carry a WebSocket; `RequireWebSocket` fails the transfer (`TransferFailedException`) instead; `SegmentsOnly` never asks
for an upgrade. `GetTransfers()` reports `TransferInfo.WriteTransport` (`WebSocket` or `Segments`, null before the first
opening) and `TransferInfo.WriteTransportFallback` (why a transfer has no WebSocket, null otherwise). A proxy in front of
the hub must relay the upgrade: see the hub's README.

### Serialization
MessagePack is the recommended default: on par with Protobuf in our benchmark, and it takes any POCO
without attributes or schema. Protobuf needs annotated contracts and produces the most compact payload;
JSON can be sent uncompressed for debugging (`WithJsonSerialization(compress: false)`). Both ends of a
transfer must use the same serializer and settings: a batch the receiver cannot decode fails the transfer.
Any other format is an `IBatchSerializer<T>` away.

The builder carries the item type, so a serializer that cannot carry it is not an overload that
exists: `new TransportRClientBuilder<string>().WithArrowSerialization()` does not compile, rather
than building a client that fails on its first batch.

## License

MIT
