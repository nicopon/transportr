# TransportR - Client

The client of [TransportR](https://github.com/nicopon/TransportR): streams typed records (`TransportRClient<T>`) or raw bytes (`ByteStreamClient`) between .NET applications through a hub both sides connect out to.

## Features

- **Efficient Streaming**: Stream large datasets in batches, with opt-in backpressure at both ends to bound memory.
- **Resilience**: Automatic reconnection and resumption from the receiver's last batch, checked by fault-injection scenarios (see the repository's `scripts/README.md`).
- **Serializer-agnostic**: MessagePack, JSON or Apache Arrow, each in a package of its own (`TransportR.Serialization.*`), or your own `IBatchSerializer<T>`.
- **Built-in auth**: OAuth2 client credentials or a token provider, renewed ahead of the token's expiry, or a static token.

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
await using var transfer = await client.InitTransferAsync(receivers.First(r => r.DisconnectedAtUtc is null).ClientId);
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
await using var send = await sender.InitTransferAsync(receivers.First(r => r.DisconnectedAtUtc is null).ClientId);
await using var source = File.OpenRead("data.bin");
await send.CopyFromAsync(source);         // or send.WriteAsync(bytes), which copies them before it returns
await send.CompleteAsync();               // returns once the receiver has acknowledged everything (buffered, not yet consumed)

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
256 MiB by default: a receiver fails the transfer on a larger chunk, and `Build()` refuses a `batchBytes` above it. On an item client the
bound also covers a batch once decompressed, and a MessagePack or Arrow sender is not stopped by it: it is the receiver that refuses,
so the sender sizes its batches for it (see the documentation of `WithMaxBatchBytes`).

### Sizing for WAN and high-throughput streaming

A sender that sets `WithMaxInFlightBatches` stops once that many batches await an acknowledgement, so on a long round
trip the window, in bytes, has to cover the bandwidth-delay product, or the acknowledgements set the rate:

```
batch size in bytes × MaxInFlightBatches ≥ round-trip time × target bandwidth
```

A batch's size in bytes is `batchBytes` on a `ByteStreamClient`; on an item client it is `WithBatchSize` items times an
item's serialized size, after compression. The window lifts no other bound: in segments, the fallback, a sender moves
one 512 KB segment per round trip whatever its window ("Performance, and its conditions" in the repository's `README.md`).

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

`.WithWriteTransportProbe()`, off by default, has the client prove after each registration what the path carries of a
write stream, for the hub to tell its `IHubProgressMonitor` before any transfer; the client logs the outcome. It costs up
to 5 s per registration on a path that accepts the upgrade without relaying it, and each transfer still finds its own
transport.

### Network
Every request of a client (its control plane, its data streams and their WebSockets) goes through one
`SocketsHttpHandler`, which `ConfigureHttpHandler`, on both builders, sets up for a proxy, a client certificate or a
private certificate authority:
```csharp
.ConfigureHttpHandler(handler =>
{
    handler.Proxy = new WebProxy("http://proxy.example:3128");
    handler.SslOptions.ClientCertificates = [clientCertificate];
    handler.SslOptions.CertificateChainPolicy = new X509ChainPolicy
    {
        TrustMode = X509ChainTrustMode.CustomRootTrust,
        CustomTrustStore = { authority }
    };
})
```
`Build()` refuses a handler that keeps cookies or decompresses responses, naming the setting. `WithClientCredentials`
asks for its tokens through the same handler, so what is set here for the hub, a `ConnectCallback` included, applies to
the identity provider too; given an `HttpClient` of its own (its last parameter), it uses that one, which the caller
keeps and disposes.

### Serialization
MessagePack is the recommended default: it takes any POCO without attributes or schema. JSON is about 20 % slower
to produce in our benchmark, and can be sent uncompressed for debugging (`WithJsonSerialization(compress: false)`).
Both ends of a
transfer must use the same serializer and settings: a batch the receiver cannot decode fails the transfer.
Any other format is an `IBatchSerializer<T>` away.

The builder carries the item type, so a serializer that cannot carry it is not an overload that
exists: `new TransportRClientBuilder<string>().WithArrowSerialization()` does not compile, rather
than building a client that fails on its first batch.

## Contracts

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

### ByteSend / ByteReceive

The handles of a byte-stream transfer, which `ByteStreamClient` opens: raw bytes, no item type and no
serializer. `ByteSend` has `WriteAsync(ReadOnlyMemory<byte>)`, `CopyFromAsync(Stream)` and `CompleteAsync`; `ByteReceive` has
`ReadAsync(Memory<byte>)`, `CopyToAsync(Stream)` and `WaitForCompletionAsync`. Both are abstract, like `Send<T>` and `Receive<T>`, and
are disposed once the transfer has ended.

### Failures

A request the hub refuses or cannot carry out throws `HubRequestFailedException`, and a transfer that ends without
delivering its data throws `TransferFailedException`. Each carries a `Category`; the message stays generic, and the
detail is in the hub's log.

| `HubRequestFailedException.Category` | |
|---|---|
| `Refused` | An unknown target, one out of the caller's rights and one that is gone alike, on purpose; the caller's own registration refused; a route reserved to the host |
| `RateLimited` | A ceiling of the hub's (429); retry once a transfer has ended |
| `HandshakeTimedOut` | The handshake, or the hub's answer to the request, did not come within the handshake timeout |
| `TargetRefusedOrFailedToPrepare` | A client asked to take part refused, or its handler failed |
| `EndedDuringHandshake` | The hub ended the transfer before its handshake completed |
| `Other` | The hub's answer to a transfer request named no instance to go to, or no transfer |

| `TransferFailedException.Category` | |
|---|---|
| `EndedByHub` | The host, a reaper or a withdrawn right ended it, or the hub refuses its stream |
| `AbandonedByPeer` | The other end gave up; the message says why |
| `AcknowledgementTimedOut` | Nothing acknowledged for `WithAcknowledgementTimeout`, or, on a receiver, the transfer did not close within it |
| `HandleReleased` | This end's handle was released first |
| `UndecodableOrUnserializableBatch` | A batch this end could not serialize, or decode: both ends must use the same serializer and settings |
| `ProtocolViolation` | What arrived breaks the wire protocol |
| `Other` | `RequireWebSocket`, and the path to the hub carries no WebSocket |

A 409 from the hub whose category the client cannot read fails as an `HttpRequestException`
(`HttpRequestError.InvalidResponse`), not as a refusal.

## License

MIT
