# TransportR

[![NuGet](https://img.shields.io/nuget/v/TransportR.svg)](https://www.nuget.org/packages/TransportR/)
[![.NET](https://img.shields.io/badge/.NET-10.0-512BD4)](https://dotnet.microsoft.com/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/nicopon/TransportR?quickstart=1)

**Move large record sets between .NET applications that cannot reach each other directly — through one hub
both sides call out to, resuming after network cuts, with bounded memory.**

TransportR is a library, not a broker: a sender streams typed records to one receiver, the hub relays the
bytes without decoding or keeping them, and the sender learns that the receiver acknowledged every batch.

---

## When it fits, and when it does not

**It fits** when applications sit in different network zones and need to exchange bulk data — ETL, data sync,
bulk imports — without opening a path between every pair of them:

```
Without a hub:                         With TransportR:
┌────────┐     ┌────────┐              ┌────────┐     ┌─────┐     ┌────────┐
│Sender 1│────▶│Receiver│              │Sender 1│────▶│     │◀────│Receiver│
└────────┘     └────────┘              └────────┘     │     │     └────────┘
┌────────┐          ▲                  ┌────────┐     │ Hub │
│Sender 2│──────────┘                  │Sender 2│────▶│     │
└────────┘                             └────────┘     └─────┘

The receiver must be reachable         Every application connects out to the hub;
by every sender: N × M paths.          only the hub is reachable: N + M paths.
```

**It does not fit** when you need:

- **Other languages than .NET** — the client exists for .NET only.
- **Fan-out or pub/sub** — a transfer has one sender and one receiver.
- **Persistence or replay** — the hub stores nothing; until the receiver acknowledges a batch, only the
  sender holds it. Use Kafka or RabbitMQ for that.

### Compared with what you may use today

| | gRPC streaming | Outbound relay + gRPC | File drop (SFTP, MFT) | Kafka | **TransportR** |
|--|--|--|--|--|--|
| **Must be reachable** | The server end | The relay; both ends connect out to it | The file server | The brokers | The hub |
| **Data at rest in between** | None | None | Files, until picked up and deleted | Retained per topic | None |
| **Receiver starts consuming** | As data is sent | As data is sent | Once the file is complete | As data is produced | As data is sent |
| **Sender learns delivery** | If the application builds it | If the application builds it | No — only that the upload ended | No — only that the brokers have it | Yes — every batch is acknowledged by the receiver |
| **After a network cut** | Up to the application | Up to the application | Per file, where client and server support it | From the consumer's committed offset | From the receiver's last batch |
| **Replay, fan-out** | No | No | Several readers can fetch a file | Yes | No |
| **Languages** | Many | Many | Any | Many | .NET only |
| **To operate** | Nothing | A relay: a managed service (Azure Relay Hybrid Connections) or a reverse tunnel you host | A file server | A cluster | One ASP.NET Core app (+ Redis and a sticky load balancer for HA) |

An outbound relay that carries gRPC gives the same N + M topology, with outbound connections only. What it leaves
to each application is what TransportR is for: an acknowledgement for every batch, resumption from the receiver's
last batch, a memory bound at each end for a caller that sets one, and groups that decide who may send to whom.

---

## Quick Start

```bash
dotnet add package TransportR.Hub.SignalR                # the hub
dotnet add package TransportR.Client.SignalR             # each application
dotnet add package TransportR.Serialization.MessagePack  # a serializer, the same at both ends
```

### 1. The hub

Any ASP.NET Core application can host it:

```csharp
// Program.cs
using TransportR.Hub.SignalR;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDataHub()
    .UseDevMode()   // trusts client-provided identity — see the Security guide before production
    .Build();

var app = builder.Build();
app.MapDataHub();
app.Run();
```

The hub picks no serializer: it forwards opaque bytes and never decodes a payload.

### 2. A sender

```csharp
using TransportR.Client.SignalR;
using TransportR.Serialization.MessagePack;

// Connect to hub
var sender = new TransportRClientBuilder<Order>()
    .WithUrl("http://localhost:5252")
    .WithMessagePackSerialization()
    .Build();

// In dev mode a client names its groups; with authentication they come from its token.
await sender.ConnectAsync(["senders"]);

// Find a receiver and start transfer
var receivers = await sender.GetReceiversAsync();
await using var transfer = await sender.InitTransferAsync(receivers[0].ClientId);

// Stream data (auto-batched, auto-compressed)
foreach (var order in GetOrders())
    await transfer.SendAsync(order);

await transfer.CompleteAsync();
```

`CompleteAsync` returns once the receiver has acknowledged every batch; it throws `TransferFailedException`
otherwise.

### 3. A receiver

```csharp
using TransportR.Client.SignalR;
using TransportR.Serialization.MessagePack;

var receiver = new TransportRClientBuilder<Order>()
    .WithUrl("http://localhost:5252")
    .WithMessagePackSerialization()
    .Build();

receiver.OnTransferStarted += async transfer =>
{
    // The handler owns the handle and must release it. The loop below ends only once the
    // transfer has reached its outcome, not at the last item, so releasing right after it
    // is correct.
    await using (transfer)
    {
        await foreach (var order in transfer.ReceiveAsync())
        {
            Console.WriteLine($"Received: {order.Id}");
        }
    }
};

await receiver.ConnectAsync(["receivers"]);
await Task.Delay(Timeout.Infinite); // Keep listening
```

---

## What it guarantees, and how that is checked

Each behaviour below is exercised by a scenario that runs real processes — hub, sender, receiver, and where
needed an identity provider, a fault-injecting proxy or a second hub — and asserts its outcome: the exact
number of rows received (2,000,000), the sender's own report that every batch was acknowledged, and, where
stated, a memory ceiling. `./scripts/test-all.sh` runs them all; see [`scripts/README.md`](./scripts/README.md).

| Behaviour | Checked by |
|---|---|
| **Resumes after network faults** from the receiver's last batch, without loss or duplicates | `chaos` (bandwidth cap, latency, connection resets and timeouts injected mid-transfer by Toxiproxy), `chaos-cut-sender`, `chaos-cut-receiver` (one side's link held silent, then closed) |
| **Bounded memory** against a slow consumer, when bounds are set | `slow-receiver`: receiver under its 300 MB ceiling (182–193 MB measured), where the same run without the byte bound reaches 870 MB |
| **Authentication on both planes**, groups taken from token scopes | `oidc`, `embedded-idp`; `data-plane-probe` asks the live hub to serve anonymous and wrong-role stream requests and expects refusals |
| **Two hubs behind a load balancer**, with affinity handoff | `ha-redis` (Redis state store), `ha-no-shared-state` |
| **This page's Quick Start** | Compiled by `tests/TransportR.DocSnippets`; its hub configuration runs in an integration test |

Unit and in-process integration tests run in CI; the scenarios run on demand.

### Performance, and its conditions

On one machine over loopback, the `default` scenario moves 2 million orders (about 500 bytes each as JSON) at
450,000 to 630,000 items per second on the sender side, whichever of MessagePack, Protobuf or JSON carries them
(five interleaved runs each). The sample receiver, which writes every row to CSV, holds the end-to-end rate near
135,000 items per second. Over a real network, bandwidth and item size decide.

One transfer has a ceiling of its own: its sender serializes on one worker, and its data crosses one HTTP stream on
each hop, sender to hub and hub to receiver. Over a long round trip, one TCP connection's window bounds the rate,
and nothing spreads a transfer over several connections. No scenario measures that rate under latency yet: `chaos`
injects latency and asserts that the transfer completes, not how fast.

---

## Configuration

The library reads no environment variable: a client is configured on its builder, a hub on `AddDataHub()`.
The variables used by `scripts/` belong to the samples, not to the packages.

| Builder call | Default | What it sets |
|---|---|---|
| `WithBatchSize(n)` | `1000` | Items per batch |
| `WithBatchTimeout(ms)` | `1000` | Flush a partial batch after this long |
| `WithMaxInFlightBatches(n)` | unbounded | Sender-side backpressure, in batches |
| `WithReceiveCapacity(items, bytes)` | unbounded | Receiver-side backpressure, first bound reached wins |

**Both backpressure bounds are opt-in, and `Build()` warns when neither is set.** The right figure depends on
how large an item is, which only the caller knows. Set both: either one alone moves the backlog to the other
process.

### Serialization

| Package | Format | Notes |
|---|---|---|
| `TransportR.Serialization.MessagePack` | Binary + LZ4 | Any POCO, no attributes or schema needed. Recommended default |
| `TransportR.Serialization.Protobuf` | Binary + Zstd | Needs annotated contracts; the most compact payload |
| `TransportR.Serialization.Json` | Text + Zstd | `compress: false` puts plain JSON on the wire, for debugging |
| `TransportR.Serialization.Arrow` | Arrow IPC + LZ4 | Apache Arrow `RecordBatch` only; bound the receiver in bytes |

MessagePack and Protobuf are on par in our benchmark; JSON is about 20 % slower to produce. Both ends of a
transfer must use the same serializer and settings: a batch the receiver cannot decode fails the transfer.

### Security

- **OAuth 2.0 / OpenID Connect** — works with any provider issuing JWT access tokens with the standard `scope`
  claim (tested with OpenIddict and IdentityServer). One that carries rights elsewhere, such as Entra ID app
  roles, is supported through your own `IClientIdentityProvider`.
- **Token renewal** — a client with a token provider renews ahead of expiry; the hub closes connections whose
  token ran out and ends their transfers.
- **Group access rules** — `transportr:group:<name>` scopes put a client in groups, and `DefaultFlowControlService`
  says which group may send to which. Without a flow control service, any group reaches any other.

For development, `TransportR.Auth.SimpleIdp` (not published to NuGet) embeds an OpenIddict provider in the
hub. See the [Security guide](./SecurityArchitecture.md).

---

## Project status

- **Preview (0.x).** The API can still change between minor versions. One downstream project integrates it so far:
  [DtPipe](https://github.com/nicopon/dtpipe), a CLI that streams, transforms and anonymizes data across databases
  and file formats.
- **The hub is on the path.** All traffic crosses it. High availability needs the Redis state store and
  effective session affinity at the load balancer, not merely preferred affinity
  (see [Architecture](./TransportRArchitecture.md#high-availability-ha)).
- **Tested on a limited set of environments**: one reverse proxy (YARP), HTTP/1.1 to the hub, two identity
  providers, and the scenarios above on developer machines. Reports from other environments are welcome.

---

## Documentation

| Guide | Covers |
|-------|-------------|
| [Architecture](./TransportRArchitecture.md) | Control plane and data plane, wire protocol, HA |
| [Resilience](./ResilienceArchitecture.md) | Reconnection, resumption, backpressure, how a transfer ends |
| [Security](./SecurityArchitecture.md) | Authentication, groups, access rules, token renewal |
| [Scripts](./scripts/README.md) | The scenarios, and what each one asserts |

### Repository layout

```
src/        Libraries (NuGet packages): core, client, hub, serializers, Redis store, dev IdP
samples/    DataHub, Sender, Receiver, TestIdp, DatabaseInitializer, Shared
tests/      Unit and integration tests, Quick Start snippets, the YARP proxy of the HA scenarios
scripts/    Build, data generation, scenarios
```

### Try it locally

```bash
git clone https://github.com/nicopon/TransportR.git
cd TransportR
./scripts/init.sh                    # downloads Toxiproxy, generates 2M test orders
./scripts/run-benchmark.sh default
```

---

## License

MIT — see [LICENSE](LICENSE).
