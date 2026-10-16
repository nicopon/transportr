# TransportR

**Bulk data transfer through a centralized hub for .NET**

[![NuGet](https://img.shields.io/nuget/v/TransportR.svg)](https://www.nuget.org/packages/TransportR/)
[![.NET](https://img.shields.io/badge/.NET-10.0-512BD4)](https://dotnet.microsoft.com/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/nicopon/TransportR?quickstart=1)

> Stream millions of records between .NET applications with automatic batching, compression, and reliable delivery — no Kafka, no RabbitMQ, just a single .NET hub.

---

## 📦 Installation

```bash
# Client package (includes core)
dotnet add package TransportR.Client.SignalR

# Hub server package
dotnet add package TransportR.Hub.SignalR

# Serialization — pick one (MessagePack recommended for performance)
dotnet add package TransportR.Serialization.MessagePack  # Binary + LZ4 (fastest)
dotnet add package TransportR.Serialization.Protobuf     # Binary + Zstd
dotnet add package TransportR.Serialization.Json         # Human-readable (debugging)
dotnet add package TransportR.Serialization.Arrow        # Apache Arrow RecordBatch
```

---

## 🚀 Quick Start

### 1. Create the Hub (Server)

Any ASP.NET Core application can become a TransportR hub with just a few lines:

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

The hub picks no serializer: it forwards opaque bytes and never decodes a payload. Serialization
is a client-side choice, below.

### 2. Send Data (Sender Client)

```csharp
using TransportR.Client.SignalR;
using TransportR.Serialization.MessagePack;

// Connect to hub
var sender = new TransportRClientBuilder<Order>()
    .WithUrl("http://localhost:5252")
    .WithMessagePackSerialization()
    .Build();

await sender.ConnectAsync();

// Find a receiver and start transfer
var receivers = await sender.GetReceiversAsync();
var transfer = await sender.InitTransferAsync(receivers[0].ClientId);

// Stream data (auto-batched, auto-compressed)
foreach (var order in GetOrders())
    await transfer.SendAsync(order);

await transfer.CompleteAsync();
```

### 3. Receive Data (Receiver Client)

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

await receiver.ConnectAsync();
await Task.Delay(Timeout.Infinite); // Keep listening
```

---

## ✨ Features

| Feature | Description |
|---------|-------------|
| **Dumb Pipe Architecture** | Hub forwards bytes without deserialization — zero overhead |
| **Auto-batching** | Configurable batch size and timeout for optimal throughput |
| **Pluggable Serialization** | MessagePack (fastest), Protobuf, or JSON — with built-in LZ4/Zstd compression |
| **Reliable Delivery** | ACK-based with automatic retry and reordering |
| **Bounded Memory** | Opt-in backpressure at both ends — bound the receiver in items *and* bytes and a slow consumer costs time, not RAM |
| **Seamless Reconnection** | Automatic recovery from network interruptions |
| **Group Routing** | Logical groups for routing and access control |
| **High Availability** | Optional Redis state store + sticky sessions |

---

## 🔐 Security

TransportR integrates seamlessly with standard .NET authentication:

- **OIDC/OAuth 2.0** — Works with any identity provider (Azure AD, Keycloak, Auth0...)
- **Bearer token authentication** — Clients authenticate with JWT tokens
- **Group-based access control** — Restrict which senders can reach which receivers

**Quick start option:** For development or testing, use the included **SimpleIdp** project (`TransportR.Auth.SimpleIdp`, not published to NuGet) — a lightweight embedded identity provider based on OpenIddict. No external IdP required.

📖 See the [Security Architecture Guide](./SecurityArchitecture.md) for detailed configuration.

---

## 🤔 When to Use TransportR

TransportR shines when you need to move bulk data between applications in segmented networks:

```
Without TransportR:                    With TransportR:
┌────────┐     ┌────────┐              ┌────────┐     ┌─────┐     ┌────────┐
│Sender 1│────▶│Receiver│              │Sender 1│────▶│     │────▶│Receiver│
└────────┘     └────────┘              └────────┘     │     │     └────────┘
┌────────┐          ▲                  ┌────────┐     │ Hub │
│Sender 2│──────────┘                  │Sender 2│────▶│     │
└────────┘                             └────────┘     └─────┘

Firewall rules: N × M                  Firewall rules: N + M
```

### Perfect For

- ✅ **Network segmentation** — Sender and receiver in different zones
- ✅ **Centralized monitoring** — Single point for audit and observability
- ✅ **Minimal infrastructure** — No Kafka/RabbitMQ cluster to maintain
- ✅ **Batch processing** — ETL, data sync, bulk imports

### Not Ideal For

- ❌ **Multi-language ecosystems** — .NET only (no Python, Java, etc.)
- ❌ **Fan-out / Pub-sub** — Point-to-point transfers only
- ❌ **Persistent queues** — No hub-side storage (sender buffers until ACK)

### Comparison

| | gRPC Streaming | Kafka | **TransportR** |
|--|----------------|-------|----------------|
| **Use case** | Direct point-to-point | Event streaming | Hub-mediated bulk transfer |
| **Persistence** | None | ✅ Durable | ❌ (Sender buffers) |
| **Multi-language** | ✅ Excellent | ✅ Excellent | ❌ .NET only |
| **Infrastructure** | None | Kafka + ZK | Single .NET app |
| **Built-in compression** | ❌ Manual | Configurable | ✅ LZ4/Zstd |

---

## 📚 Documentation

| Guide | Description |
|-------|-------------|
| [Architecture](./TransportRArchitecture.md) | System design and data flow |
| [Resilience](./ResilienceArchitecture.md) | Reconnection and fault tolerance |
| [Security](./SecurityArchitecture.md) | OIDC, groups, and access control |

---

## 🏗️ Project Structure

```
TransportR/
├── src/                                # Libraries / NuGet packages
│   ├── TransportR/                     # Core interfaces & protocols
│   ├── TransportR.Client.SignalR/      # Client implementation & OAuth2 client credentials
│   ├── TransportR.Hub.SignalR/         # Hub server & group flow control
│   ├── TransportR.Serialization.MessagePack/
│   ├── TransportR.Serialization.Protobuf/
│   ├── TransportR.Serialization.Json/
│   ├── TransportR.Serialization.Arrow/
│   ├── TransportR.Auth.SimpleIdp/      # Embedded dev Identity Provider (non-packable)
│   └── TransportR.Storage.Redis/       # Redis state store
│
├── samples/                            # Working examples
│   ├── DataHub/                        # Sample hub
│   ├── Sender/                         # Sample sender
│   ├── Receiver/                       # Sample receiver
│   ├── DatabaseInitializer/            # Test data generator
│   ├── TestIdp/                        # Standalone IdP sample
│   └── Shared/                         # Shared sample code
│
└── tests/
    ├── TransportR.DocSnippets/       # Compiles the Quick Start snippets above
    ├── TransportR.TestProxy/         # YARP reverse proxy for the HA scenarios
    └── TransportR.Tests/
```

---

## 🔧 Configuration

The library reads no environment variable: a client is configured on its builder, a hub on
`AddDataHub()`. The variables used by `scripts/` belong to the benchmark samples, not to the
packages — see [`scripts/README.md`](./scripts/README.md).

| Builder call | Default | What it sets |
|---|---|---|
| `WithBatchSize(n)` | `1000` | Items per batch |
| `WithBatchTimeout(ms)` | `1000` | Flush a partial batch after this long |
| `WithMaxInFlightBatches(n)` | unbounded | Sender-side backpressure, in batches |
| `WithReceiveCapacity(items, bytes)` | unbounded | Receiver-side backpressure, first bound reached wins |

**Both backpressure bounds are opt-in and `Build()` warns when neither is set.** The right figure
depends on how large an item is, which only the caller knows; left unset, a consumer slower than
its producer is absorbed in memory rather than throttled.

---

## 🧪 Try It Locally

```bash
# Clone and initialize
git clone https://github.com/nicopon/TransportR.git
cd TransportR
./scripts/init.sh          # Generate 2M test orders

# Run the benchmark
./scripts/run-benchmark.sh default
```

---




## 📄 License

MIT License — see [LICENSE](LICENSE) for details.
