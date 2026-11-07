# TransportR - Architecture Overview

> Bulk data transfer through a centralized hub for segmented network environments.

---

## Overview

**TransportR** transfers bulk data between applications through a central hub. Its primary value is simplifying network topology: instead of N×M direct connections, all clients connect only to the hub (N+M connections).

### Core Projects

```
TransportR/                              # Interfaces, protocols, abstractions
TransportR.Client/                       # Control requests, event stream and HTTP streaming client & OAuth2 auth
TransportR.Hub/                          # HTTP API server (control and data planes) & group flow control
TransportR.Serialization.MessagePack/    # MessagePack + Lz4 serializer
TransportR.Serialization.Protobuf/       # Protobuf + Zstd serializer
TransportR.Serialization.Json/           # System.Text.Json + Zstd serializer (uncompressed for debugging)
TransportR.Serialization.Arrow/          # Apache Arrow RecordBatch serializer
TransportR.Auth.SimpleIdp/               # Lightweight Identity Provider (dev/test)
TransportR.Storage.Redis/                # Redis State Store implementation
```

### Key Characteristics

| Aspect | Description |
|--------|-------------|
| **Network Simplification** | Single ingress point for all transfers |
| **Dumb Pipe Architecture** | Hub forwards variable-length opaque bytes without deserialization |
| **Reliable Delivery** | Packet-level ACK with automatic retry |
| **Group-based Access** | Route transfers by logical groups |
| **Observability** | Hub-side byte counting via `IHubProgressMonitor` |

---

## Detailed Architecture

### Data Flow

```mermaid
sequenceDiagram
    participant S as Sender
    participant H as DataHub
    participant R as Receiver

    R->>H: 1. GET /api/client/{id}/events (registers the receiver)
    S->>H: 2. GET /api/client/{id}/events (registers the sender)
    S->>H: 3. GET /api/client/{id}/receivers
    H-->>S: [Receiver]
    S->>H: 4. POST /api/client/{id}/transfers (ReceiverId)
    H->>R: event ready-to-receive (TransferId)
    R-->>H: POST /api/client/{id}/replies/{requestId}
    H-->>S: TransferId (the sender starts its session from it)
    
    loop Data Transfer
        S->>H: Batch (header + payload)
        H->>R: Forward Batch (dumb-pipe)
        R-->>H: ACK
        H-->>S: ACK
    end
    
    S->>R: Complete (in-stream)
    R-->>S: CompleteAck (in-stream)
    
    Note over S,R: 🔌 Streams close via EOF (pure dataplane)
    S->>H: end of the data stream
    H->>R: end of the stream
```

> **Note**: Transfer completion uses pure dataplane signals (the end of a stream), not the control plane.
> See [ResilienceArchitecture.md](./ResilienceArchitecture.md#transfer-completion-eof-domino-sequence) for details.

### Control Plane vs Data Plane

TransportR clearly separates responsibilities between coordination and transfer:

1. **Control Plane (HTTP requests and server-sent events)**
    - **Role**: "The Coordinator"
    - **Methods**: a client opens `GET /api/client/{id}/events`, a stream of server-sent events by which the hub reaches it, and asks the hub for the rest with ordinary requests under the same path (see *Communication Channels*)
    - **Responsibility**: Manages presence, lightweight signaling, and state synchronization. It negotiates the transfer parameters but never touches the bulk data.

2. **Data Plane (HTTP Streaming)**
    - **Role**: "The Pipe"
    - **Methods**: `GET` (a WebSocket) or `POST` (segments) `/api/stream/{id}/send`, `GET /api/stream/{id}/receive`, and `POST /api/stream/{id}/abort` for the one case a stream cannot carry (see *Abort request* below)
    - **Responsibility**: Moves batches of data. The **Hub** acts as a **Dumb Pipe**, forwarding opaque binary chunks from Sender to Receiver without inspecting or deserializing the payload.

### Communication Channels

| Channel | Transport | Usage |
|---------|-----------|-------|
| **Control plane: events** | HTTP GET, server-sent events (`/api/client/{id}/events`) | Hub → Client. Opening it registers the client and its end marks the client disconnected. Events: `registered` (first, at once), `keepalive`, `ready-to-receive`, `request-send`, `cancel-transfer` |
| **Control plane: requests** | HTTP (`/api/client/{id}/…`) | Client → Hub: `GET receivers` (discovery), `POST transfers` (open a transfer), `POST replies/{requestId}` (answer to an event that asks), `POST presence/{connectionId}`, `POST transfers/{transferId}/heartbeat`, `POST transfers/{transferId}/ping` (the probe before a session reopens its streams) |
| **Data Send** | WebSocket (`GET /api/stream/{id}/send`), or HTTP POST, one segment per request (`POST` on the same path); Header: `X-Connection-Type: Data` | Sender → Hub |
| **Data Receive** | HTTP GET (`/api/stream/{id}/receive`, Header: `X-Connection-Type: Data`) | Hub → Receiver |
| **Control Send** | WebSocket or HTTP POST segments, as Data Send (Header: `X-Connection-Type: Control`) | ACKs Receiver → Hub |
| **Control Receive** | HTTP GET (Header: `X-Connection-Type: Control`) | ACKs Hub → Sender |
| **Abort request** | HTTP POST (`/api/stream/{id}/abort`) | Participant → Hub, when its in-stream `Abort` cannot be written because the data in front of it fills the stream; the hub ends the transfer |
| **Token renewal** | HTTP PUT (`/api/client/{id}/token`) | Client → Hub, ahead of the token's expiry |

Every control request names the client in its path and carries the token the client holds at that moment; with
authentication on, it is refused with a 404 unless the path's `clientId` is the identity that token attests.
The events stream is HTTP/1.1 like the data streams (below); a reverse proxy must forward it as it is written
rather than buffer it, and keep its idle timeout above the hub's keepalive interval (*Key Timings* in
`ResilienceArchitecture.md`).

### Data Plane Transport: HTTP/1.1, WebSocket or Segments

A write stream, the sender's Data or the receiver's Control, goes over a **WebSocket** (one `GET /api/stream/{id}/send`
with an upgrade) where the path to the hub carries one, and over **segments** (a succession of short
`POST /api/stream/{id}/send` requests) otherwise. The client picks at each opening (`WithWriteTransport`):
`PreferWebSocket`, the default, falls back; `RequireWebSocket` fails the transfer where there is no WebSocket;
`SegmentsOnly` never asks. Read streams are long GETs either way. Above the transport nothing differs (`Ping`/`Pong`,
resumption, the end sequence), and the hub copies the bytes of both unread: what it reads is the transport's own, an
epoch and an offset, a WebSocket's probe and `Close` status.

**A WebSocket opens in this order.**

1. The client sends the upgrade with `X-Connection-Type`, the token, the affinity cookie and `heartbeatMs` (the presence
   interval, which every WebSocket announces).
2. The hub answers `426` to a request that is not an upgrade (a front end did not relay it; logged as a warning), `400`
   to an unreadable `X-Connection-Type`, `404` with `TransportR-Refused` to an unknown transfer or a caller who is not
   its participant, `400` to a missing `heartbeatMs`; otherwise it accepts. The route brings `UseWebSockets` with it:
   the host adds nothing to its pipeline.
3. **The proof.** The client sends an empty `Text` message, the hub answers with one, and the upgrade and the proof
   together have 5 s. A front end can accept an upgrade and still hold bytes back; this is the one test of the
   client-hub hop that depends neither on the peer nor on a hub parked on a slow one, and a path that carries it and
   holds bytes back later is not detected by it. The hub reads that message's type and nothing else: the socket then
   carries `Binary` messages only, copied to the bridge unread.
4. **The first byte.** The socket enters the bridge only when it carries one, as an epoch does with its first segment,
   or a side with nothing to say yet would cut its peer's stream at each reconnection of the peer and be cut in turn.
   It replaces the source the bridge holds, a segment epoch included, whose late segments are answered `409`.
5. **The stream.** A flush is one binary message of at most `StreamBufferSize` (512 KB) and waits for no answer: a peer
   that reads slowly fills the socket, which blocks the flush. That is the backpressure. Each end sends a keepalive
   frame every presence interval, with no keepalive timeout, which would abandon a sound stream whenever the hub is
   parked on a slow peer.

**Whatever is neither a verdict of the hub nor a proven WebSocket reads as a path that does not carry one.** `404` with
`TransportR-Refused` and `410` are the transfer gone; `401` renews the token and opens once more. Any other answer than
`101` (a `404` without that header, `200`, `403`, `405`, `421`, `426`, `5xx`), a connection cut before the answer, no
answer within 5 s, and a `101` whose proof goes unanswered or is cut are the path. The client then uses segments until
the session ends, in the same attempt and before the first byte, and says why in its log (a warning) and in
`TransferInfo.WriteTransportFallback`; the next transfer tries a WebSocket again. Under `RequireWebSocket` the transfer
fails instead, with a message that names no path or setting. A path that passes no segment either fails on the first
one, and the session reconnects as after any cut.

A cut of a proven WebSocket is reopened as any cut is (probe, new opening, `Ping`/`Pong`), over a WebSocket again. A path
that cuts every one, on a frame size or a firewall, would have the session reconnect for ever where segments pass:
three cuts in a row, each within 30 s of its opening, read as the path too, and a connection that lives 30 s clears the
count. A refusal is not a cut.

**A WebSocket ends in one of three ways.**

- **Cleanly**: `Close(4000)` in each direction, the client's after its last flush and the hub's once the bridge has
  closed the stream. A private status, because that end retires the transfer: a front end that cuts with a `Close` of
  its own, 1000 or 1001, has to read as the cut it is. The hub reads any other `Close` as a cut.
- **Refused**: `Close(4409)` from the hub, what a `409` says to a segment (the stream was replaced, its peer was lost,
  the transfer is over), read as retryable and refused. It goes at the client's next write, not at once, as a `409`
  does: sent sooner, a client whose peer reconnected would reconnect in turn and cut that peer. A transfer gone is a
  `404` at the reopening, as for an epoch.
- **Cut**: the connection fails, or closes with another status. The client keeps a read pending for this, since a
  `Close` nobody reads leaves its writes succeeding as if nothing had happened.

**Segments** are the fallback, and pass wherever a request does. A front end that buffers a request body holds back the
few bytes a stream ends on, an acknowledgement or a `Pong`, until the request is over; a short request is over at once.
Each segment carries the next bytes of one opening of the stream, its **epoch**, at their **offset** in it (`DataPlane`).
The hub reads the epoch and the offset from the query, never a byte of the body.

- **One segment in flight.** A flush is a segment, of at most `StreamBufferSize` (512 KB). The hub puts the segments
  of an epoch end to end into the stream a bridge copies from and answers one once the peer has it, so a peer that
  reads slowly slows the flushes down. A segment beyond what the hub has taken is refused, nothing is reordered, and
  the hub keeps no byte past the request. A sender's rate is bounded by one segment per round trip.
- **The answer.** `200`: all of the segment reached the peer. `202`: the hub held it for 30 s, a fixed bound under the
  60 s a default nginx allows a request without an answer, and the peer took only part of it; the client sends the
  rest again, in the same epoch, and the hub skips what it relayed already. Both carry `TransportR-Relayed`, how many
  bytes of the epoch have reached the peer. `409`: the hub holds no epoch this segment follows on from (it was
  replaced or is over, or the segment reached an instance other than the one holding it, which is not a `421`: bridges
  are local to an instance); the client opens a new epoch and the session reconnects as after any cut. `401` took
  nothing: the client renews its token and sends the segment again. `404` and `410` are the transfer gone. On any
  refusal, of a segment or a WebSocket, the attempt ends only once its read stream has, 2 s at most: what the hub
  relayed before refusing, a sender's `Abort` for one, is read first.
- **The end.** The last segment carries `final=true` and goes after the `CompleteAck`. The hub answers it once the
  bridge has closed the stream (*Transfer Completion* in `ResilienceArchitecture.md`).

Both transports are HTTP/1.1, and the client says so on every request, an upgrade included (`HttpVersion.Version11`,
`RequestVersionExact`, in `HttpStreamManager`). It is a decision, not a default left alone:

- **HTTP/2 has nothing to offer this traffic.** A participant holds one long GET per transfer and writes on one socket,
  or one request at a time: there is nothing to multiplex and no request header worth compressing.
- **HTTP/2 caps an upload at its flow-control window.** With Kestrel's default windows, one continuous
  sender-to-hub stream reached 16 MB/s at 20 ms round-trip and 4.7 MB/s at 80 ms (h2c, loopback behind a
  latency-injecting TCP proxy). A hub host can raise `Http2.InitialStreamWindowSize`; HTTP/1.1 needs no such tuning.
  That was measured on a continuous upload: segments under HTTP/2 have not been, and a WebSocket over HTTP/2
  (RFC 8441) is excluded.

For a deployment:

- Kestrel may offer HTTP/2 to other clients; TransportR clients do not ask for it. A reverse proxy that speaks HTTP/2
  to the hub is not covered: that hop has not been validated.
- A reverse proxy has to do what the control plane asks of it already: answer short requests in full, and relay a GET
  as it is written rather than buffer it (`X-Accel-Buffering: no`, which nginx honours; the hub sends the headers
  before it has a byte to relay).
- For a WebSocket it also has to relay the upgrade (`Upgrade` and `Connection` on `GET /api/stream/{id}/send`) and the
  bytes of the socket as they come, both ways. Where the reference proxy strips the two headers the hub answers `426`
  and the client uses segments at once; where it accepts the upgrade and relays nothing, the client gives up on the
  proof after 5 s.
- With segments, a request body is bounded by the proxy's limit: nginx's `client_max_body_size` is 1 MB by default,
  which a 512 KB segment fits and a 2 MB one does not (413), so `StreamBufferSize` stays under it behind such a proxy.
- A GET must carry a byte, and a segment must be answered, before the proxy's idle limit: 60 s for a default nginx,
  240 s for App Service. A sender with nothing to write sends a `Ping` every presence interval (15 s): a byte on the
  receiver's data GET, and its `Pong` one on the sender's control GET. A WebSocket's keepalive frames go at the same
  interval; whether a front end counts them as traffic has not been measured.
- Validated with segments: App Service (Linux, `sitecontainers`, `http20Enabled=false`, `http20ProxyFlag=0`) with a
  transfer of 200 items, one of 2 million and one of 460 s to a throttled receiver, each complete to the item with no
  408 or 499 on `/send`; the last had each end's streams cut and reopened twice, for a cause not established. Also a
  stock nginx (the `nginx-default` scenario) and the reference proxy in `tests/TransportR.TestProxy` (YARP). A WebSocket
  has been carried on loopback only: directly, through the reference proxy, which relays the upgrade and the affinity
  cookie, and through Toxiproxy for the cuts. The `nginx-default` scenario, a `proxy_pass` that sets neither header, has
  not been run with one, and what a stock nginx, App Service or a corporate proxy answers has not been measured.

### Resilience & Reliability

TransportR utilizes a **Dataplane First** resilience strategy:

- **Stream Handshake (Ping/Pong)**: After a cut, the client probes the hub with a request, then reopens its streams once it answers; a Ping/Pong on the new data stream tells the sender where the receiver stands.
- **Packet-Level Resumption**: Receiver indicates the exact offset to resume from, eliminating the need for Hub-side state (Checkpoints).
- **Sender-side Buffering**: Payloads are kept until End-to-End ACK is received.
- **Hub Lifecycle Management**: Hub terminates transfers that go silent (timeout), and, if configured, those that stop progressing.

See [ResilienceArchitecture.md](./ResilienceArchitecture.md) for the complete design.

### Security

Authentication and Authorization are handled via OAuth2/JWT with group-based scoping (`transportr:group:*`).
See [SecurityArchitecture.md](./SecurityArchitecture.md) for the security model.

### High Availability (HA)

TransportR supports horizontal scaling (Active/Active) for Hubs:

1. **Shared State**: Client registrations and transfer sessions are stored in Redis
   (`TransportR.Storage.Redis`) -> Used for registration, discovery and routing.
2. **Sticky Sessions**: Required at the Load Balancer level. A client has an affinity of its own, that of the instance holding its events stream, which its registration, discovery, presence and renewal requests carry. A transfer has another, that of the instance holding its bridge, which its handshake, its replies, its heartbeats, its probe and its data streams carry. The client sets the `Cookie` header request by request, and its HTTP handler holds no cookie container, which would override it.
3. **Affinity Handoff**: A sender whose receiver's events stream is held by another instance is answered `421` with the cookie that reaches that instance, and sends that one `InitTransfer` request again with it. Its own events stream does not move, and neither do its other transfers. A stream opened without a cookie whose response sets one is opened once more carrying it, since the registration would otherwise record no instance.
4. **TestProxy**: A YARP-based reference implementation is provided for testing HA locally; `ha-transfer-affinity` is the scenario that exercises the redirection and the affinity of each transfer.

**What is *not* shared, and why sticky sessions are a requirement rather than an optimisation**:
the per-transfer progress counters (`TransferState`: items sent, acks, last activity) live in a
dictionary local to the hub instance that registered the transfer or accepted one of its streams,
never in `IStateStore`. `TransferCleanupService` kills a transfer on those counters, so a deployment
where a transfer's heartbeats and streams reach different instances gives each one a half-silent
transfer and invites both to reap it. Moving the
counters into Redis would buy a case the deployment already excludes, at the price of a write per
heartbeat — so the constraint is written here instead: **HA requires effective affinity, not merely
preferred affinity.**

---

## Core Interfaces

| Interface | Purpose | Location |
|-----------|---------|----------|
| `IDataSender<T>` | Initiate transfers, discover receivers | [ITransportRClient.cs](src/TransportR/Interfaces/ITransportRClient.cs) |
| `IDataReceiver<T>` | Receive transfers via event | [ITransportRClient.cs](src/TransportR/Interfaces/ITransportRClient.cs) |
| `IBatchSerializer<T>` | Pluggable serialization | [IBatchSerializer.cs](src/TransportR/Interfaces/IBatchSerializer.cs) |
| `IHubProgressMonitor` | Hub-side monitoring (bytes only) | [IHubProgressMonitor.cs](src/TransportR/Interfaces/IHubProgressMonitor.cs) |

> The hub operates as a "dumb-pipe" and only tracks bytes.

## Observability

**The packages emit no metrics of their own.** Two hooks are provided and the host decides what to
do with them: `IHubProgressMonitor` on the hub side, and the `TransferInfo` records
`GetTransfers()` returns on the client side. `samples/DataHub` builds its own OpenTelemetry meter
on top of the first — copy it rather than expect one from the package.

---

## Binary Protocol

### Wire Format (v2)

The transport protocol uses a **30-byte fixed header** followed by an **opaque binary payload**. The payload serialization is completely decoupled via `IBatchSerializer<T>`.

```
┌──────────────────────────────────────────────────────────────┐
│ ChunkHeader (30 bytes fixed)                                 │
├──────────────────────────────────────────────────────────────┤
│ Version  │ Type │ PayloadLen │ BatchId │ Count │ TransferId │
│ 1 byte   │ 1 b  │ 4 bytes    │ 4 bytes │ 4 b   │ 16 bytes   │
├──────────────────────────────────────────────────────────────┤
│ Payload (variable length, opaque bytes)                      │
│ ← Serialized by IBatchSerializer<T> implementation           │
└──────────────────────────────────────────────────────────────┘
```

### ChunkType Values

| Type | Value | Description |
|------|-------|-------------|
| Batch | 0x01 | Data payload (serialized items) |
| BatchAck | 0x02 | Batch acknowledgment |
| Complete | 0x03 | Transfer complete signal (requires CompleteAck) |
| CompleteAck | 0x04 | Acknowledgment of Complete (ensures reliable termination) |
| Ping | 0x05 | Connection probe (sender → receiver) |
| Pong | 0x06 | Connection probe response; `BatchId` carries the receiver's next expected batch, the resume point |
| Abort | 0x07 | One end gave up; `BatchId` carries an `AbortReason` |
| StreamBatch | 0x08 | Data payload of a transfer without a serializer (raw bytes); `Count` is the payload's size in bytes |

> **Architectural Note:** The transport layer is **serializer-agnostic**. The `IBatchSerializer<T>` interface allows pluggable implementations (MessagePack, JSON, ProtoBuf, Arrow). It is typed so that a client built on a serializer that cannot carry its item type does not compile. A batch body the receiver cannot decode fails its session instead of being resent: it arrived whole, and a resend would decode no better.

---

## Integration

See [README.md](./README.md) for code examples (Server, Sender, Receiver).

---

## Tests

Unit and integration tests cover the critical components:

- `BatchAccumulator` — Flush strategies, batch IDs
- `ReceiverState` — Ordering, duplicates
- `ChunkIO` — Binary protocol read/write
- `FlowControlService` — Group permissions, wildcards
- `StateStore` — InMemory + Redis (a container, or a local `redis-server`)
- `TransferManager` — Lifecycle (No persistence)

Run: `dotnet test`

---



## Performance

The measured rates, their conditions and what caps one transfer are in the README, under
["Performance, and its conditions"](./README.md#performance-and-its-conditions).

Run benchmarks: `./scripts/run-benchmark.sh`

---
