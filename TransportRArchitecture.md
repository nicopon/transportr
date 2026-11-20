# TransportR - Architecture Overview

> Bulk data transfer through a centralized hub for segmented network environments.

---

## Overview

**TransportR** transfers bulk data between applications through a central hub. Its primary value is simplifying network topology: instead of N×M direct connections, all clients connect only to the hub (N+M connections).

### Core Projects

```
TransportR/                              # Shared contract: routes, client description, write transport
TransportR.Client/                       # Control requests, event stream and HTTP streaming client & OAuth2 auth
TransportR.Hub/                          # HTTP API server (control and data planes) & group flow control
TransportR.Serialization.MessagePack/    # MessagePack + Lz4 serializer
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
| **Group-based Access** | Groups taken from token scopes decide which client may send to which |
| **Observability** | Hub-side byte counting and write transports via `IHubProgressMonitor` |

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

TransportR separates coordination from transfer. Every route of either plane is under *Communication Channels*.

1. **Control plane, "the coordinator"** (HTTP requests and server-sent events). A client opens its events stream, by
   which the hub reaches it, and asks the hub for the rest with ordinary requests under the same path. This plane
   manages presence, lightweight signalling and state synchronization, and negotiates each transfer, but never touches
   the bulk data.
2. **Data plane, "the pipe"** (HTTP streaming). It moves the batches. The hub acts as a **dumb pipe**: it forwards opaque
   binary chunks from sender to receiver without inspecting or deserializing the payload.

### Communication Channels

| Channel | Transport | Usage |
|---------|-----------|-------|
| **Control plane: events** | HTTP GET, server-sent events (`/api/client/{id}/events`) | Hub → Client. Opening it registers the client and its end marks the client disconnected. Events: `registered` (first, at once), `keepalive`, `ready-to-receive`, `request-send`, `cancel-transfer` |
| **Control plane: requests** | HTTP (`/api/client/{id}/…`) | Client → Hub: `GET receivers` (discovery), `POST transfers` (open a transfer), `POST replies/{requestId}` (answer to an event that asks), `POST presence/{connectionId}`, `POST transfers/{transferId}/heartbeat`, `POST transfers/{transferId}/ping` (the probe before a session reopens its streams) |
| **Data Send** | WebSocket (`GET /api/stream/{id}/send`), or HTTP POST, one segment per request (`POST` on the same path); Header: `X-Connection-Type: Data` | Sender → Hub |
| **Data Receive** | HTTP GET (`/api/stream/{id}/receive`, Header: `X-Connection-Type: Data`) | Hub → Receiver |
| **Control Send** | WebSocket or HTTP POST segments, as Data Send (Header: `X-Connection-Type: Control`) | ACKs Receiver → Hub |
| **Control Receive** | HTTP GET (Header: `X-Connection-Type: Control`) | ACKs Hub → Sender |
| **Abort request** | HTTP POST (`/api/stream/{id}/abort`) | Participant → Hub, when its in-stream `Abort` cannot be written; the hub ends the transfer ("6. The Abort Request" in `ResilienceArchitecture.md`) |
| **Token renewal** | HTTP PUT (`/api/client/{id}/token`) | Client → Hub, ahead of the token's expiry |
| **Write-transport probe** | WebSocket, or an empty HTTP POST (`/api/stream/probe/send`) | Client → Hub, after each registration when enabled; no transfer behind it (below) |

Every control request names the client in its path, which must be the identity its token attests ("Security
Guarantees" in `SecurityArchitecture.md`). The events stream is HTTP/1.1 like the data streams (below), and is relayed
as they are ("Deploying the hub" in `src/TransportR.Hub/README.md`).

### Data Plane Transport: HTTP/1.1, WebSocket or Segments

A write stream is the sender's Data or the receiver's Control. It goes over a **WebSocket** (one
`GET /api/stream/{id}/send` with an upgrade) where the path to the hub carries one, and over **segments** (a succession
of short `POST /api/stream/{id}/send` requests) otherwise. Read streams are long GETs either way. The client picks at each
opening, by its policy (`WithWriteTransport`):

- `PreferWebSocket`, the default, falls back on segments;
- `RequireWebSocket` fails the transfer where there is no WebSocket;
- `SegmentsOnly` never asks for one.

Above the transport nothing differs (`Ping`/`Pong`, resumption, the end sequence). The hub copies the bytes of both
transports unread: what it reads is the transport's own, an epoch and an offset, a WebSocket's probe and `Close` status.

**The write-transport probe informs and decides nothing.** With `WithWriteTransportProbe()` (off by default), a client
opens a write stream of the same form with no transfer behind it, in the background after each registration
(`/api/stream/probe/send`, naming its events stream in `connection`). It tries a WebSocket and its proof, else an empty
`POST`, as the policy allows, judged as an opening is. The hub announces what reached it, before any transfer
(*Observability*). Each transfer still finds its own transport when its stream opens, and the proof does not see a
front end that holds bytes back after it.

**A WebSocket opens in this order.**

1. The client sends the upgrade with `X-Connection-Type`, the token, the affinity cookie and `heartbeatMs` (the presence
   interval, which every WebSocket announces).
2. The hub accepts the upgrade unless it refuses it:

   | Request | Answer |
   |---|---|
   | not an upgrade (a front end did not relay it) | `426`, logged as a warning |
   | `X-Connection-Type` unreadable | `400` |
   | `heartbeatMs` missing, under 1 s, or from half of `TransferInactivityTimeoutSeconds` up | `400` |
   | an unknown transfer, or a caller who is not its participant | `404` with `TransportR-Refused` |

   The route brings `UseWebSockets` with it: the host adds nothing to its pipeline.
3. **The proof.** The client sends an empty `Text` message and the hub answers with one; the upgrade and the proof
   together have 5 s. A front end can accept an upgrade and still hold bytes back. The proof is the one test of the
   client-hub hop that depends neither on the peer nor on a hub parked on a slow one. It does not detect a path that
   carries it and holds bytes back later. The hub reads that message's type and nothing else: the socket then carries
   `Binary` messages only, copied to the bridge unread.
4. **The first byte.** The socket enters the bridge only once it carries one, as an epoch does with its first segment.
   Sooner, a side with nothing to say yet would cut its peer's stream at each of the peer's reconnections, and be cut
   in turn. The socket then replaces the source the bridge holds, a segment epoch included, whose late segments are
   answered `409`.
5. **The stream.** A flush is one binary message of at most `StreamBufferSize` (512 KB), and waits for no answer. A
   peer that reads slowly fills the socket, which blocks the flush: that is the backpressure. Each end sends a keepalive
   frame every presence interval. Neither sets a keepalive timeout, which would abandon a sound stream whenever the hub
   is parked on a slow peer.

**What the client makes of the answer to an opening:**

| Answer | Means |
|---|---|
| `101`, then the proof answered within 5 s | a WebSocket |
| `404` with `TransportR-Refused`, or `410` | the transfer is gone |
| `401` | renew the token and open once more |
| any other answer (`404` without that header, `200`, `403`, `405`, `421`, `426`, `5xx`), a cut before the answer, no answer within 5 s, a proof unanswered or cut | the path carries no WebSocket |

On a path with no WebSocket, the client uses segments until the session ends, in the same attempt and before the first
byte. It says why in a warning and in `TransferInfo.WriteTransportFallback`, and the next transfer tries a WebSocket
again. Under `RequireWebSocket` the transfer fails instead, with a message that names no path or setting. A path that
passes no segment either fails on the first one, and the session reconnects as after any cut.

A cut of a proven WebSocket is reopened as any cut is (probe, new opening, `Ping`/`Pong`), over a WebSocket again.
Three cuts in a row, each within 30 s of its opening, also mean the path carries no WebSocket. Without that rule, a path
that cuts every socket (on a frame size, a firewall) would have the session reconnect for ever where segments pass. A
connection that lives 30 s clears the count, and a refusal is not a cut.

**A WebSocket ends in one of three ways.**

- **Cleanly**: `Close(4000)` in each direction, the client's after its last flush and the hub's once the bridge has
  closed the stream. The status is private because that end retires the transfer: a front end that cuts with a `Close`
  of its own, 1000 or 1001, must read as the cut it is. The hub reads any other `Close` as a cut.
- **Refused**: `Close(4409)` from the hub, what a `409` says to a segment (the stream was replaced, its peer was lost,
  the transfer is over), read as retryable and refused. Like a `409`, it goes at the client's next write, not at once.
  Sent sooner, it would have a client whose peer reconnected reconnect in turn, and cut that peer. A transfer gone is a
  `404` at the reopening, as for an epoch.
- **Cut**: the connection fails, or closes with another status. The client keeps a read pending to see it, since a
  `Close` nobody reads leaves its writes succeeding as if nothing had happened.

**Segments** are the fallback, and pass wherever a request does. A front end that buffers a request body holds back the
few bytes a stream ends on (an acknowledgement, a `Pong`) until the request is over, and a short request is over at
once. Each segment carries the next bytes of one opening of the stream, its **epoch**, at their **offset** in it
(`DataPlane`). The hub reads the epoch and the offset from the query, never a byte of the body.

- **One segment in flight**, so a sender's rate is bounded by one segment per round trip. A flush is a segment, of at
  most `StreamBufferSize` (512 KB). The hub puts an epoch's segments end to end into the stream a bridge copies from,
  and answers each once the peer has it: a peer that reads slowly slows the flushes down. A segment beyond what the
  hub has taken is refused; nothing is reordered, and the hub keeps no byte past the request.
- **The answer.** `200` and `202` carry `TransportR-Relayed`, how many bytes of the epoch have reached the peer.

  | Status | Means | The client |
  |---|---|---|
  | `200` | all of the segment reached the peer | sends the next |
  | `202` | the hub held it for 30 s (`UploadEpoch.DefaultHoldBound`) and the peer took part of it | sends the rest again, in the same epoch; the hub skips what it relayed |
  | `409` | no epoch this segment follows on from: replaced, over, or held by another instance (bridges are local, so not a `421`) | opens a new epoch; the session reconnects as after any cut |
  | `401` | nothing taken | renews its token and sends the segment again |
  | `404`, `410` | the transfer is gone | — |

  On any refusal, of a segment or a WebSocket, the attempt ends only once its read stream has, 2 s at most. What the
  hub relayed before refusing, a sender's `Abort` for one, is thus read first.
- **The end.** The last segment carries `final=true` and goes after the `CompleteAck`. The hub answers it once the
  bridge has closed the stream (*Transfer Completion* in `ResilienceArchitecture.md`).

Both transports are HTTP/1.1, and the client says so on every request, an upgrade included (`HttpVersion.Version11`,
`RequestVersionExact`, in `HttpStreamManager`). It is a decision, not a default left alone:

- **HTTP/2 has nothing to offer this traffic.** A participant holds one long GET per transfer and writes on one socket,
  or one request at a time: there is nothing to multiplex and no request header worth compressing.
- **HTTP/2 caps an upload at its flow-control window.** With Kestrel's default windows, one continuous
  sender-to-hub stream reached 16 MB/s at 20 ms round-trip and 4.7 MB/s at 80 ms (h2c, loopback behind a
  latency-injecting TCP proxy). A hub host can raise `Http2.InitialStreamWindowSize`; HTTP/1.1 needs no such tuning.

Not measured: segments under HTTP/2. A WebSocket over HTTP/2 (RFC 8441) is excluded.

What this asks of a front end, and the environments it has been validated in, are under "Deploying the hub" in
`src/TransportR.Hub/README.md`.

### Resilience & Reliability

TransportR utilizes a **Dataplane First** resilience strategy:

- **Stream Handshake (Ping/Pong)**: After a cut, the client probes the hub with a request, then reopens its streams once it answers. A Ping/Pong on the new data stream tells the sender where the receiver stands.
- **Packet-Level Resumption**: Receiver names the next batch it expects, the one to resume from, eliminating the need for Hub-side state (Checkpoints).
- **Sender-side Buffering**: Payloads are kept until End-to-End ACK is received.
- **Hub Lifecycle Management**: Hub terminates transfers that go silent (timeout), and, if configured, those that stop progressing.

See [ResilienceArchitecture.md](./ResilienceArchitecture.md) for the complete design.

### Security

Authentication and Authorization are handled via OAuth2/JWT with group-based scoping (`transportr:group:*`).
See [SecurityArchitecture.md](./SecurityArchitecture.md) for the security model.

### High Availability (HA)

TransportR supports horizontal scaling (Active/Active) for Hubs:

1. **Shared State**: client registrations and transfer sessions are stored in Redis (`TransportR.Storage.Redis`), for
   registration, discovery and routing.
2. **Sticky Sessions**, required at the load balancer, carry two affinities:
   - a client's, the instance holding its events stream: its registration, discovery, presence and renewal requests;
   - a transfer's, the instance holding its bridge: its handshake, replies, heartbeats, probe and data streams.

   The client sets the `Cookie` header per request; its HTTP handler holds no cookie container, which would override it.
3. **Affinity Handoff**: a sender whose receiver's events stream is held by another instance is answered `421` with the
   cookie that reaches that instance, and sends that one `InitTransfer` request again with it. Its own events stream
   does not move, and neither do its other transfers. A stream opened without a cookie, whose response sets one, is
   opened once more carrying it: the registration would otherwise record no instance.
4. **TestProxy**: a YARP-based reference implementation for testing HA locally; `ha-transfer-affinity` is the scenario
   that exercises the redirection and the affinity of each transfer.

**HA requires effective affinity, not merely preferred affinity.** The per-transfer progress counters (`TransferState`:
items sent, acks, last activity) stay in a dictionary local to the instance that registered the transfer or accepted
one of its streams, never in `IStateStore`. `TransferCleanupService` reaps on them, so a transfer whose heartbeats and
streams reach two instances looks half-silent to each, and both may reap it. Sharing the counters would cost a store
write per heartbeat, for a case the deployment already excludes.

---

## Main Interfaces

| Interface | Purpose | Location |
|-----------|---------|----------|
| `IDataSender<T>` | Initiate transfers, discover receivers | [ITransportRClient.cs](src/TransportR.Client/Abstractions/ITransportRClient.cs) |
| `IDataReceiver<T>` | Receive transfers via event | [ITransportRClient.cs](src/TransportR.Client/Abstractions/ITransportRClient.cs) |
| `IBatchSerializer<T>` | Pluggable serialization | [IBatchSerializer.cs](src/TransportR.Client/Abstractions/IBatchSerializer.cs) |
| `IHubProgressMonitor` | Hub-side monitoring (bytes and write transports) | [IHubProgressMonitor.cs](src/TransportR.Hub/Abstractions/IHubProgressMonitor.cs) |

## Observability

**The packages emit no metrics of their own.** They provide two hooks, and the host decides what to do with them:
`IHubProgressMonitor` on the hub side, and the `TransferInfo` records `GetTransfers()` returns on the client side.
`samples/DataHub` builds its own OpenTelemetry meter on the first: copy it rather than expect one from the package. The
hub announces the transport of each write stream it takes on, to its log and to
`IHubProgressMonitor.OnWriteStreamOpened`, and of each write-transport probe to `OnWriteTransportProbed`.

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

Integers are little-endian. A GUID is the 16 bytes `Guid.ToByteArray()` gives: the first three fields of its text form
(4, 2 and 2 bytes) little-endian, the last 8 bytes in order.

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
| SealedBatch | 0x09 | `Batch` sealed end to end (*End-to-End Encryption*, below) |
| SealedStreamBatch | 0x0A | `StreamBatch` sealed end to end; `Count` is the plaintext's size in bytes |

The sender's data stream carries `Ping`, the batches, `Complete` and `Abort`; the receiver's control stream `BatchAck`,
`Pong`, `CompleteAck` and `Abort`. Any other type on the stream an end reads, an unknown one included, is a protocol
violation, refused at its header, in clear as encrypted.

> **Architectural Note:** The transport layer is **serializer-agnostic**. The `IBatchSerializer<T>` interface allows pluggable implementations (MessagePack, JSON, Arrow). A batch body the receiver cannot decode fails its session instead of being resent (§1 in `ResilienceArchitecture.md`).

### End-to-End Encryption

`WithEndToEndEncryption()`, on both builders, seals every batch so that the hub forwards bytes it cannot read;
`WithEndToEndEncryption(Func<Guid, byte[]?> preSharedKey)` also mixes a 32-byte pre-shared key (PSK) into the keys. Both
ends are built alike, and `Build()` refuses where `AesGcm.IsSupported` is false. What each level protects, and against
whom, is under "End-to-End Encryption" in [SecurityArchitecture.md](./SecurityArchitecture.md#end-to-end-encryption).
One suite, never negotiated, all in the BCL: ECDH on P-256, HKDF-SHA256, AES-256-GCM, HMAC-SHA256. In substance the
construction is Noise `NNpsk0`'s and TLS 1.3's PSK-DHE key schedule, written out here because Noise defines no P-256
function and the BCL has no portable X25519.

```mermaid
sequenceDiagram
    participant S as Sender
    participant H as Hub
    participant R as Receiver

    Note over S,R: At each opening of the streams, before any batch
    S->>R: Ping, payload: the sender's share (65 bytes)
    R-->>S: Pong, payload: the receiver's share then the confirmation (97 bytes), BatchId: the resume point
    Note over S: Seals nothing, and signs no Complete, before a Pong whose confirmation holds
    loop Each batch
        S->>R: SealedBatch or SealedStreamBatch, payload: the ciphertext then the tag (16 bytes)
        R-->>S: BatchAck, no payload, for a batch that opened
    end
    S->>R: Complete, payload: the sender's receipt (32 bytes), BatchId: the last batch's
    R-->>S: CompleteAck, payload: the receiver's receipt (32 bytes), BatchId: 0
```

- **Shares.** Each end draws an ephemeral P-256 key pair per session, kept across its reconnections and erased once the
  keys are derived. A share is the uncompressed point of SEC 1 (`0x04`, then X and Y, 32 bytes each, big-endian). A
  keepalive `Ping` and its `Pong` carry the same payloads as the handshake's. A share that is not a point of the curve,
  or differs from the first one a session accepted, is refused. Without a `Pong` whose confirmation holds, the sender
  fails as without an acknowledgement (`WithAcknowledgementTimeout`).
- **Keys.** The transcript `T` is 227 bytes, in this order: the protocol identifier, one byte of length (47) then the
  ASCII `transportr e2e v1 P-256 HKDF-SHA256 AES-256-GCM`, length-prefixed so that no identifier another suite or
  version takes yields the same bytes; the mode (1 byte: `0x00`, `0x01` with a PSK); the `TransferId`, the sender's
  client id and the receiver's, 16 bytes each, written as the header writes the `TransferId`; the sender's share and
  the receiver's. Each end writes the two ids as it holds them: the sender, its own and the target it named to
  `InitTransferAsync`, or, for a transfer the host opens, the receiver the hub announces; the receiver, its own and the
  sender the hub announces. `PRK = HKDF-Extract(salt = PSK or 32 zero bytes, IKM = the ECDH secret, its X coordinate,
  32 bytes big-endian)`, and each 32-byte key is `HKDF-Expand(PRK, label ‖ T)`, with the ASCII labels
  `transportr e2e batch`, `transportr e2e confirmation`, `transportr e2e sender receipt` and
  `transportr e2e receiver receipt`. The confirmation is `HMAC-SHA256(confirmation key, T)`, all 32 bytes, as for both
  receipts: they cross once per stream opening or transfer, and an untruncated MAC needs no rule of its own. The mode
  in `T` keeps an all-zero PSK from passing for none.
- **Sealed batches.** AES-256-GCM under the batch key. The nonce is the header's `BatchId` as written there (4 bytes,
  little-endian) followed by 8 zero bytes; the associated data is the 30-byte header as written on the wire, its type
  and sealed length included. A `BatchId` is spent once per session, never wraps (an encrypted transfer carries at most
  2³¹ batches), and a resend replays the sealed bytes: no nonce covers two plaintexts. The receiver opens a batch
  before it judges the batch's place in the order, and acknowledges none that does not open.
- **Receipts.** Each is `HMAC-SHA256(its receipt key, TransferId ‖ BatchId)`, with the `BatchId` of the `Complete`
  (int32, little-endian). The sender signs only its final `BatchId`, once every batch is registered, so no `Complete`
  for fewer batches exists; the receiver checks the receipt before it judges a `Complete` premature, and writes the
  same `CompleteAck` again on a reopened stream; a `CompleteAck` that comes before the sender has signed a `Complete`
  is refused. No MAC covers `BatchAck`, `Abort`, a `Pong`'s `BatchId`, the `BatchId` of a `Ping` or a `CompleteAck`,
  nor the `TransferId` and `Count` of any control chunk: forged or altered, they lead to a failure (a batch missing, a
  `Complete` refused until the acknowledgement timeout, a transfer ended) or to nothing delivered that differs (a
  lower resume point resends batches the receiver holds, which it drops).
- **Bounds.** `WithMaxBatchBytes` bounds the plaintext, and the reader admits a sealed batch 16 bytes longer. In
  encrypted mode a control chunk has exactly the length the diagram gives, whatever `WithMaxBatchBytes`, and `Abort`
  carries nothing. Any other length is a protocol violation, judged on the header before any of the payload is read.
- **Refusals.** An encrypted end refuses a `Ping` or `Pong` without a payload and a batch in clear, and aborts with
  `AbortReason.EncryptionModeMismatch` (5). A clear end ignores a `Ping`'s payload, so an encrypted sender finds the
  mismatch at the `Pong`. It answers a sealed batch with `Abort` (`UndecodableBatch`), as a batch of a kind it does
  not carry, and refuses a chunk its stream does not carry (*ChunkType Values*). A tag, confirmation or receipt that
  does not verify, a share that changes, or a sealed batch before the keys aborts with `AbortReason.VerificationFailed`
  (6); a PSK on one end only, or two different ones, fails at the confirmation, before any batch. An end whose provider gives no key refuses the transfer when its first
  stream opens, which carries its `Abort` (`VerificationFailed`) and nothing else. The end that refuses fails with
  `TransferFailureCategory.EndToEndEncryptionFailed` and a message that names no key; its peer learns it as
  `AbandonedByPeer`. Nothing is resent, neither the refused batch, which arrived whole, nor any other: the `Abort`
  goes first.
- **The PSK.** The provider is called once per transfer, synchronously, with the peer's client id, and returns 32
  bytes; `null`, another length or an exception refuses the transfer and never falls back to no PSK. Ignoring the id
  gives one key per trust domain. `TransferInfo.Protection` (`TransferProtection.None`, `Encrypted`, `EncryptedWithPreSharedKey`) says
  which level a transfer runs at.

A transfer in clear is byte for byte what it was: version 2, the same header, no payload on a control chunk.

---

## Integration

See [README.md](./README.md) for code examples (Server, Sender, Receiver), and the hub's
[README](./src/TransportR.Hub/README.md#transfers-the-host-opens) for transfers the host opens.

---

## Performance

The measured rates, their conditions and what caps one transfer are in the README, under
["Performance, and its conditions"](./README.md#performance-and-its-conditions).

Run benchmarks: `./scripts/run-benchmark.sh`

---
