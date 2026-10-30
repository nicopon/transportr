# AGENTS.md

This file provides guidance when working with code in this repository.

## What this is

TransportR is a .NET library for bulk data transfer between applications through a centralized hub
(SignalR + HTTP streaming), for network-segmented environments. It is explicitly **not** a message
broker: no pub/sub, no hub-side persistence/replay, point-to-point transfers only. See README.md for
the pitch/comparison-to-Kafka-and-gRPC, and the three docs below for the design.

- `TransportRArchitecture.md` — control plane (SignalR) vs data plane (HTTP streaming) split, binary
  wire protocol (30-byte `ChunkHeader` + opaque payload), HA model.
- `ResilienceArchitecture.md` — "Dataplane First" resilience: resume point via in-stream Ping/Pong
  (streams reopen once a SignalR probe answers), packet-level resumption, the EOF "domino sequence"
  that terminates a transfer.
- `SecurityArchitecture.md` — JWT/OAuth2, `transportr:group:*` scopes, dev vs production auth mode.

## Build & Test

Target framework: `net10.0` for all libraries under `src/`, samples and tests. CI (`.github/workflows/dotnet.yml`) builds against the .NET 10 SDK.

```bash
dotnet restore
dotnet build TransportR.sln -c Release
dotnet test                                              # all unit tests
dotnet test --filter "FullyQualifiedName~ChunkIOTests"   # single test class
dotnet test --collect:"XPlat Code Coverage"
```

`./scripts/build_release.sh` builds each project individually in Release (libs → samples → tests) —
prefer it when you need the sample/test binaries under `bin/Release/net10.0/` for a benchmark run,
since `run-benchmark.sh --no-build` execs those directly.

Redis-backed tests (`RedisStateStoreTests`) run on a Testcontainers `redis:7.0` when Docker answers,
and otherwise on a `redis-server` already on the machine (`tools/`, `PATH`, `/opt/homebrew/bin`),
started on a free port per test (`tests/TransportR.Tests/RedisServer.cs`). They are decorated with
`[RedisAvailableFact]` and auto-skip (not fail) only when neither exists. Nothing installs Redis for you.

### End-to-end / benchmark scenarios

`SignalRIntegrationTests` carries a transfer to completion in process, over `TestServer`; `AddDataHub()`
registers a no-op `IHubProgressMonitor` when the host registers none of its own, so no test host needs
one just to avoid a 500 on the first stream request. Anything beyond one small transfer — faults,
volume, HA, auth modes — is covered by running real processes
via the scripts, which double as both benchmark and non-regression check (they assert exact line
counts in the output CSVs):

```bash
./scripts/init.sh                    # one-time: downloads Toxiproxy (and Redis on linux-amd64), generates data/orders.bin (2M orders)
./scripts/run-benchmark.sh           # scenario "default": no auth
./scripts/run-benchmark.sh oidc      # external TestIdp + OIDC
./scripts/run-benchmark.sh chaos     # Toxiproxy-injected network faults mid-transfer
./scripts/run-benchmark.sh ha-redis  # 2 hubs behind the TestProxy, Redis-backed IStateStore
./scripts/run-benchmark.sh slow-receiver  # throttled consumer, asserts a receiver memory ceiling
./scripts/test-all.sh                # runs every scenario in test-all.sh's list in sequence; exit 1 on any failure
./scripts/verify-loop.sh             # runs test-all.sh 4× back-to-back (flake/leak hunting)
```

Scenarios are defined in `scripts/scenarios/*.env` and are the **only** place that sets their
config — env vars exported before invoking the script are overwritten by the scenario file, by
design (see `scripts/README.md`). Add a new scenario as a new `.env` file rather than parameterizing
an existing one.

`ENABLE_HA=true` (see the `ha` path in `run-benchmark.sh`) starts `TransportR.TestProxy` (YARP,
cookie-based sticky sessions) fronting two `DataHub` instances on ports 5001/5002, and points
clients at the proxy — this is the local reference setup for exercising `AffinityRedirectException`
handoff.

## Architecture

### Control plane vs data plane

This split is the central design decision and shows up everywhere: SignalR (`CommandHub` /
`TransportR.Hub.SignalR/Hubs/CommandHub.cs`) only ever does presence/discovery/handshake
(`Connect`, `GetReceivers`, `InitTransfer`, the heartbeats, and the `Ping` a reconnecting session
probes it with) — it never touches bulk bytes. Data itself moves
over plain HTTP streaming (`StreamController`: `POST /api/stream/{id}/send`, `GET
/api/stream/{id}/receive`), with the hub acting as a **dumb pipe** that forwards opaque chunks
without deserializing them. Reliability, the resume point and even transfer-completion detection are
data-plane mechanisms (in-stream Ping/Pong, EOF detection) specifically to avoid SignalR-vs-stream
race conditions. One thing still leans on SignalR, and the docs say so: a reconnecting session waits
for a SignalR `Ping` to answer before reopening its streams. The hub judges a transfer's activity on
its heartbeats and on the bytes its streams carry, counted and never read. Don't reintroduce a
control-plane message for something the data plane already decides (see "Why Not Control Plane?"
in `ResilienceArchitecture.md`). The one request that is not a stream is `POST /api/stream/{id}/abort` (`StreamController.Abort`): a sender whose in-stream `Abort`
cannot overtake the data in front of it (a full stream carries nothing more) asks the hub to end the
transfer, which the hub does through `ITransferTerminator`. It is for that case only — the data plane
cannot decide it — and it stays a request the hub acts on, not a message anyone listens for.

The data plane is HTTP/1.1 on purpose, pinned in `HttpStreamManager`: do not enable HTTP/2 without reading
"Data Plane Transport: HTTP/1.1" in `TransportRArchitecture.md` (an upload is capped by the flow-control
window, and the write side relies on a response meaning the body is over).

The handshake itself lives in `SignalRTransferInitiator`, not in the hub: `CommandHub.InitTransfer`
drives it for a sender that asks, and `ITransferInitiator` for the application hosting the hub,
which opens a transfer between two registered clients without either asking. One implementation,
so authorization and the cleanup of a half-done handshake cannot drift between the two.

### Wire protocol

Every chunk is a fixed 30-byte `ChunkHeader` (`src/TransportR/Protocol/ChunkHeader.cs`:
Version/Type/PayloadLen/BatchId/Count/TransferId) followed by an opaque payload. `ChunkIO`
(`src/TransportR/Protocol/ChunkIO.cs`) reads/writes this header; `ChunkType` values (`Batch`,
`BatchAck`, `Complete`, `CompleteAck`, `Ping`, `Pong`, `Abort`) are the whole vocabulary; `Abort`
carries an `AbortReason` in `BatchId`. Payload
serialization is fully decoupled behind `IBatchSerializer<T>` — `TransportR.Serialization.{MessagePack,
Protobuf,Json,Arrow}` are interchangeable implementations; the hub never needs to know which one is
in use because it never deserializes the payload. The item type sits on the interface, not on the
call, so a serializer that cannot carry a client's item type (`WithArrowSerialization()` on anything
but `RecordBatch`) is a compile error rather than a throw on the first batch — `MessagePack`,
`Protobuf` and `Json` are generic over `T`, `Arrow` closes over `RecordBatch`.

### Project layering

```
src/TransportR                          # Protocol, interfaces, models — no SignalR/hub deps
src/TransportR.Client.SignalR           # Client: SignalRDataClient<T>, HubConnectionManager, batching, OAuth2 client credentials
src/TransportR.Hub.SignalR              # Hub: CommandHub (SignalR) + StreamController (HTTP), BridgeManager, group flow control
src/TransportR.Serialization.{MessagePack,Protobuf,Json,Arrow}  # IBatchSerializer<T> implementations
src/TransportR.Auth.SimpleIdp              # Embedded OpenIddict-based dev IdP (hub side, opt-in, non-packable)
src/TransportR.Storage.Redis                # IStateStore backed by Redis, for HA
```

`IStateStore` (`src/TransportR/Interfaces/IStateStore.cs`) is the abstraction that makes HA possible:
client registrations and `TransferSession`s go through it, with three implementations —
`InMemoryStateStore` (default, single hub), `RedisStateStore` (HA, shared across hub instances), and
`TestProxyStateStore`, the hub-side adapter of the reference HA harness, which forwards to `ProxyStateStore` in `TransportR.TestProxy` (not a product feature).
A new state-carrying concept for HA needs a home in this interface, not a hub-local field.

Client registrations outlive their connection on purpose (a reconnect reclaims them), so they are
marked `DisconnectedAtUtc` rather than deleted, and evicted after `DisconnectedGraceSeconds`
(`ClientRegistryConfiguration`). `GetReceivers` exposes the marker and never filters on it: a caller
finding nobody fails outright, whereas one trying a dead client retries.

Presence is kept alive by `ICommandHub.Heartbeat()`, invoked every `PresenceHeartbeatInterval`
independently of any transfer, and returning `false` when the hub no longer knows the connection so
the client re-registers. The per-transfer `SenderHeartbeat`/`ReceiverHeartbeat` cannot serve this:
they only exist while a transfer does and they feed `TransferState`, not the registration.

### Sender/receiver client model

`SignalRDataClient<T>` implements both `IDataSender<T>` and `IDataReceiver<T>`
(`src/TransportR/Interfaces/ITransportRClient.cs`). A sender calls `InitTransferAsync` to get a
`Send<T>` handle (`src/TransportR/Abstractions/Send.cs`); a receiver registers `OnTransferStarted`
and consumes a `Receive<T>` handle via `IAsyncEnumerable`. A transfer the host opens reaches the
sender through `OnSendRequested` instead; a client with no handler refuses it. `BatchAccumulator` buffers items
client-side and flushes by size or timeout; `BatchProcessor` drives the per-transfer send/receive
loop; `ReceiverState` tracks ordering/dedup on the receiving side and hands decoded items to
`ReceiveBuffer`. `HubConnectionManager` owns the
SignalR connection and its reconnection lifecycle; `HttpStreamManager` owns the data-plane HTTP
streams and restarts them on `IOException`/`HttpRequestException` (the hub never proactively
notifies clients of a disconnect — see "Disconnection Detection" in `ResilienceArchitecture.md`).

### Hub-side lifecycle

`ConnectionBridge` + `BridgeStateMachine` model one active transfer as a bridge between a sender
stream and a receiver stream (`BridgeManager` is only the `ConcurrentDictionary` that keys them by
`transferId:ConnectionType`). `BridgeStateMachine` takes **events**, not target states —
`OnSenderRegistered`/`OnSenderLost`/… — and picks the resulting state itself, so a caller cannot
drive it into a state its table forbids, and `SenderConnected`/`ReceiverConnected` are derived from
the state rather than stored beside it. Its client-side counterpart, `DataSessionStateMachine`
(driven by `DataSession`), takes events for the endings (`OnStreamsLost`, `OnReleased`, `OnAborted`,
`OnEndedByHub`) and `TransitionTo` for the rest, which refuses only a move out of a terminal state. They
are directly unit-tested (`BridgeStateMachineTests.cs`, `ConnectionBridgeTests.cs`,
`DataSessionStateMachineTests.cs`) — a
change to transfer lifecycle belongs in the state machine, not scattered as ad-hoc flags in
`ConnectionBridge`/`TransferManager`.
A transfer ends well when the sender's data POST ends cleanly: the bridge takes `OnCompleted` and
`ITransferTerminator.RetireAsync` removes the state and both bridges, telling nobody. Only that
signal retires; a rupture may resume. It is why a completed sender's `DisposeAsync` waits for that POST.
`BridgeCleanupService`/`TransferCleanupService` are the reaper, and they judge different things:
`BridgeCleanupService` cancels a bridge idle for `BridgeTimeoutSeconds` (300s), ending its transfer
through `ITransferTerminator`, and **skips `Active` bridges entirely**; `TransferCleanupService`
terminates a transfer with no activity at all for
`TransferInactivityTimeoutSeconds` (60s) — it knows a transfer from its registration or the first stream
it accepts, never from a heartbeat or a lookup — and, only if `StalledProgressTimeoutSeconds` is set,
off by default — one that is alive but making no progress. Heartbeats and stream bytes refresh activity but
not progress, which is why the second check exists and why it is opt-in (a reconnection looks like a
stall from the hub). The other place the hub unilaterally ends a transfer is `AccessSweepService`/
`AccessReconciler`, which judge a participant's right to act, not its liveness (see "Auth").

### Backpressure

Both ends can be bounded, the two compose, and **both are `null` by default** — see
"Backpressure" in `ResilienceArchitecture.md`. Opt-in is deliberate: the right figure depends on
item size, which only the caller knows, so a default would be a guess nobody goes looking for.
`Build()` warns when neither is set.

The sender's `MaxInFlightBatches` counts **batches**, and its slot is taken *by the producer,
before serialization* — moving it behind the serializer converts a managed backlog into an
off-heap one. That placement is also why it cannot count bytes: the serialized size is not known
yet. It does not need to, because the receiver's `ReceiveBufferCapacity` reaches it — a full
receive buffer parks the read loop, which withholds the ack that reopens the send window.

`ReceiveBufferCapacity` counts **items and wire bytes**, whichever binds first: an item count is
not a bound when an item is an Arrow record batch, and a byte budget is a clumsy one for small
records. A chunk larger than the whole budget, in either dimension, is still admitted into an
empty buffer — the bound must throttle, never stall, and a batch is handed to the consumer whole
or not at all, because a resend cannot name anything smaller. `null` means no bound; a `BufferCapacity` with neither
dimension set is refused rather than treated as one.

### Auth

Groups are derived from JWT scopes (`transportr:group:senders`/`transportr:group:receivers`), never
trusted from client-supplied data, so `IFlowControlService` authorization is only as strong as the
IdP issuing those scopes. `EnableAuth=false` (default) is dev mode: the hub trusts whatever group the
client claims — never conflate this with production. `TransportR.Auth.SimpleIdp` /
`samples/TestIdp` exist purely so a scenario can exercise the real JWT path without an external IdP;
they are not meant to be production identity providers.

Whether a token is still good, whether a client holds any right, whether it may send to another and
whether two identities are one is decided once, by `AccessPolicy`, for `Connect`, `InitTransfer`,
`GetReceivers`, the data plane, renewal and the sweep — a new surface asks it rather than keeping a
copy. A client renews its token ahead of the expiry (`PUT /api/client/{id}/token`), or when its host asks
(`RefreshRegistrationAsync`), and the hub closes a connection, and ends its transfers, when that does not
happen: see "Token Renewal" in `SecurityArchitecture.md`.

## Working conventions (from `.agent/workflows/coding-guidelines.md`)

- English only, in code, comments, and docs.
- Present a plan before any major refactor and wait for confirmation before executing it.
- Client-facing error messages must stay generic (no internal paths/group names/config); put
  diagnostic detail only in server-side logs.
- **Strict over tolerant**: don't silently auto-correct dirty input or add backward-compat shims in
  code — if a tolerance mechanism seems useful, propose it as an explicit option instead of building
  it in quietly.
- **Comments say why, not what.** A comment restating the line below it is deleted, not reworded.
  A doc comment carries what a caller cannot read off the signature — ownership, threading, what
  breaks if it is ignored — and stops there.
- **No deliberation in committed code.** A comment that asks a question, weighs two options, or
  opens with "for now" / "let's assume" / "actually" / "wait" is thinking, not documentation: it
  tells the next reader the matter is settled when it is not. Resolve it, or move it to `.notes/`
  and leave a one-line pointer. `./scripts/lint-comments.sh` enforces this and runs in CI ahead
  of the build.
- **A doc comment outlives nothing.** When a member goes, its `///` block goes with it.
- **Prose has a budget, and it is spent.** The transfer lifecycle is already described in this
  file, three `*Architecture.md` files, six package READMEs and the XML docs. Before describing
  a mechanism in a fifth place, delete one of the four — two sources for one subject are two
  sources that will diverge. A new document needs the same justification as a new interface.
- **Say what you did not check.** A `Verified:` line names what was run; when a change or a review
  touched only part of a surface, it also names what it did not cover ("7 of 94 source files",
  "unit tests only, no scenario"). Verification that does not state its perimeter reads as if it
  had none.
- **Measure interleaved, or do not quote a number.** This machine drifts ~10% between sessions, so
  a before/after run in two sittings measures the sitting. Alternate the two builds in one sitting
  and report the spread, not just the means.
- **Scope discipline**: TransportR is deliberately not a message broker. Flag (rather than silently
  implement) anything that pushes it toward pub/sub, hub-side persistence/replay, or topic routing —
  point to Kafka/RabbitMQ instead. Question any addition needing more than 2-3 new interfaces before
  building it.
- No generation dates in documentation.
