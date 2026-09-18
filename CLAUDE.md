# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

TransportR is a .NET library for bulk data transfer between applications through a centralized hub
(SignalR + HTTP streaming), for network-segmented environments. It is explicitly **not** a message
broker: no pub/sub, no hub-side persistence/replay, point-to-point transfers only. See README.md for
the pitch/comparison-to-Kafka-and-gRPC, and the three docs below for the design.

- `TransportRArchitecture.md` — control plane (SignalR) vs data plane (HTTP streaming) split, binary
  wire protocol (30-byte `ChunkHeader` + opaque payload), HA model.
- `ResilienceArchitecture.md` — "Dataplane First" resilience: reachability via in-stream Ping/Pong
  (not SignalR), packet-level resumption, the EOF "domino sequence" that terminates a transfer.
- `SecurityArchitecture.md` — JWT/OAuth2, `transportr:group:*` scopes, dev vs production auth mode.

## Build & Test

Target frameworks: multi-target `net8.0;net10.0` for the libraries under `src/`, `net10.0` only for
samples/tests. CI (`.github/workflows/dotnet.yml`) builds against the .NET 8 SDK.

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

Redis-backed tests (`RedisStateStoreTests`) use Testcontainers and are decorated with
`[DockerAvailableFact]` (`tests/TransportR.Tests/DockerAvailableAttribute.cs`) — they auto-skip
(not fail) when the Docker daemon isn't reachable, so a green run without Docker is expected, not a
gap.

### End-to-end / benchmark scenarios

There is no in-process integration test for a full sender→hub→receiver transfer; that's covered by
running real processes via the scripts, which double as both benchmark and non-regression check
(they assert exact line counts in the output CSVs):

```bash
./scripts/init.sh                    # one-time: downloads Toxiproxy/Redis, generates data/orders.bin (2M orders)
./scripts/run-benchmark.sh           # scenario "default": no auth
./scripts/run-benchmark.sh oidc      # external TestIdp + OIDC
./scripts/run-benchmark.sh chaos     # Toxiproxy-injected network faults mid-transfer
./scripts/run-benchmark.sh ha-redis  # Redis-backed IStateStore, single hub
./scripts/test-all.sh                # runs default, embedded-idp, oidc, chaos, ha-redis in sequence; exit 1 on any failure
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
(`Connect`, `GetAvailableClients`, `InitTransfer`) — it never touches bulk bytes. Data itself moves
over plain HTTP streaming (`StreamController`: `POST /api/stream/{id}/send`, `GET
/api/stream/{id}/receive`), with the hub acting as a **dumb pipe** that forwards opaque chunks
without deserializing them. Reliability, resumption and even transfer-completion detection are all
data-plane mechanisms (in-stream Ping/Pong, EOF detection) specifically to avoid SignalR-vs-stream
race conditions — don't reintroduce a control-plane message for something the data plane already
decides (see "Why Not Control Plane?" in `ResilienceArchitecture.md`).

### Wire protocol

Every chunk is a fixed 30-byte `ChunkHeader` (`src/TransportR/Protocol/ChunkHeader.cs`:
Version/Type/PayloadLen/BatchId/Count/TransferId) followed by an opaque payload. `ChunkIO`
(`src/TransportR/Protocol/ChunkIO.cs`) reads/writes this header; `ChunkType` values (`Batch`,
`BatchAck`, `Complete`, `CompleteAck`, `Ping`, `Pong`) are the whole vocabulary. Payload
serialization is fully decoupled behind `IBatchSerializer` — `TransportR.Serialization.{MessagePack,
Protobuf,Json}` are interchangeable implementations; the hub never needs to know which one is in use
because it never deserializes the payload.

### Project layering

```
src/TransportR                          # Protocol, interfaces, models — no SignalR/hub deps
src/TransportR.Client.SignalR           # Client: SignalRDataClient<T>, HubConnectionManager, DataSession, batching
src/TransportR.Hub.SignalR              # Hub: CommandHub (SignalR) + StreamController (HTTP), BridgeManager
src/TransportR.Serialization.{MessagePack,Protobuf,Json}  # IBatchSerializer implementations
src/TransportR.FlowControl                # Group-based authorization (IFlowControlService)
src/TransportR.Auth.ClientCredentials      # OAuth2 client-credentials token acquisition (client side)
src/TransportR.Auth.SimpleIdp              # Embedded OpenIddict-based dev IdP (hub side, opt-in)
src/TransportR.Storage.Redis                # IStateStore backed by Redis, for HA
```

`IStateStore` (`src/TransportR/Interfaces/IStateStore.cs`) is the abstraction that makes HA possible:
client registrations and `TransferSession`s go through it, with three implementations —
`InMemoryStateStore` (default, single hub), `RedisStateStore` (HA, shared across hub instances), and
`ProxyStateStore` in `TransportR.TestProxy` (the reference HA test harness, not a product feature).
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
and consumes a `Receive<T>` handle via `IAsyncEnumerable`. `BatchAccumulator` buffers items
client-side and flushes by size or timeout; `BatchProcessor` drives the per-transfer send/receive
loop; `ReceiverState` tracks ordering/dedup on the receiving side. `HubConnectionManager` owns the
SignalR connection and its reconnection lifecycle; `HttpStreamManager` owns the data-plane HTTP
streams and restarts them on `IOException`/`HttpRequestException` (the hub never proactively
notifies clients of a disconnect — see "Disconnection Detection" in `ResilienceArchitecture.md`).

### Hub-side lifecycle

`BridgeManager` + `BridgeStateMachine` model one active transfer as a bridge between a sender stream
and a receiver stream; `DataSessionStateMachine` models the per-client session states. Both state
machines are directly unit-tested (`BridgeStateMachineTests.cs`, `DataSessionStateMachineTests.cs`,
`DominoStateTransitionTests.cs` for the completion sequence) — a change to transfer lifecycle
belongs in the state machine, not scattered as ad-hoc flags in `BridgeManager`/`TransferManager`.
`BridgeCleanupService`/`TransferCleanupService` are the reaper, and they judge different things:
`BridgeCleanupService` cancels a bridge idle for `BridgeTimeoutSeconds` (300s) and **skips `Active`
bridges entirely**; `TransferCleanupService` terminates a transfer with no activity at all for
`TransferInactivityTimeoutSeconds` (60s), and — only if `StalledProgressTimeoutSeconds` is set,
off by default — one that is alive but making no progress. Heartbeats refresh activity but not
progress, which is why the second check exists and why it is opt-in (a reconnection looks like a
stall from the hub). This is the *only* place the hub unilaterally kills a transfer.

### Auth

Groups are derived from JWT scopes (`transportr:group:senders`/`transportr:group:receivers`), never
trusted from client-supplied data, so `IFlowControlService` authorization is only as strong as the
IdP issuing those scopes. `EnableAuth=false` (default) is dev mode: the hub trusts whatever group the
client claims — never conflate this with production. `TransportR.Auth.SimpleIdp` /
`samples/TestIdp` exist purely so a scenario can exercise the real JWT path without an external IdP;
they are not meant to be production identity providers.

## Working conventions (from `.agent/workflows/coding-guidelines.md`)

- English only, in code, comments, and docs.
- Present a plan before any major refactor and wait for confirmation before executing it.
- Client-facing error messages must stay generic (no internal paths/group names/config); put
  diagnostic detail only in server-side logs.
- **Strict over tolerant**: don't silently auto-correct dirty input or add backward-compat shims in
  code — if a tolerance mechanism seems useful, propose it as an explicit option instead of building
  it in quietly.
- **Scope discipline**: TransportR is deliberately not a message broker. Flag (rather than silently
  implement) anything that pushes it toward pub/sub, hub-side persistence/replay, or topic routing —
  point to Kafka/RabbitMQ instead. Question any addition needing more than 2-3 new interfaces before
  building it.
- No generation dates in documentation.
