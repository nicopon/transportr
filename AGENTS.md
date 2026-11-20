# AGENTS.md

This file provides guidance when working with code in this repository.

## What this is

TransportR is a .NET library for bulk data transfer between applications through a centralized hub
(HTTP requests and server-sent events for control, HTTP streaming for data), for network-segmented
environments. It is explicitly **not** a message broker: no pub/sub, no hub-side persistence/replay, point-to-point
transfers only. See README.md for the pitch/comparison-to-Kafka-and-gRPC. The design lives in three documents, which this
file points into rather than repeats:

- `TransportRArchitecture.md` — projects, control plane vs data plane, the data plane's transport (HTTP/1.1, WebSocket or
  segments), binary wire protocol, HA.
- `ResilienceArchitecture.md` — the table of timings, disconnection and registration lifetime, resumption (probe, then
  in-stream Ping/Pong), the reapers, backpressure, the abort request, the EOF "domino sequence", the client's internals.
- `SecurityArchitecture.md` — JWT/OAuth2, `transportr:group:*` scopes, flow control, what each plane admits, token
  renewal, dev vs production auth mode.

The package READMEs under `src/` are for users: the hub's covers its integration, its deployment and the transfers its host opens.

## Build & Test

Target framework: `net10.0` for all libraries under `src/`, samples and tests. CI (`.github/workflows/dotnet.yml`) builds against the .NET 10 SDK.

```bash
dotnet restore
dotnet build TransportR.sln -c Release
dotnet test                                              # all unit tests
dotnet test --filter "FullyQualifiedName~ChunkIOTests"   # single test class
dotnet test --collect:"XPlat Code Coverage"
```

`./scripts/build_release.sh` builds each project in Release (libs → samples → tests); use it before
`run-benchmark.sh --no-build`, which runs the binaries under `bin/Release/net10.0/`.

Redis-backed tests (`RedisStateStoreTests`, `[RedisAvailableFact]`) skip, not fail, when the machine has neither Docker
nor a `redis-server`; `tests/TransportR.Tests/README.md` says where they look.

### End-to-end / benchmark scenarios

`TransferIntegrationTests` carries one transfer in process, over `TestServer` (no test host needs its own
`IHubProgressMonitor`: `AddDataHub()` registers a no-op one). Faults, volume, HA and auth modes run as real processes
through the scripts, which are both benchmark and non-regression check (exact line counts in the output CSVs):

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

`ENABLE_HA=true` starts `TransportR.TestProxy` (YARP, cookie-based sticky sessions) in front of two `DataHub` instances
on ports 5001/5002. It is the local setup for the affinity redirect (a `421` to an `InitTransfer`) and for per-transfer
affinity; `ha-transfer-affinity` is its acceptance scenario.

## Decisions not to reverse

Each is described where its pointer leads. A change that undoes one is the owner's decision, not a refactor.

### Hub and planes

- **The hub never reads a frame.** It forwards opaque chunks: only the clients read a `ChunkHeader` (internal to
  `TransportR.Client`, which the hub does not reference), and what the hub reads of a write stream is its transport's. A
  change that has the hub read one reverses this decision rather than extending it ("Data Plane Transport" in
  `TransportRArchitecture.md`).
- **The data plane decides what it can.** Resumption and the end of a transfer are data-plane mechanisms, to avoid
  control-vs-stream races: don't reintroduce a control-plane message for something the data plane already decides ("Why
  Not Control Plane?" in `ResilienceArchitecture.md`).
- **The abort request is for one case**, an `Abort` that cannot overtake the data in front of it, and stays a request the
  hub acts on, not a message anyone listens for (§6 in `ResilienceArchitecture.md`). The write-transport probe, the other
  data-plane route with no transfer's bytes, decides nothing ("Data Plane Transport" in `TransportRArchitecture.md`).
- **The data plane is HTTP/1.1 on purpose** (`HttpStreamManager`, a WebSocket's upgrade included), and a fallback from
  WebSocket to segments is observable, never silent: read "Data Plane Transport" before changing either.
- **The control plane's wire form is internal** (`ControlPlane` and its records, `ClientController`, `ControlChannel`);
  what is public is the path segments of `WireRoutes`. A change that hands a host one of those types undoes that.
- **The handshake lives in `TransferInitiator`**, for a sender that asks (`ClientController.InitTransfer`) and for the
  host (`ITransferInitiator`), so authorization and the cleanup of a half-done handshake cannot drift between the two
  ("Transfers the host opens" in `src/TransportR.Hub/README.md`).

### State and lifecycle

- **`IStateStore` is the home of every state HA shares** (`src/TransportR.Hub/Abstractions/IStateStore.cs`: in memory,
  Redis, and `TestProxyStateStore` for the HA harness, not a product feature): a new state-carrying concept for HA goes
  there, not in a hub-local field. The per-transfer progress counters stay local on purpose, which is why HA requires
  effective affinity ("High Availability" in `TransportRArchitecture.md`).
- **Registrations outlive their stream**, so a reconnect reclaims them, and `GetReceivers` never filters on
  `DisconnectedAtUtc`. The presence heartbeat keeps them, not a transfer's heartbeat (§1b, §1c in
  `ResilienceArchitecture.md`).
- **The state machines take events, not target states** (`BridgeStateMachine`, `DataSessionStateMachine`), and derive
  flags such as `SenderConnected` from the state. A change to transfer lifecycle belongs there, not in ad-hoc flags in
  `ConnectionBridge`/`TransferManager`; their tests drive them directly ("Client State Machine" in
  `ResilienceArchitecture.md`).
- **Only a clean end of the sender's data stream retires a transfer** (its final segment, or `Close(4000)`); a rupture
  may resume ("Transfer Completion: EOF Domino Sequence" in `ResilienceArchitecture.md`).
- **Liveness and rights are judged apart.** The reapers judge
  silence, and progress only where `StalledProgressTimeoutSeconds` is set, since a reconnection looks like a stall (§3 in
  `ResilienceArchitecture.md`); the access sweep judges rights ("Token Renewal" in `SecurityArchitecture.md`).

### Client and access

- **Backpressure is opt-in at both ends, `null` by default**: the right figure depends on item size, which only the
  caller knows, and `Build()` warns when neither is set. The sender's window counts batches, taken before serialization;
  the receiver's bound counts items and wire bytes and throttles, never stalls; a `BufferCapacity` that bounds nothing is
  refused (§5 in `ResilienceArchitecture.md`; the refusal in the XML docs of `BufferCapacity`).
- **The item type sits on `IBatchSerializer<T>`**, so a serializer that cannot carry it is a compile error, not a throw
  on the first batch ("Serialization" in `src/TransportR.Client/README.md`).
- **Groups come from JWT scopes, never from client-supplied data**, so authorization is only as strong as the IdP. Dev
  mode (`UseDevMode()`; `EnableAuth=false` in `samples/DataHub`) trusts whatever group a client claims: never conflate it
  with production, nor `TransportR.Auth.SimpleIdp` and `samples/TestIdp` with identity providers ("Scope Convention" and
  "Development vs Production Mode" in `SecurityArchitecture.md`).
- **`AccessPolicy` alone decides** whether a token is good, a right held, a send allowed and two identities one: for
  registration, `InitTransfer`, `GetReceivers`, the data plane, renewal, the sweep, and, through `RegisteredCaller`, every
  route that acts for a registered client, the write-transport probe included. A new surface asks it rather than keeping
  a copy. On every control route the path's `clientId` has to be the identity the request's token attests ("Security
  Guarantees" and "Token Renewal" in `SecurityArchitecture.md`).
- **End-to-end encryption lives in the clients.** The hub holds no key and does not change for it. The pre-shared key
  comes from each node's own configuration, never from a service hosted with the hub ("End-to-End Encryption" in
  `SecurityArchitecture.md`).
- **One suite, never negotiated.** Noise and TLS were examined and set aside: the construction is theirs in substance,
  written out (the first paragraph of "End-to-End Encryption" in `TransportRArchitecture.md`). Changing the
  transcript, a label or a size breaks the wire, and the frozen vector of `EndToEndCipherTests` is then recomputed by
  `tests/vectors/end_to_end_kat.py`, not by the C# code.

## Working conventions

- English only, in code, comments, and docs.
- **Be objective and critical.** No flattery. Challenge an idea you see a problem in, and state an uncertainty
  rather than speculate with confidence.
- Present a plan before any major refactor and wait for confirmation before executing it.
- Client-facing error messages must stay generic (no internal paths/group names/config); put
  diagnostic detail only in server-side logs. A security trade-off goes to the owner as options, each with
  its implications.
- **Strict over tolerant**: don't silently auto-correct dirty input or add backward-compat shims in
  code — if a tolerance mechanism seems useful, propose it as an explicit option instead of building
  it in quietly.
- **Comments say why, not what.** A comment restating the line below it is deleted, not reworded.
  A doc comment carries what a caller cannot read off the signature — ownership, threading, what
  breaks if it is ignored — and stops there. Nor does a comment tell what the code used to do: a
  version nobody reads any more belongs to the commit log.
- **No deliberation in committed code.** A comment that asks a question, weighs two options, or
  opens with "for now" / "let's assume" / "actually" / "wait" is thinking, not documentation: it
  tells the next reader the matter is settled when it is not. Resolve it, or move it to `.notes/`
  and leave a one-line pointer. `./scripts/lint-comments.sh` enforces this and runs in CI ahead
  of the build.
- **A doc comment outlives nothing.** When a member goes, its `///` block goes with it.
- **Prose has a budget, and it is spent.** The transfer lifecycle is already described in three
  `*Architecture.md` files, eight package READMEs and the XML docs; this file only points into them.
  Before describing a mechanism in a fourth place, delete one of the three — two sources for one
  subject are two sources that will diverge. A new document needs the same justification as a new
  interface.
- **Rule first, one claim per sentence.** Open with what the reader must know or do, then conditions and reasons;
  three cases or more make a list or a table, whose cells hold a value or a clause. A user document states a
  decision's effect; its defence lives once, in the maintainer's document.
- **Say what you did not check.** A `Verified:` line names what was run; when a change or a review
  touched only part of a surface, it also names what it did not cover ("7 of 94 source files",
  "unit tests only, no scenario"). Verification that does not state its perimeter reads as if it
  had none.
- **Measure interleaved, or do not quote a number.** This machine drifts ~10% between sessions, so
  a before/after run in two sittings measures the sitting. Alternate the two builds in one sitting
  and report the spread, not just the means.
- **Scope discipline**: TransportR is deliberately not a message broker. Flag (rather than silently
  implement) anything that pushes it toward pub/sub, hub-side persistence/replay, or topic routing —
  point to Kafka/RabbitMQ instead. Question any addition needing more than 2-3 new interfaces, or new
  infrastructure to operate, before building it. A host coordinates its own clients over a channel of
  its own: TransportR's control plane offers it no hook, and a hook, a group of event streams or a
  fan-out proposed there is the same drift toward pub/sub. Ask of each addition a consumer requests
  whether it is transport or orchestration; orchestration belongs to the host.
  - **Other languages**: not excluded, and best left to outside contributors. The wire format ports easily; the resilience
    logic (reconnection, Ping/Pong, resumption) does not, and each language multiplies bug parity and testing: flag that cost.
- No generation dates in documentation.
