# Resilience & Reachability Architecture

> Technical documentation of TransportR's fault tolerance mechanisms

## Overview

TransportR implements a **"Dataplane First" resilience pattern**:
1.  **State is driven by streams**: what happens on the HTTP streams decides the transfer's state.
2.  **Resumption via Stream Handshake**: after a cut, the client probes the hub with a request, then reopens its streams; a Ping/Pong on the new data stream carries the resume point.
3.  **Smart Resend**: the handshake names the last batch the receiver processed (Packet Level Ack), so the sender resumes without duplicates.
4.  **Hub Lifecycle Management**: the hub routes passively and monitors actively (Reaper). It ends silent transfers, and stalled ones where enabled, but does not orchestrate recovery.

---

## Key Timings

The one table of timings. A constant is named by its `Type.Member`, which `./scripts/lint-comments.sh` checks exists.
A **fixed** value has no setting. Any other is set where *Settable* says: a builder method on the client, an options
class on the hub. *Jittered*: each delay is drawn between half and all of its value.

| Side | Name | Default | Settable | What it bounds |
|---|---|---|---|---|
| client | `ClientOptions.PongTimeout` | 10 s | **fixed** | Wait for the in-stream `Pong` that carries the resume point |
| client | `ReconnectionHandler.ProbeTimeout` | 5 s | **fixed** | One probe while `Reconnecting` (`POST /api/client/{id}/transfers/{transferId}/ping`), to the instance holding the bridge. A `2xx`, or a `404` carrying `TransportR-Refused`, means the hub answers |
| client | `DataSession.FirstProbePause`, `DataSession.MaxProbePause` | 2 s → 15 s | **fixed** | Pause after an unanswered probe: doubles with each miss, jittered, back to 2 s on each new loss |
| client | `DataSession.CompletingPhaseRetryMs` | 500 ms | **fixed** | Receiver: pause between two attempts to reopen a stream once it has received `Complete` |
| client | `DataSession.LoopErrorPause` | 0.5 to 1 s | **fixed** | Pause after an unexpected error in the session loop, jittered |
| client | `DataSession.CompleteRetryMs` | 5 s | **fixed** | Interval between two `Complete` signals while nothing acknowledges |
| client | `ClientOptions.CompleteStallTimeout` | 60 s | `WithAcknowledgementTimeout` | Time since the last acknowledgement after which `CompleteAsync`, or a producer parked on a full send window, fails. A receiver that accepted `Complete` waits this long for the transfer to close |
| client | `DataSession.AbortWriteTimeout` | 2 s | **fixed** | Wait for `Abort` to reach the wire when a handle is released |
| client | `DataSession.AbortRequestTimeout` | 2 s | **fixed** | The `POST /api/stream/{id}/abort` sent when the `Abort` could not be written |
| client | `HttpStreamManager.ReadSideEndTimeout` | 2 s | **fixed** | Wait for the hub to end the read stream: after it refused the write stream, before the attempt ends; when a completed receiver's handle is released |
| client | `ClientOptions.HandshakeTimeout` | 20 s | **fixed** for a client; per call for a host (`handshakeTimeout`, `ITransferInitiator`) | The receiver's answer to `InitTransfer`, and a host-opened sender's; the hub applies its caller's value. Outside 0 to 5 min (a session's lifetime): `400` to a client, `ArgumentOutOfRangeException` to a host |
| client | `ClientConnection.HandshakeMargin` | 10 s | **fixed** | Added to the handshake timeout to bound `InitTransferAsync` as a whole: the request, its replay after a redirect, both answers read. Past it the call fails with `HubRequestFailedException` |
| client | `ClientOptions.HeartbeatInterval` | 1 s | **fixed** | Per-transfer statistics heartbeat (`POST …/transfers/{transferId}/heartbeat`), which only runs while a transfer does |
| client | `DataSession.HeartbeatTimeout` | 5 s | **fixed** | One transfer heartbeat; a beat still waiting is not joined by the next |
| client | `ClientOptions.PresenceHeartbeatInterval` | 15 s | **fixed** | The presence heartbeat (§1b), announced when the events stream opens, each beat bounded by the interval. Also a sender's idle time before it writes a `Ping` (§1), and a WebSocket write stream's keepalive, at both ends |
| client | `ClientOptions.WebSocketOpenBound` | 5 s | **fixed** | The upgrade and the proof of a WebSocket write stream, together; past it the session uses segments. The write-transport probe's WebSocket has as much, and as much again for its `Close` |
| client | `HttpStreamManager.ShortLivedRupturesUntilFallback`, `HttpStreamManager.ShortLived` | 3 in a row, each within 30 s of its opening | **fixed** | Cuts of a proven WebSocket after which the session uses segments. A connection that lives 30 s clears the count |
| client | `ClientOptions.StreamBufferSize` | 512 KB | **fixed** | Not a delay: the buffer in front of each data-plane write, which is also the largest segment or WebSocket message it sends |
| client | `ClientOptions.BatchFlushIntervalMs` | 1000 ms | `WithBatchTimeout` | A partial batch's wait before it is flushed; checked every half interval, a millisecond at the least (`Accumulator<TBatch>.FlushIntervalLoop`) |
| client | `ClientOptions.TokenRefreshThreshold` | 5 min | `WithTokenRefreshThreshold` | How long before its expiry a token is renewed, and no more than half of what is left of it |
| client | `ClientCredentialsOptions.RefreshThreshold` | 5 min | **fixed** | How long before its expiry `WithClientCredentials` stops reusing its cached token |
| client | `TokenRenewer.RequestTimeout` | 10 s | **fixed** | One `PUT /api/client/{id}/token` |
| client | `TokenRenewer.FirstRetry`, `TokenRenewer.MaxRetry` | 1 s → 30 s | **fixed** | Delay after a failed renewal: doubling, jittered, until the expiry |
| client | `ControlChannel.StreamSilenceLimit` | 30 s | **fixed** | A stream that carried nothing, keepalives included, for this long is abandoned and opened again. An opening the hub has not answered with the registration by then fails |
| client | `ControlChannel.FirstOpenAttempts`, `ControlChannel.FirstOpenDelay`, `ControlChannel.MaxOpenDelay` | 5 attempts, 500 ms → 5 s | **fixed** | Attempts to open the first stream, the delay doubling; a refusal is raised at once. Restarting a data stream is not backed off |
| client | `ControlChannel.FirstReopenDelay`, `ControlChannel.MaxReopenDelay` | 1 s → 30 s | **fixed** | After a stream was lost: doubling, jittered, no limit |
| hub | `ClientStream.KeepAliveInterval` | 15 s | **fixed** | Quiet on an events stream before the hub writes a `keepalive` event: half the client's silence limit |
| hub | Client silence | twice the announced presence interval (30 s) | **fixed**, announced by the client | An events stream the hub has heard nothing from is closed; any request the hub admits from that client counts as hearing from it |
| hub | Sender's data stream, silence | twice the interval its opening announces (30 s) | **fixed**, announced by the client | A write stream of the sender's data taken as cut: an epoch with no segment, or a WebSocket whose read has waited that long for a byte. No other write stream is judged on silence |
| hub | `UploadEpoch.DefaultHoldBound` | 30 s | **fixed** | A segment waiting for the peer to read it: answered `202` with what got through, and the client sends the rest again. Below the 60 s a default nginx allows a request without an answer |
| hub | `WebSocketInput.ProbeBound` | 10 s | **fixed** | Wait for a WebSocket's probe after the upgrade, the write-transport probe's included, above the client's 5 s so that the client is the one to decide about the path |
| hub | `WebSocketInput.CloseBound` | 2 s | **fixed** | A `Close` the hub sends, and after a `Close(4409)` its wait for the client's own; the write-transport probe's wait for the client's `Close`, and its answer |
| hub | `DataPlaneConfiguration.BridgeTimeoutSeconds` | 300 s | `DataPlaneConfiguration` | A bridge short of `Active` waiting for its peer: the hub ends the transfer, which cancels the bridge |
| hub | `DataPlaneConfiguration.TransferInactivityTimeoutSeconds` | 60 s | `DataPlaneConfiguration` | A transfer with no activity at all: no heartbeat, no byte on either stream |
| hub | `DataPlaneConfiguration.StalledProgressTimeoutSeconds` | disabled | `DataPlaneConfiguration` | Opt-in: a transfer alive but whose counters do not move |
| hub | `BridgeCleanupService.SweepInterval` | 5 s | **fixed** | Interval between two `BridgeTimeoutSeconds` checks |
| hub | `BridgeCleanupService.SweepErrorPause` | 10 s | **fixed** | Pause after a failed sweep |
| hub | `TransferCleanupService.SweepInterval` | 10 s | **fixed** | Interval between two inactivity and stalled-progress checks |
| hub | `TransferManager.SessionTtl` | 5 min | **fixed** | Expiry of a transfer's session in the `IStateStore`, pushed back by its heartbeats and by the bytes of its streams |
| hub | `TransferManager.SessionExtensionInterval` | 10 s | **fixed** | At most one extension of that expiry per transfer and interval |
| hub | `ClientRegistryConfiguration.DisconnectedGraceSeconds` | 300 s | `ClientRegistryConfiguration` | How long a registration whose events stream ended is kept |
| hub | `ClientRegistryConfiguration.ConnectedTtlSeconds` | 3600 s | `ClientRegistryConfiguration` | A connected registration the hub has not heard from at all (stores with server-side expiry) |
| hub | `AccessOptions.AccessSweepIntervalSeconds` | 5 s | `AccessOptions` | Interval between two passes that judge every registered client again |
| hub | `AccessOptions.TokenExpiryMarginSeconds` | 0 s | `AccessOptions` | How long a token is still honoured after its `exp` |

### Cross-process invariants

- `StalledProgressTimeoutSeconds`, when set, is greater than `BridgeTimeoutSeconds`: a reconnection looks like a stall from the hub. Refused when the
  hub starts (`AddDataHub`).
- `TransferInactivityTimeoutSeconds` is more than twice the shortest heartbeat interval a stream may announce (1 s), or no interval is
  accepted and every stream is refused. Refused when the hub starts (`AddDataHub`).
- `DisconnectedGraceSeconds` equals `BridgeTimeoutSeconds` (300 s) on purpose: a registration is not kept longer than the transfer it would resume.
  Nothing checks it (`ClientRegistryConfiguration`).
- The per-transfer heartbeat (1 s, fixed) is far below `TransferInactivityTimeoutSeconds` (60 s) (`ClientOptions.HeartbeatInterval`). The hub
  checks only that its own value is positive.
- The presence heartbeat (15 s, fixed) is below `ConnectedTtlSeconds` (3600 s), which it refreshes (`ClientOptions.PresenceHeartbeatInterval`).
  The hub checks only that its own value is positive.
- The longest probe pause (15 s, `DataSession.MaxProbePause`) is well inside `TransferInactivityTimeoutSeconds` (60 s): a session
  finds a hub that comes back within 15 s.
- The hub's keepalive (15 s) is half the client's silence limit (30 s), so a stream is abandoned only after two keepalives went missing. Nothing
  compares them, nor either with the idle timeout of a proxy in front of the hub, which has to be above the keepalive.
- The completion stall bound (`ClientOptions.CompleteStallTimeout`, 60 s, `WithAcknowledgementTimeout`) is the client's own, and at the
  defaults it is reached first. While `CompleteAsync` waits, the sender's per-transfer heartbeats keep the transfer active, so
  `TransferInactivityTimeoutSeconds` does not end it. The hub ends the transfer:
  - for a receiver that went away, no sooner than `BridgeTimeoutSeconds` (300 s) later;
  - for a receiver still connected but no longer reading, only on `StalledProgressTimeoutSeconds`, where it is set.

  Nothing compares the client's bound with them. Set above the one that applies, it lets the hub end the transfer first, and the sender
  fails on that ending rather than on its own bound.
- A client renews `min(TokenRefreshThreshold, half of what is left)` before its token's expiry (`TokenRenewer.Advance`); the hub closes it at most
  `AccessSweepIntervalSeconds` after the expiry plus `TokenExpiryMarginSeconds` (`AccessOptions`). Nothing compares the two.

---

## Sequence Diagram: Disconnection & Reconnection

```mermaid
sequenceDiagram
    participant S as Sender
    participant H as Hub (Bridge)
    participant R as Receiver

    Note over S,R: ⚡ Normal transfer in progress

    S->>H: Batch #42
    H->>R: Forward Batch #42
    R-->>H: ACK #42
    H-->>S: ACK #42

    Note over H: ❌ NETWORK FAILURE

    rect rgb(255, 230, 230)
        Note over H: Bridge.OnDisconnected() for specific stream
        Note over S: Stream failure detected
        Note over R: Stream failure detected
    end

    Note over S,R: Session → Reconnecting

    S->>H: POST /api/client/{id}/transfers/{transferId}/ping
    H-->>S: 204 (the instance holding the transfer answers)

    Note over S,R: Reopen the streams (Autonomously)

    S->>H: open the write stream (WebSocket, or segments)
    R->>H: GET /api/stream/{id}/receive
    Note over H: Bridge reconnected (if both present)

    rect rgb(230, 255, 230)
        Note over S: Stream Handshake (Reachability)
        S->>R: HTTP Ping (via stream)
        R-->>S: Pong(nextExpected=43)
        Note over S: ✓ Resumption Point Confirmed
    end

    Note over S: Skip batches < 43

    S->>H: Batch #43 (resend)
    H->>R: Forward Batch #43
    R-->>H: ACK #43
    H-->>S: ACK #43

    Note over S,R: ✓ Transfer resumes normally
```

---

## Mechanism Details

### 1. Disconnection Detection (Client-Side)

The hub **does not** notify clients of disconnections over their events streams, to avoid races. `HttpStreamManager`
reports how each stream attempt ended as a `StreamOutcome`, and `DataSession` decides what follows (*Client
Internals*, below). A stream that fails is `Retryable`, whatever the exception, and the session reconnects. The
exceptions are a cancelled stream, a broken wire protocol (`Fatal`), and a 404 or 410 (`Gone`). The manager itself
restarts only a stream refused for an expired token, once.

**A batch that does not decode or serialize is not a cut**: the session fails instead of reconnecting. An undecodable
batch arrived whole, so a resend would decode no better.

- The receiver writes `Abort` (`UndecodableBatch`) to the sender, then fails; the sender's `SendAsync` or
  `CompleteAsync` throws `TransferFailedException`.
- A sender whose batch does not serialize fails at once, and tells the receiver with `Abort` (`SerializationFailed`).

Either `Abort` falls back on the abort request (§6) when it cannot be written within `AbortWriteTimeout`. An end-to-end
encryption refusal ends the transfer too, with an `Abort` and no resumption ("End-to-End Encryption" in
`TransportRArchitecture.md`).

What a cut is on a write stream depends on its transport ("Data Plane Transport" in `TransportRArchitecture.md`, which
also says when cuts make the session fall back on segments):

- over segments, a segment that fails, which ends the epoch; the session reconnects with a new one. No connection stays
  open between two segments to fail;
- over a WebSocket, the socket failing, or closing with a status that is neither the end of the stream nor a refusal.
  Neither end sets a keepalive timeout, so the client sees a half-open WebSocket no sooner than a half-open read GET.

**A dead sender is found by its silence**, on either transport. With nothing to write, it sends a `Ping` every
`PresenceHeartbeatInterval`, and the hub takes its data stream as cut after two intervals without a byte (the rule of
§1b; *Key Timings*). Keepalive frames are not bytes of the stream, and a hub parked on a slow peer has no read pending,
so a slow receiver never passes for a silent sender. A receiver's control stream is not judged: it goes quiet whenever
the consumer holds a batch. A dead receiver shows at its data GET instead, and the sender then fails for lack of
acknowledgements.

### 1b. Registration Lifetime (Hub-Side)

A client registration is **not deleted** when its events stream ends: the end stamps `DisconnectedAtUtc`, which
`GetReceivers` exposes through `ClientInfo`. On an unstable network the same client usually opens a new stream within
seconds under the same `ClientId`, and deleting its registration would break a transfer about to resume.

-   **Nothing filters on it**, because hiding a briefly silent client is the worse error: a caller that finds nobody
    fails outright, one that tries a dead client retries. A caller with several candidates should prefer those with
    no value set.
-   **`DisconnectedGraceSeconds`** (300 s) is how long a dropped registration is kept: the window in which a
    reconnecting client can claim it back.
-   **`ConnectedTtlSeconds`** (3600 s) is a backstop for registrations orphaned by a hub that died before it stamped
    them. It applies to stores with server-side expiry (Redis); the in-memory store keeps connected registrations for
    the lifetime of the process.
-   **The presence heartbeat** (`POST /api/client/{id}/presence/{connectionId}`) refreshes it. The client sends it
    every `PresenceHeartbeatInterval` (15 s) while its stream is open, whether or not a transfer runs, so an idle
    client never expires.
-   **It is also how the hub sees a cut that closes nothing.** A write to a stream whose peer vanished without closing
    the connection does not fail, so the hub would otherwise see nothing until that connection closed. The client
    announces its interval when it opens its stream (`heartbeatMs`), and the hub closes a stream it has heard nothing
    from for two intervals (*Key Timings*). That end marks the client disconnected, as any other.

> **The per-transfer heartbeat cannot do this job**: a `DataSession` sends it only while its transfer runs, and it
> reports transfer statistics into `TransferState`. Presence belongs to the client, not to a transfer.

### 1c. Losing a Registration While Connected

A client can hold a stream the hub has no registration for. Discovery then returns nothing for it, and nobody can open
a transfer to it, while its own transfers in flight keep working: resumption is entirely data-plane. Three causes:

-   a hub restart;
-   a registration evicted past its grace period;
-   another stream of the same identity that took the registration over, which then names that stream.

Recovery is the client's, by two paths:

-   The presence heartbeat is answered **`404`** when the hub holds no registration for the stream. The client then
    opens a new stream, which registers it afresh, and closes the old one once the new one is registered.
-   A stream that ended (cut, closed by the hub, or silent for 30 s) is opened again after a delay
    (`ControlChannel.FirstReopenDelay` in *Key Timings*), and each opening registers the client. The client then
    restarts the streams of its transfers at once, rather than waiting for their next failure.

Without this policy the two stores would fail in opposite ways. An in-memory registration would live for ever; a Redis
one would expire on a fixed timer, whether or not the client is still there.

### 2. Stream Handshake (Reachability & Resume)

A session in `Reconnecting` reopens its streams only once the hub answers its probe: an ordinary request
(`POST …/transfers/{transferId}/ping`) that carries the transfer's affinity, so it reaches the instance holding the
bridge. The resume point is not asked of the hub (no "Checkpoint"): the sender asks the receiver **through the new
data stream**:

1.  **Sender** sends `ChunkType.Ping` immediately upon connecting the stream.
2.  **Receiver** responds with `ChunkType.Pong`, carrying the `NextExpectedBatchId`.
3.  **Sender** uses this ID to prune its retry buffer.

In encrypted mode, `Ping` and `Pong` also carry the key exchange ("End-to-End Encryption" in `TransportRArchitecture.md`).

Resumption thus rests on the **receiver's actual state**, the source of truth.

**A batch is the smallest thing this can name, so the receiver hands one to its consumer whole or not at all.** Capacity
for every item is taken before the first is published, and the hand-off is not cancellable. A batch stopped half way
would raise no `NextExpectedBatchId`, be resent whole, and deliver its published prefix to the consumer twice.

```csharp
// Sender Logic
await ChunkIO.WriteChunkAsync(stream, pingChunk);
// ... wait for Pong ...
if (pong.NextExpectedBatchId > currentBatch.Id) {
    Skip(currentBatch); // Already received
}
```

### 3. Hub-Driven Timeout (Dead Transfer Cleanup)

Two reapers end transfers on the hub:

-   **`TransferCleanupService`** watches every active transfer, by the policies below.
-   **`BridgeCleanupService`** ends, through `ITransferTerminator`, the transfer of a bridge left short of `Active` for
    `BridgeTimeoutSeconds`, waiting for its peer. It skips `Active` bridges, left to the policies below.

What `TransferCleanupService` judges:

-   **LastActivityUtc**: updated by the per-transfer heartbeats, the probe request (`…/ping`), and every read the hub makes from one of the transfer's streams, whose bytes are counted, never read. A transfer whose streams carry data stays alive while its participants' heartbeats are silent.
-   **LastProgressUtc**: updated ONLY when `ItemsSent`, `AcksReceived`, `ItemsReceived` or `AcksSent` increases.
-   **Which hub instance judges**: the one that registered the transfer, or that accepted one of its streams. An instance that only looks the transfer up in the shared store, for a misrouted request or to check whether the hub still holds it, starts no state. Its sweep therefore never ends a transfer it does not carry.
-   **Inactivity policy** (`TransferInactivityTimeoutSeconds`, 60 s): a transfer with no activity at all, neither a heartbeat nor a byte, is ended, whether or not it ever reported. The hub knows a transfer from its registration, or from the first of its streams it accepts. Heartbeats and probes refresh `LastActivityUtc`, so this deliberately does **not** fire for a peer that is connected but stuck.
-   **Stalled-progress policy** (`StalledProgressTimeoutSeconds`, **disabled** by default): a transfer alive but whose counters have not moved is ended. This is the "Zombie" case (a deadlock or a client issue).
-   **Why the second is opt-in**: from the hub, a reconnection looks like a stall, since neither makes progress. Enabling it means naming the longest outage the deployment intends to survive; a value at or below `BridgeTimeoutSeconds` would kill transfers that would have recovered, and the hub refuses it when it starts.
-   **Not a liveness check**: a transfer whose participant's token ran out, or whose groups may no longer reach each other, is ended by the access sweep instead (see "Token Renewal" in `SecurityArchitecture.md`).

**What the clients see.** A transfer the hub ends is cancelled on both clients with a `cancel-transfer` event, whatever
ended it: these policies, a bridge past its timeout, the access sweep, a host, or a participant's abort request (§6).

-   Each session *fails* with "The hub ended the transfer before it completed.", and sends nothing to its peer, which
    the hub told.
-   As the session stops, `TransferFailedException` is thrown by a parked `SendAsync`, a `CompleteAsync` waiting for
    the acknowledgement, the enumeration (after the items already received) and `WaitForCompletionAsync`.
-   The streams the clients then cut end at the hub as `Information` lines, since it no longer holds the transfer. The
    same cut on a transfer it still holds is a warning: a client that should reconnect.
-   A host releasing its handle is a different ending, and says so ("released before the transfer completed").

### 4. Affinity Handoff (HA)

Behind a load balancer with sticky sessions, a sender is sent to the instance holding its receiver's stream, and each transfer then carries the affinity of the instance holding its bridge ("High Availability" in `TransportRArchitecture.md`). For a transfer in trouble:

-   **After a cut,** a session reopens its streams and sends its probe, heartbeats and `abort` with its own transfer's affinity, whatever the client's other transfers have done since. A client that kept one affinity for all its transfers would send the first one's streams to the instance of the second, where no bridge exists (`ha-transfer-affinity`).
-   **A lost instance** ends its clients' events streams, which they open again on another instance and register there. The bridges it held are lost with it.
-   **What does not cross instances.** The hub tells a client through the stream it holds, so it sends no `cancel-transfer` to a participant held by another instance; that participant learns of the end from the data plane. With affinity handoff on, a transfer the host opens through `ITransferInitiator` is refused unless both clients' streams are on the instance that opens it.

### 5. Backpressure (Both Ends, Data Plane Only)

A slow consumer must cost the transfer time, not memory. There is no control-plane message for this: the pushback is the data plane throttling itself, end to end, and the hub takes no part in it.

**The chain.** One slow consumer stops the producer, without anyone being told:

1.  A full receive buffer parks the receiver's read loop.
2.  The parked read loop stops draining the HTTP response body, which closes the TCP window back to the hub and blocks
    the hub's forwarding write.
3.  Over segments, that holds back the answer to the sender's segment, and so its next flush. Over a WebSocket, the hub
    reads the next message only once its write has returned, so the socket fills and the sender's flush blocks.
4.  A batch is acknowledged only *after* it lands in the receive buffer, so the acknowledgement that would reopen the
    sender's in-flight window is withheld for as long as the consumer is behind.

**The two bounds, and their units.**

| End | Option | Counts | Default |
|---|---|---|---|
| Sender | `WithMaxInFlightBatches` | batches flushed but not yet acknowledged | `null` — unbounded |
| Receiver | `WithReceiveCapacity` | items **and** wire bytes, first to bind | `null` — unbounded |

**Set both ends: out of the box there is no backpressure**, and either bound alone moves the backlog to the other
process. The receiver's bound blocks the sender's write (the chain above) but not its serialization worker, which never
waits: its batches queue behind the blocked write. The send window stops that worker but cannot see the receiver's
bytes. With the receiver bounded alone, `slow-receiver`'s sender peaks at 451 MB (the scenario asserts only the
receiver), and an integration moving Arrow batches reached 1 693 MB on the sender. With neither bound, that integration
reached 1 038 MB on the receiver.

Both are `null` by default because the right figure depends on item size, which only the caller knows: for
record-sized payloads an unbounded buffer is a memory spike, for multi-megabyte ones it is the process. A guessed
default would look handled and go unchecked. `Build()` warns when neither end is set.

**WAN and high-throughput sizing.** The sender's in-flight window must satisfy the bandwidth-delay product:
$$\text{Window} = \text{MaxInFlightBatches} \times \text{batch size} \ge \text{RTT} \times \text{Bandwidth}$$
On a 25 ms link aiming for 60–100 MB/s, BDP is 1.5–2.5 MB. A window smaller than this (such as 8 batches of 64 KB = 512 KB) starves the producer while waiting for acknowledgements across round-trips. For continuous streaming across WAN or cloud regions, 256 KB or 512 KB batches with an in-flight window of 4–8 MB maintain pipeline saturation while keeping memory bounded.

**A producer parked on a full window fails once nothing has been acknowledged for `CompleteStallTimeout`**, with
`TransferFailedException`, the rule `CompleteAsync` applies. Only an acknowledgement releases it, so a receiver that
stopped early would otherwise park it for ever; a peer still acknowledging, however slowly, is still working. Both
failures send `Abort` (`AcknowledgementTimeout`) before closing the data stream: that stream closes as cleanly as a
finished transfer's, and the hub relays it to the receiver as one.

**An `Abort` can be lost.** The hub keeps nothing it relays, so an `Abort` the receiver never read is gone. The
receiver's streams restart when the sender's control read ends (the hub cuts the receiver's control write), and the
restart discards what was still unread. A data stream whose source has finished is answered `410 Gone`, which the
client reads as the end of the transfer (`Failed`, or `Completed` in the closing phase), not as a stream to reopen. A
fatal stream failure, this one or a transfer the hub no longer knows, releases what an `Abort` releases. It releases a
consumer parked in `ReceiveAsync`, which throws `TransferFailedException` once it has taken what was already received,
and a producer parked on the window.

**An `Abort` can also be stuck** behind data the receiver does not read, and it then goes as a request of its own (§6).

**The sender's window counts batches, taken by the producer before serialization** (in a `ByteStreamClient`, which has
no serialization, before the batch's buffer is rented). Taken in front of the serialization worker instead, it would
bound the serialized bytes by letting the un-serialized items pile up behind them. For an off-heap payload such as an
Arrow `RecordBatch`, a managed backlog would then become a native one. Nor can it count bytes: the serialized size is unknown
when the slot is taken, and only the caller could price an item before it is serialized. It need not: with the window
set, the receiver's byte bound reaches the sender through the chain above, by withholding the acknowledgements the
window waits for.

**The receiver's capacity counts items and wire bytes, and the first reached binds.** One unit cannot serve both
payload shapes: ten thousand items is a memory figure for orders and a meaningless one for multi-megabyte record
batches, and a byte budget is the reverse.

-   **Wire bytes** are the serialized payload as it arrived, charged to a chunk before it is decoded and returned item
    by item as the consumer takes them. They are the only size known without the item type's heap cost, and they are
    not that cost: Arrow IPC bodies are close to their decoded buffers less whatever LZ4 saved, while MessagePack
    records decode to several times their wire size. Size the bound against that expansion.
-   **A chunk larger than the whole budget**, in either dimension, is still admitted into an empty buffer. Nothing could
    ever free room for it, and a chunk cannot be handed over in parts, so refusing it would stall the transfer for good.
    Lowering either bound therefore throttles a transfer and cannot deadlock one.

### 6. The Abort Request

**An `Abort` cannot overtake data the receiver has not read**, and that is not rare. When the sender gives up with more
data unacknowledged than the pipe between the two holds, the `Abort` is still queued when `AbortWriteTimeout` (2 s)
expires. Cancelling the stream then looks to the hub like a cut to wait out. What the pipe holds depends on the
sockets' buffers and on what the connection has already carried, so no figure in bytes can be given. The larger the
window times the *wire* size of a batch (after compression), the likelier. On a loopback socket with a consumer that
reads nothing, 50 passes each:

| Window | Passes with the `Abort` stuck behind it |
|---|---|
| 8 batches of 8 × 4 KB | none |
| 8 batches of 8 × 16 KB | 16 % |
| 8 batches of 8 × 32 KB | 64 % |
| 8 batches of 8 × 64 KB | all |

**A participant whose `Abort` was not written within `AbortWriteTimeout` sends `POST /api/stream/{id}/abort`**, for this
case only: a request of its own is the one thing that still gets through a full stream. The hub ends the transfer
through `ITransferTerminator`, as for a host terminating a run, and both participants are told to cancel. The receiver
fails as for any transfer the hub ends (§3), not with the `Abort`'s reason. It is a request the hub acts on, not a
message anyone listens for.

- **Which instance acts.** Only the one that carried the transfer's streams: it holds the bridges and the event streams to tell. Behind a proxy with affinity the request follows the streams' cookie, which the session sets on it as on its streams (§4). A hub that holds none of the transfer's bridges answers 404 and leaves it alone.
- **When it cannot be made** (the hub is unreachable, a proxy blocks the path, the hub predates the route), only `BridgeTimeoutSeconds`, or `StalledProgressTimeoutSeconds` where it is set, releases the receiver.
- **Who may send it**: either participant, admitted as on the stream routes ("Security Guarantees" in `SecurityArchitecture.md`).

---

## Protocol Chunk Types

The chunk types are listed once, under "ChunkType Values" in
[TransportRArchitecture.md](./TransportRArchitecture.md#chunktype-values). This document relies on
`Ping` and `Pong`, the stream handshake of section 2, and on `Complete` and `CompleteAck` below.

---

## Transfer Completion: EOF Domino Sequence

A transfer ends on **pure dataplane signals**: no control-plane message is used.

### The Sequence

```mermaid
sequenceDiagram
    participant S as Sender
    participant H as Hub
    participant R as Receiver

    S->>R: 1. Complete (LastBatchId), on the data stream
    Note over R: Ignored while a batch up to LastBatchId is missing; the sender re-sends it every 5 s
    R->>S: 2. CompleteAck, on the control stream
    Note over R: Takes no more data, and ends its control stream after the ack
    H-->>R: 3. End of the control stream answered: the hub has read the ack
    Note over R: Completed
    Note over S: 4. CompleteAck read: Completed, the outgoing channel closes
    S->>H: 5. End of the data stream: it ends cleanly (EOF)
    Note over H: CleanEOF: bridge.Complete(), then RetireAsync
```

Steps 3 and 4 have no order between them. A receiver whose stream fails before step 3 reopens both and writes the acknowledgement again first; a reopened stream answered `404` means step 5 has happened, which is the end as well. A consumer waits for that closing phase at most `CompleteStallTimeout` (`WaitForCompletionAsync`).

In encrypted mode, `Complete` and `CompleteAck` also carry receipts ("End-to-End Encryption" in `TransportRArchitecture.md`).

### Hub Detection

The hub detects completion when `CopyAndFlushAsync()` returns `StreamEndReason.CleanEOF`: the sender's data stream ended
cleanly (step 5). Over segments, that is the segment marked `final`; over a WebSocket, the client's `Close(4000)`, which
the hub reads as the end of the stream.

```csharp
var endReason = await CopyAndFlushAsync(inputStream, receiverOutput, bufferSize, token);
if (endReason == StreamEndReason.CleanEOF && bridge.Complete()) // Step 5
{
    await retirer.RetireAsync(transferId); // state, session and both bridges; nobody is told
}
```

**Only that clean end retires the transfer**, and nothing is sent: both participants have seen it end. A rupture is a
transfer that may resume. The receiver's control stream ends cleanly too, but before the sender is known to have read
the acknowledgement. Once the transfer is retired, a reopened stream meets a transfer the hub no longer knows (`404`),
which a receiver in its closing phase reads as the end.

**The sender's data stream ends cleanly only if the client lets it.** `CompleteAsync` returns on the acknowledgement,
and the end of the stream follows. `DisposeAsync` of a completed session therefore waits for the hub's answer to that
end, at most `AbortWriteTimeout` (2 s), before it cancels the streams. The answer is the final segment's response, or
the hub's own `Close(4000)`, given once the bridge has closed the stream. A client that releases the sender sooner, or
dies, leaves the hub a rupture, which the inactivity sweep finds ("3. Hub-Driven Timeout", above).

**A receiver in its closing phase reads its data GET to the end**, which the hub reaches after step 5. `DisposeAsync`
of a completed receiver waits for that end (at most 2 s) before it cancels the streams. The receiver is `Completed` at
step 3, which may come before step 4, and the hub cuts a source whose destination has gone. A receiver that closed its
data GET then would take the end of the sender's stream down before the sender read the acknowledgement, and nobody
would send it again.

### Why Not Control Plane?

| Approach | Issue |
|----------|-------|
| A `TransferShutdown` event on the events stream | Race condition: message may arrive before/after stream events |
| Explicit `TransferComplete` call | Adds network round-trip, can fail independently |
| **EOF Detection (current)** | Synchronous, guaranteed ordering, no extra messages |

---

## Client State Machine

```mermaid
stateDiagram-v2
    [*] --> Idle
    Idle --> Starting: StartSenderAsync / StartReceiverAsync
    Starting --> Streaming: session loop opens the streams
    Streaming --> Reconnecting: a stream ended or failed retryably
    Reconnecting --> Streaming: probe answered
    Streaming --> Completed: [sender] CompleteAck read
    Streaming --> ReceiverCompleteReceived: [receiver] Complete accepted
    Reconnecting --> ReceiverCompleteReceived: [receiver] Complete accepted before the probe answered
    ReceiverCompleteReceived --> Completed: end of the control stream answered, or 404 on a reopened stream
    Completed --> [*]
```

`DataSessionStateMachine` holds the state. The endings are events (`OnStreamsLost`, `OnReleased`, `OnAborted`,
`OnEndedByHub`); every other move goes through `TransitionTo`. `TransitionTo` refuses a move out of a final state, and
out of `ReceiverCompleteReceived` to anything but a final state: a closing receiver that loses a stream reopens it from
its phase. Every state that is not final can end in:

- `Failed`: a fatal stream (protocol violation, `404` outside the closing phase), the peer's `Abort`, the hub ending the transfer, nothing acknowledged for `CompleteStallTimeout`, or a batch that does not serialize or decode;
- `Disposed`: the handle was released first, which queues an `Abort` for the peer.

`Completed`, `Failed` and `Disposed` are final, and `Stopped` completes on the first of them. A released handle keeps an outcome it already has: releasing a completed session leaves it `Completed`.

`Reconnecting` probes the hub until it answers, with no retry limit; the pause after each failed probe doubles from 2 s
to 15 s (*Key Timings*). `ReceiverCompleteReceived` is the only closing phase, so `IsCompleting` is always false on a
sender.

### Client Internals

One `DataSession` per transfer. What runs, and who owns it:

| Part | Owns | Runs on |
|------|------|---------|
| `DataSession` | The session loop (open the streams, reconnect, close), the write and read loops it hands each attempt, the state machine, `CompleteAsync`/`WaitForCompletionAsync`, the stall bound, delivery of `Abort` on release, the per-transfer heartbeat | One loop task; the caller's thread for `SendAsync`/`CompleteAsync`; a timer for the heartbeat |
| `HttpStreamManager` | One attempt at a time: two streams (data, control) under one cancellation source, the write one over a WebSocket or segments by `WriteTransportPolicy`. It reports how each attempt ended as a `StreamOutcome` (`Ended`, `Cancelled`, `Gone`, `Retryable`, `Fatal`), and sends the `abort` request | Two stream tasks per attempt |
| `BatchProcessor` | The outgoing channel (batches and control chunks, unbounded); the unacknowledged batches and the in-flight slots; the serialization worker, which reads the batches off the source the client builds; the dispatch of incoming chunks; the receiver's `ReceiverState` (order, duplicates) and the sink the client builds. It knows neither the item type nor whether the client carries items or bytes | The serialization worker (sender); the read loop (both) |
| `BatchAccumulator<T>` | Items not yet in a batch; flushes by size or `BatchFlushIntervalMs`, and takes an in-flight slot per batch | The caller's thread, and a flush timer |
| `ReconnectionHandler` | The probe request while `Reconnecting`, and the Ping/Pong handshake at the head of every sender data stream | The session loop; the sender's write loop |

Above the sessions, `TransportRClient<T>` and `ByteStreamClient` both compose `ClientConnection`: what a client is to the
hub, whatever its transfers carry. It holds the registration, the presence heartbeat, the token renewal and the
`ControlChannel`, which holds the events stream, opens it again when it is lost, and makes the requests of the control
plane.

Each side writes one stream and reads the other. The write loop drains the outgoing channel and flushes once per drain; the read loop hands each chunk to `BatchProcessor`.

| Role | Writes | Reads (GET) |
|------|---------------|-------------|
| Sender | Data | Control |
| Receiver | Control | Data |

What each stream carries is under "ChunkType Values" in [TransportRArchitecture.md](./TransportRArchitecture.md#chunktype-values).
