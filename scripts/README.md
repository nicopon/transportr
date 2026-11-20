# Scripts

Unified scripts for building, initializing, and benchmarking TransportR.

## Quick Start

```bash
# Build the solution
./scripts/build_release.sh

# Generate test data (2M items)
./scripts/init.sh

# Run benchmark
./scripts/run-benchmark.sh
```

---

## Scripts

### `build_release.sh`

Builds each project of the solution in Release mode, libraries first, then samples and tests.

```bash
./scripts/build_release.sh
```

### `init.sh`

Downloads Toxiproxy, reports where Redis will come from, and generates the test data.

```bash
# Default: 2M items
./scripts/init.sh

# Custom count
ITEM_COUNT=500000 ./scripts/init.sh

# Keep existing data
./scripts/init.sh --skip-data
```

Output: `data/orders.bin`

It exits non-zero as soon as something it attempted fails, and a failed download leaves no half-written binary behind.

Redis, by platform:

- **`linux-amd64`**: it downloads a static `redis-server` into `tools/`.
- **Elsewhere**: nothing installs Redis. It prints which route `ha-redis` will use at run time: a native `redis-server`
  (`tools/`, `PATH` or `/opt/homebrew/bin`), else a container of `REDIS_IMAGE` (default `redis:7-alpine`).
- **With neither a `redis-server` nor a Docker daemon**: `ha-redis` fails at start-up; it is not skipped.

### `run-benchmark.sh`

Unified benchmark runner with scenario support.

```bash
./scripts/run-benchmark.sh [scenario]
```

| Scenario | Description |
|----------|-------------|
| `default` | Dev mode without authentication |
| `oidc` | OIDC authentication with TestIdp |
| `embedded-idp` | OIDC authentication against the SimpleIdp embedded in DataHub |
| `data-plane-probe` | OIDC run that asks the live hub to refuse anonymous and wrong-role stream requests |
| `chaos` | Network chaos simulation |
| `chaos-bounded` | Chaos simulation with a bounded send window (`MAX_IN_FLIGHT_BATCHES=8`) |
| `fast-chaos` | Chaos simulation on 200 000 items, for quick iterations |
| `chaos-cut-sender` | Holds the sender's link silent (nothing passes, nothing is refused), then closes its connections; the transfer must resume and deliver every line |
| `chaos-cut-receiver` | The same cut on the receiver's link |
| `ha-no-shared-state` | HA mode (2 hubs behind the TestProxy, sticky sessions); the hubs share state through the TestProxy's own store rather than Redis |
| `ha-redis` | HA mode (2 hubs behind the TestProxy, sticky sessions) with the Redis state store |
| `ha-transfer-affinity` | One sender opens two staggered transfers to two receivers on different hubs, then both links are cut while the first transfer is still flowing. Each transfer's streams must reopen on the hub that holds its bridge, which a client with one affinity for all its transfers fails |
| `segments-default` | The `default` transfer with both clients told to use segments only (`WRITE_TRANSPORT=SegmentsOnly`); every write stream must have opened over segments |
| `segments-chaos-cut-sender` | `chaos-cut-sender` with segments only |
| `ws-upgrade-refused` | HA mode with the TestProxy stripping the WebSocket upgrade (`PROXY_UPGRADE=strip`): each client must say why it has no WebSocket and carry the transfer over segments |
| `ws-required-refused` | The same path with both clients requiring a WebSocket (`RequireWebSocket`): the transfer must fail, with the client's generic message and the hub's refusal in its log |
| `ws-held` | HA mode with the TestProxy accepting the upgrade and relaying nothing (`PROXY_UPGRADE=hold`): each client must give up within its bound and carry the transfer over segments |
| `ws-held-late` | The same path, but it answers the opening proof first (`PROXY_UPGRADE=hold-late`) and retains what is written after it: no fallback can follow, so the transfer must fail with the client's stall message and not hang |
| `ws-cut` | `chaos-cut-sender` over a WebSocket: the sender's data stream must be opened over a WebSocket again after the cut |
| `parked-receiver` | A consumer at 12 items a second against a one-batch receive buffer, 6000 items: the write streams stay idle for tens of seconds, over a WebSocket. About eight minutes |
| `parked-receiver-segments` | The same run with segments only |
| `nginx-default` | The `default` transfer through an nginx container configured with nothing but a `proxy_pass` (needs a container runtime) |
| `slow-receiver` | Throttled consumer against a byte-bounded receive buffer, with a memory ceiling |
| `slow-receiver-unbounded-bytes` | The control for the one above: same run, no byte bound. Expected to grow, and deliberately kept out of `test-all.sh` |
| `byte-stream` | `data/orders.bin` carried as bytes, with no serializer; the receiver's file must have the size and the SHA-256 of the source |
| `byte-stream-cut-sender` | `chaos-cut-sender` for the byte-stream mode, on a window of one batch; the transfer must resume and the file must still be the source |
| `sealed` | The `default` transfer with both samples sealing every batch end to end, without a pre-shared key; both ends must say `Encrypted` |
| `byte-stream-sealed-cut-sender` | `byte-stream-cut-sender` sealed with a pre-shared key generated for the run; the file must still be the source, both ends must say `EncryptedWithPreSharedKey`, and the key must be in no file of the output |
| `byte-stream-cut-receiver` | `chaos-cut-receiver` for the byte-stream mode, on a window of one batch; the transfer must resume and the file must still be the source |
| `byte-stream-segments` | `byte-stream` with a WebSocket excluded; every write stream must be carried by segments |
| `byte-stream-segments-cut-sender` | `byte-stream-cut-sender` over segments only |
| `byte-stream-oidc` | `byte-stream` against the TestIdp, under JWT authentication |
| `byte-stream-parallel` | one sender opens a byte-stream transfer to each of two receivers; both files must be the source |
| `byte-stream-ha-redis` | `byte-stream` through two hubs behind the TestProxy, with the Redis state store |

### Asserting what carried the write streams

A client journals what each of its write streams was opened over, and why it gave up on a WebSocket. A
scenario turns that into a result with these variables, read by `lib/transport.sh` from the sender's and
the receiver's own logs:

| Variable | Meaning |
|----------|---------|
| `WRITE_TRANSPORT` | `PreferWebSocket`, `RequireWebSocket` or `SegmentsOnly` for both clients. Unset: the client default (`PreferWebSocket`) |
| `ASSERT_WRITE_TRANSPORT` | `WebSocket` or `Segments`: every opening of both sides says so. With `WebSocket` and no `EXPECT_WRITE_FALLBACK`, no client may have given up on a WebSocket |
| `EXPECT_WRITE_FALLBACK` | `true`: each side gave up on a WebSocket and said why |
| `EXPECT_WEBSOCKET_REOPENED` | `true`: a transfer's data stream was opened over a WebSocket at least twice |
| `EXPECT_RESUME` | `true`: each side behind the proxy (`CHAOS_CLIENTS`) went from Streaming to Reconnecting and back |
| `EXPECT_TRANSFER_FAILURE` | `true`: the run must fail. Green when the sender reports the failure with the client's generic message (`EXPECT_FAILURE_MESSAGE`), the hub logged `EXPECT_HUB_LOG`, and no CSV is complete. The receivers are printed, not judged, after `TRANSFER_TIMEOUT_SECONDS` |
| `EXPECT_FAILURE_MESSAGE` | With `EXPECT_TRANSFER_FAILURE`: what the sender's failure message must contain. Default: `requires a WebSocket` |
| `EXPECT_HUB_LOG` | With `EXPECT_TRANSFER_FAILURE`: what the hub must have logged. Default: `not opened over a WebSocket` (a write stream with no upgrade); empty for a run where the hub is not meant to have seen the request |
| `SOURCE_MAX_ITEMS` | The sender stops after that many items, which sends a prefix of the source file |
| `BYTE_STREAM_BATCH_BYTES` | Both samples run in the byte-stream mode, batches cut at that many bytes. The run is judged on the size and the SHA-256 of the receiver's `.bin` file, not on lines. `BATCH_SIZE`, `RECEIVE_MAX_ITEMS`, `SOURCE_MAX_ITEMS` and `CONSUME_ITEMS_PER_SECOND` count items and are refused with it |
| `E2E_ENCRYPTION` | `true`: both samples seal every batch end to end (`Sender__EndToEndEncryption`, `Receiver__EndToEndEncryption`) |
| `E2E_PRE_SHARED_KEY` | `true`, with `E2E_ENCRYPTION`: a 32-byte key per run (`openssl rand -base64 32`) goes to both samples and is never written or printed. The run fails if it is in any file of the output directory, the hub's logs included |
| `ASSERT_PROTECTION` | `None`, `Encrypted` or `EncryptedWithPreSharedKey`: every transfer of both sides journals that `TransferInfo.Protection` |
| `PROXY_UPGRADE` | What the TestProxy of an HA run does with a request that asks for a WebSocket. Default `pass`; the other values are below |

`PROXY_UPGRADE` values:

- `pass`: relay the request.
- `strip`: remove `Upgrade` and `Connection`, so the hub sees a plain GET.
- `hold`: accept the upgrade and relay nothing.
- `hold-late`: answer the client's first message, then read nothing.

The chaos scenarios and `nginx-default` assert no transport. A client with three connections in a row cut within
30 seconds of opening falls back to segments by design, so the chaos scenarios check the line count.

### `test-all.sh`, `verify-loop.sh`

`test-all.sh` runs `init.sh`, a Release build and every scenario of its `SCENARIOS` list in sequence, and exits 1
if any of them failed. `verify-loop.sh` runs `test-all.sh` up to four times back to back, stopping at the first
failed loop, for flakes and leaks.

### `benchmark-compare.sh`

Runs the `default` scenario three times with MessagePack, then three times with JSON (`SERIALIZER_TYPE`), and
prints the sender's throughput for each. The runs are sequential, not interleaved.

### `client-bench.sh`

Compares builds of the client by the CPU time of the client process per MiB of batch body carried
(`tests/TransportR.ClientBench`, items or byte-stream mode, source and sink in memory). It does not run a scenario.

It builds the hub once and runs it apart. Each arm is a Release build of the harness in its own directory; the arms
are interleaved round after round, and the first arm runs again as an A/A witness. It prints each arm's median, mean,
extremes and interquartile range, then the A/A gap, the size of a difference worth reading. Its figures are comparable
only within one sitting. It exits 1 when a run fails, an integrity failure of the received data included.

```bash
dotnet build tests/TransportR.ClientBench -c Release -o /tmp/arm-before
./scripts/client-bench.sh --mode bytes before=/tmp/arm-before
```

### `lint-comments.sh`

Refuses deliberation and history left in comments, and doc comments with no member under them. CI runs it ahead of
the build.

---

## Creating Custom Scenarios

Create a `.env` file in `scripts/scenarios/`:

```bash
# scripts/scenarios/custom.env

SCENARIO_NAME="My Custom Benchmark"
SCENARIO_EXPECTED="SUCCESS with custom config"

ENABLE_AUTH=false
ENABLE_IDP=false

BATCH_SIZE=500
NUM_SENDERS=2
NUM_RECEIVERS=2
```

Run: `./scripts/run-benchmark.sh custom`

> [!IMPORTANT]
> **A scenario's `.env` file sets all of its configuration.** It overwrites any variable set before the script runs
> (`BATCH_SIZE=500 ./scripts/run-benchmark.sh` keeps the file's value), so that a scenario stays coherent (`ha-redis`
> always uses Redis). To change a parameter, edit the scenario file or create a new one.

---

## Asserting memory

RSS sampling writes one row per process per second into `$OUTPUT_DIR/rss.csv`. A scenario turns it on with one of:

| Variable | Effect |
|----------|--------|
| `MAX_RECEIVER_RSS_MB`, `MAX_SENDER_RSS_MB` | A ceiling: a run whose peak goes over it fails, as on a wrong line count. A ceiling with no sample behind it fails too |
| `SAMPLE_RSS` | `true`: records and prints the peak without asserting (`slow-receiver-unbounded-bytes`) |

A scenario that sets none of them samples nothing.

---

## Asserting the transfer actually succeeded

The line count is not enough. A sender can deliver every row and still fail with `TransferFailedException`
(its `Complete` unacknowledged within the stall bound) while the sample exits 0. Conversely, a batch is acknowledged
when it reaches the receive buffer, so a sender can report success while the receiver is still writing rows.

The runner therefore also requires each sender to log `[SENDER][STATS] Transfer <id> complete:`, written only once
`CompleteAsync` has returned. It is the only check here that tells a delivered transfer from a delivered file.
