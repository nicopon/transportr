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

It exits non-zero as soon as something it attempted fails, which is what makes
`test-all.sh` able to judge it. A failed download leaves no half-written binary behind.

It does **not** provision Redis. Only `linux-amd64` has a static build to fetch; elsewhere
it reports which route `ha-redis` will take, and that scenario resolves Redis at run time —
a native `redis-server` when the machine has one, a `redis:7-alpine` container otherwise.
Override the image with `REDIS_IMAGE`. With neither, `ha-redis` fails at startup rather than
being skipped.

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
| `ha-transfer-affinity` | One sender opens two staggered transfers to two receivers placed on different hubs, then both links are cut while the first transfer is still flowing; each transfer's streams must reopen on the hub that holds its bridge, which a client that kept one affinity for all its transfers fails |
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
| `EXPECT_TRANSFER_FAILURE` | `true`: the run is meant to fail. It is green only when the sender reports the failed transfer with the client's generic message, the hub says it received a write stream with no upgrade, and no CSV holds every line. The receivers are watched until `TRANSFER_TIMEOUT_SECONDS` and what became of them is printed, not judged |
| `EXPECT_FAILURE_MESSAGE` | With `EXPECT_TRANSFER_FAILURE`: what the sender's failure message must contain. Default: `requires a WebSocket` |
| `EXPECT_HUB_LOG` | With `EXPECT_TRANSFER_FAILURE`: what the hub must have logged. Default: `not opened over a WebSocket`; set empty for a run where the hub is not meant to have seen the request |
| `SOURCE_MAX_ITEMS` | The sender stops after that many items, which sends a prefix of the source file |
| `BYTE_STREAM_BATCH_BYTES` | Both samples run in the byte-stream mode, with batches cut at that many bytes. The run is judged on the size and the SHA-256 of the receiver's `.bin` file, not on lines; `BATCH_SIZE`, `RECEIVE_MAX_ITEMS`, `SOURCE_MAX_ITEMS` and `CONSUME_ITEMS_PER_SECOND` count items and are refused alongside it |
| `PROXY_UPGRADE` | What the TestProxy of an HA run does with a request that asks for a WebSocket: `pass` (the default), `strip` (remove `Upgrade` and `Connection`, so the hub sees a plain GET), `hold` (accept the upgrade and relay nothing) or `hold-late` (answer the client's first message, then read nothing) |

The chaos scenarios assert no transport: a client that has had three connections in a row cut within
30 seconds of opening falls back to segments by design, so what they check is the line count.
`nginx-default` asserts none either.

### `test-all.sh`, `verify-loop.sh`

`test-all.sh` runs `init.sh`, a Release build and every scenario of its `SCENARIOS` list in sequence, and exits 1
if any of them failed. `verify-loop.sh` runs `test-all.sh` up to four times back to back, stopping at the first
failed loop, for flakes and leaks.

### `benchmark-compare.sh`

Runs the `default` scenario three times with MessagePack, then three times with JSON (`SERIALIZER_TYPE`), and
prints the sender's throughput for each. The runs are sequential, not interleaved.

### `client-bench.sh`

Compares builds of the client by the CPU time of the client process per MiB of batch body carried
(`tests/TransportR.ClientBench`, items or byte-stream mode, source and sink in memory). It builds the hub once, runs it
apart, and interleaves the arms (each a Release build of the harness in its own directory) round after round, with the
first arm run again as an A/A witness. It prints the median, mean, extremes and interquartile range of each arm, then
the A/A gap, which is the size of a difference worth reading. It does not run a scenario, and its figures are only
comparable within one sitting. Exits 1 when a run fails, an integrity failure of the received data included.

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
> **Configuration is defined entirely in `.env` files.** Environment variables set before running the script (e.g., `BATCH_SIZE=500 ./scripts/run-benchmark.sh`) will be **overwritten** by the scenario file. To change parameters, either:
> - Edit the existing scenario `.env` file, or
> - Create a new scenario file with your custom configuration
>
> This design ensures scenario coherence (e.g., `ha-redis` always uses Redis).

---

## Asserting memory

A scenario that declares `MAX_RECEIVER_RSS_MB` or `MAX_SENDER_RSS_MB` turns on RSS sampling: one
row per process per second into `$OUTPUT_DIR/rss.csv`, and a run that peaks over its ceiling fails
the same way a wrong line count does. Scenarios that declare neither sample nothing and cost
nothing.

This is what makes a memory claim checkable rather than asserted. `slow-receiver` holds the
receiver under 300 MB where its control, which differs only by the byte bound, reaches 870 MB.

---

## Asserting the transfer actually succeeded

The row count is not the verdict. A transfer can deliver every row and still end in
`TransferFailedException` on the sender — its `Complete` unacknowledged within the stall bound —
which leaves a full CSV, a clean session registry and a flat memory profile behind, and the sample
catches the exception and exits 0. The converse holds too: a batch is acknowledged when it reaches
the receive buffer, so the sender can report success while the receiver is still writing rows. A run can therefore be wrong in every way that matters and still satisfy
every other check on this page.

So the runner also requires each sender to have logged `[SENDER][STATS] Transfer <id> complete:`,
which only happens on the path where `CompleteAsync` returned. That line is the sender's own word
that the receiver acknowledged everything, and it is the only check here that distinguishes a
delivered transfer from a delivered file.
