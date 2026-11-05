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
| `nginx-default` | The `default` transfer through an nginx container configured with nothing but a `proxy_pass` (needs a container runtime) |
| `slow-receiver` | Throttled consumer against a byte-bounded receive buffer, with a memory ceiling |
| `slow-receiver-unbounded-bytes` | The control for the one above: same run, no byte bound. Expected to grow, and deliberately kept out of `test-all.sh` |

### `test-all.sh`, `verify-loop.sh`

`test-all.sh` runs `init.sh`, a Release build and every scenario of its `SCENARIOS` list in sequence, and exits 1
if any of them failed. `verify-loop.sh` runs `test-all.sh` up to four times back to back, stopping at the first
failed loop, for flakes and leaks.

### `benchmark-compare.sh`

Runs the `default` scenario three times with MessagePack, then three times with JSON (`SERIALIZER_TYPE`), and
prints the sender's throughput for each. The runs are sequential, not interleaved.

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
