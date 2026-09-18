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

Builds the entire solution in Release mode.

```bash
./scripts/build_release.sh
```

### `init.sh`

Downloads Toxiproxy and generates the test data.

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

### `run-benchmark.sh`

Unified benchmark runner with scenario support.

```bash
./scripts/run-benchmark.sh [scenario]
```

| Scenario | Description |
|----------|-------------|
| `default` | Dev mode without authentication |
| `oidc` | OIDC authentication with TestIdp |
| `chaos` | Network chaos simulation |
| `ha` | HA mode (2 Hubs + TestProxy) with sticky sessions |
| `ha-redis` | Redis State Store (single hub, for HA preparation) |

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

## Architecture

```
scripts/
├── build_release.sh     # Build solution
├── init.sh              # Generate test data
├── run-benchmark.sh     # Unified benchmark runner
├── README.md
├── lib/
│   ├── common.sh        # Utilities, logging
│   ├── process.sh       # Process management
│   └── idp.sh           # TestIdp for OIDC
└── scenarios/
    ├── default.env
    ├── oidc.env
    └── chaos.env
```
