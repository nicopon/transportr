# DatabaseInitializer

Writes `data/orders.bin`, a file of fictitious `Order` records (Bogus data, MessagePack-serialized), for the samples to send.

## Usage

```bash
# From the repository root, so that the file lands in data/
ITEM_COUNT=2000000 dotnet run --project samples/DatabaseInitializer

# Or through the initialization script
./scripts/init.sh
ITEM_COUNT=500000 ./scripts/init.sh
```

| Variable | Description | Default |
|----------|-------------|---------|
| `ITEM_COUNT` | Number of orders to generate | `100000` (`scripts/init.sh` sets 2000000) |

## Output

`data/orders.bin` holds one `[Size: Int32][MessagePack: byte[]]` pair per order, back to back. The `Order` class is that of
`samples/Shared` (see its README).

| Order count | File size |
|-------------|-----------|
| 100,000 | ~26 MB |
| 1,000,000 | ~260 MB |
| 2,000,000 | ~520 MB |

## Notes

- An existing file is **deleted** before generation.
- The `data/` directory is created if it does not exist.
- The data is deterministic only with a Bogus seed, which the sample does not set.
