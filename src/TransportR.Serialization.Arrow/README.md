# TransportR - Apache Arrow Serialization

Moves Apache Arrow `RecordBatch` payloads over [TransportR](https://github.com/nicopon/TransportR),
as Arrow IPC.

## Installation

```bash
dotnet add package TransportR.Serialization.Arrow
```

## Usage

```csharp
var client = new TransportRClientBuilder<RecordBatch>()
    .WithUrl("http://localhost:5252")
    .WithArrowSerialization()
    .Build();
```

`WithArrowSerialization` exists only on a builder carrying `RecordBatch`. Unlike the MessagePack and JSON
packages, this serializer is not generic — Arrow carries record batches and
nothing else — so a client built on the wrong item type does not compile rather than throwing on
its first batch.

## Two things this package decides for you

**Every chunk is a complete IPC stream** — schema, batches, end-of-stream marker. The protocol
decodes chunks independently and replays them after a cut, so a chunk depending on a schema sent
once at the start would decode into nothing after the first cut. Repeating the schema is the price
of that independence, and it is small next to a batch.

**This serializer owns the batches it is given and disposes them once written.** A `RecordBatch`
holds off-heap memory that something has to release, and this is the only place that knows the
batch has been written. The sender does not re-serialize on a resend — the unacked buffer holds
the serialized bytes — which is what makes disposing here safe rather than a use-after-free
waiting for a retry. Batches you receive are yours, allocated through the `MemoryAllocator` you
pass in, so a tracking pool can observe them.

## Sizing the receive buffer — read this one

`WithReceiveCapacity` is unbounded by default, and the item dimension is close to meaningless
here: ten thousand record batches is not a memory figure, it is somewhere between a few megabytes
and a few hundred gigabytes. **Bound the bytes.**

```csharp
.WithReceiveCapacity(maxBytes: 256 * 1024 * 1024)
```

Two things to know about that number:

- It counts **wire** bytes — the IPC payload as it crossed the network, not the decoded buffers.
  With the default LZ4 compression the decoded batches are larger than the budget by whatever the
  codec saved; construct the serializer with `compress: false` and the two are close.
- A chunk larger than the whole budget is still admitted into an empty buffer, so a batch bigger
  than you planned for throttles the transfer rather than stalling it.

The sender end is separate and also unbounded by default: `WithMaxInFlightBatches` counts batches,
not bytes, because its slot is taken before serialization where the size is not yet known. Set it
too: the receiver's bound stops the sender's network write, but not its serialization worker, so
with the receiver bounded alone the serialized batches pile up in the sender instead (see
"Backpressure" in `ResilienceArchitecture.md`).
