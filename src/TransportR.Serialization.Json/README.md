# TransportR - JSON Serialization

Carries batches of any POCO over [TransportR](https://github.com/nicopon/TransportR) as
System.Text.Json, Zstd-compressed by default.

## Installation

```bash
dotnet add package TransportR.Serialization.Json
```

## Usage

```csharp
var client = new TransportRClientBuilder<Order>()
    .WithUrl("http://localhost:5252")
    .WithJsonSerialization()   // options, compressionLevel, compress
    .Build();
```

`WithJsonSerialization(compress: false)` puts plain UTF-8 JSON on the wire, which is the reason to
use this package while debugging. It is about 20 % slower to produce than MessagePack or Protobuf on
the TransportR benchmark, and uncompressed it is about 2.8 times its compressed size on the wire.

Both ends of a transfer must use the same serializer and settings, `compress` included: a batch the
receiver cannot decode fails the transfer.
