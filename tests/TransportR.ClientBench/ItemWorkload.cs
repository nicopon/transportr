using TransportR.Client;
using TransportR.Client.Services;
using TransportR.Serialization.MessagePack;
using TransportR.Serialization.MessagePack.Services;

namespace TransportR.ClientBench;

/// <summary>
/// The item mode with MessagePack: items of a fixed ring, sent again and again in the same order, so
/// the source costs nothing and the receiver can tell which item must come next.
/// </summary>
internal sealed class ItemWorkload : Workload
{
    // A whole number of batches makes one lap of the ring, so a batch is always one of the same few.
    private const int RingLength = 4096;
    private const int FlushIntervalMs = 10_000;

    private readonly int _batchItems;
    private readonly Item[] _ring;
    private TransportRClient<Item>? _sender;
    private TransportRClient<Item>? _receiver;
    private TaskCompletionSource<bool> _received = NewReceived();
    private long[] _batchBodyBytes = [];
    private long _expectedItems;

    public ItemWorkload(int seed, int batchItems)
    {
        if (batchItems <= 0 || RingLength % batchItems != 0)
            throw new ArgumentException($"The batch size in items must divide {RingLength}.", nameof(batchItems));

        _batchItems = batchItems;
        var block = Pattern.Create(seed);
        var random = new Random(seed);
        _ring = new Item[RingLength];
        for (var i = 0; i < RingLength; i++)
        {
            var length = 192 + random.Next(0, 64);
            _ring[i] = new Item
            {
                Seq = i,
                Label = $"item-{i:D5}",
                Payload = block.AsSpan(random.Next(0, Pattern.Length - length), length).ToArray(),
            };
        }
    }

    public override string Mode => "items";

    public override async Task ConnectAsync(string hubUrl, int sendWindow, CancellationToken cancellationToken)
    {
        _batchBodyBytes = await MeasureBatchBodiesAsync().ConfigureAwait(false);

        _receiver = new TransportRClientBuilder<Item>()
            .WithUrl(hubUrl)
            .WithBatchSize(_batchItems)
            .WithBatchTimeout(FlushIntervalMs)
            .WithMessagePackSerialization(useAnnotations: false)
            .Build();
        _receiver.OnTransferStarted += transfer =>
        {
            _ = Task.Run(() => ReceiveAsync(transfer, _received, _expectedItems, cancellationToken), CancellationToken.None);
            return Task.CompletedTask;
        };
        await _receiver.ConnectAsync([ReceiverGroup], cancellationToken).ConfigureAwait(false);

        _sender = new TransportRClientBuilder<Item>()
            .WithUrl(hubUrl)
            .WithBatchSize(_batchItems)
            .WithBatchTimeout(FlushIntervalMs)
            .WithMaxInFlightBatches(sendWindow)
            .WithMessagePackSerialization(useAnnotations: false)
            .Build();
        await _sender.ConnectAsync([SenderGroup], cancellationToken).ConfigureAwait(false);
    }

    public override async Task<long> TransferAsync(long targetBodyBytes, CancellationToken cancellationToken)
    {
        var (batches, bodyBytes) = Plan(targetBodyBytes);
        var items = batches * _batchItems;
        _expectedItems = items;
        var received = _received = NewReceived();

        await using var send = await _sender!.InitTransferAsync(_receiver!.ClientId).ConfigureAwait(false);
        var sending = Task.Run(async () =>
        {
            for (long i = 0; i < items; i++)
                await send.SendAsync(_ring[i % RingLength], cancellationToken).ConfigureAwait(false);

            await send.CompleteAsync(cancellationToken).ConfigureAwait(false);
        }, CancellationToken.None);

        await AwaitBothAsync(sending, received.Task, cancellationToken).ConfigureAwait(false);
        return bodyBytes;
    }

    /// <summary>
    /// The body each of the few distinct batches makes on the wire: serialized here by the serializer the
    /// clients use, which is all the public API shows of a body. Done before any transfer, so that it is
    /// not part of what a transfer costs.
    /// </summary>
    private async Task<long[]> MeasureBatchBodiesAsync()
    {
        var serializer = new MessagePackBatchSerializer<Item>(useAnnotations: false);
        var sizes = new long[RingLength / _batchItems];
        for (var b = 0; b < sizes.Length; b++)
        {
            using var body = new MemoryStream();
            await serializer.SerializeBatchAsync(body, _ring.Skip(b * _batchItems).Take(_batchItems).ToList()).ConfigureAwait(false);
            sizes[b] = body.Length;
        }

        return sizes;
    }

    private (long Batches, long BodyBytes) Plan(long targetBodyBytes)
    {
        long batches = 0, bytes = 0;
        while (bytes < targetBodyBytes)
        {
            bytes += _batchBodyBytes[batches % _batchBodyBytes.Length];
            batches++;
        }

        return (batches, bytes);
    }

    private async Task ReceiveAsync(Receive<Item> transfer, TaskCompletionSource<bool> received, long expectedItems, CancellationToken cancellationToken)
    {
        try
        {
            await using (transfer.ConfigureAwait(false))
            {
                long count = 0;
                await foreach (var item in transfer.ReceiveAsync(cancellationToken).ConfigureAwait(false))
                {
                    var expected = _ring[count % RingLength];
                    if (item.Seq != expected.Seq
                        || item.Label != expected.Label
                        || !item.Payload.AsSpan().SequenceEqual(expected.Payload))
                        throw new InvalidDataException($"The item received at rank {count} differs from the item sent.");

                    count++;
                }

                if (count != expectedItems)
                    throw new InvalidDataException($"The receiver got {count} items of the {expectedItems} sent.");
            }

            received.SetResult(true);
        }
        catch (Exception ex)
        {
            received.SetException(ex);
        }
    }

    public override async ValueTask DisposeAsync()
    {
        await DisposeClientAsync(_sender).ConfigureAwait(false);
        await DisposeClientAsync(_receiver).ConfigureAwait(false);
    }
}
