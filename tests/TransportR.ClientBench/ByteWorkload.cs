using TransportR.Client;
using TransportR.Client.Services;

namespace TransportR.ClientBench;

/// <summary>The byte-stream mode: a stream of bytes cut from the pattern block, with no serializer.</summary>
internal sealed class ByteWorkload(int seed, int batchBytes) : Workload
{
    private readonly byte[] _block = Pattern.Create(seed);
    private ByteStreamClient? _sender;
    private ByteStreamClient? _receiver;
    private TaskCompletionSource<bool> _received = NewReceived();
    private long _expectedBytes;

    public override string Mode => "bytes";

    public override async Task ConnectAsync(string hubUrl, int sendWindow, CancellationToken cancellationToken)
    {
        _receiver = new ByteStreamClientBuilder(batchBytes)
            .WithUrl(hubUrl)
            .Build();
        _receiver.OnTransferStarted += transfer =>
        {
            _ = Task.Run(() => ReceiveAsync(transfer, _received, _expectedBytes, cancellationToken), CancellationToken.None);
            return Task.CompletedTask;
        };
        await _receiver.ConnectAsync([ReceiverGroup], cancellationToken).ConfigureAwait(false);

        _sender = new ByteStreamClientBuilder(batchBytes)
            .WithUrl(hubUrl)
            .WithMaxInFlightBatches(sendWindow)
            .Build();
        await _sender.ConnectAsync([SenderGroup], cancellationToken).ConfigureAwait(false);
    }

    public override async Task<long> TransferAsync(long targetBodyBytes, CancellationToken cancellationToken)
    {
        _expectedBytes = targetBodyBytes;
        var received = _received = NewReceived();

        await using var send = await _sender!.InitTransferAsync(_receiver!.ClientId).ConfigureAwait(false);
        var sending = Task.Run(async () =>
        {
            await send.CopyFromAsync(new PatternStream(_block, targetBodyBytes), batchBytes, cancellationToken).ConfigureAwait(false);
            await send.CompleteAsync(cancellationToken).ConfigureAwait(false);
        }, CancellationToken.None);

        await AwaitBothAsync(sending, received.Task, cancellationToken).ConfigureAwait(false);
        return targetBodyBytes;
    }

    private async Task ReceiveAsync(ByteReceive transfer, TaskCompletionSource<bool> received, long expectedBytes, CancellationToken cancellationToken)
    {
        try
        {
            await using (transfer.ConfigureAwait(false))
            {
                var sink = new VerifyingStream(_block);
                await transfer.CopyToAsync(sink, cancellationToken).ConfigureAwait(false);
                if (sink.Length != expectedBytes)
                    throw new InvalidDataException($"The receiver got {sink.Length} bytes of the {expectedBytes} sent.");
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
        if (_sender is not null) await _sender.DisposeAsync().ConfigureAwait(false);
        if (_receiver is not null) await _receiver.DisposeAsync().ConfigureAwait(false);
    }
}
