using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TransportR.Client;
using TransportR.Serialization.MessagePack;

namespace DocSnippets;

internal static class ClientHosting
{
    public static void ConfigureHostedReceiver(HostApplicationBuilder builder, string hubUrl)
    {
        builder.Services.AddTransportRClient<Order>(client => client
            .WithUrl(hubUrl)
            .WithReceiveCapacity(maxItems: 1000)
            .WithMessagePackSerialization());

        builder.Services.AddHostedService<OrderReceiverWorker>();
    }

    private sealed class OrderReceiverWorker(
        IDataReceiver<Order> receiver,
        ILogger<OrderReceiverWorker> logger) : BackgroundService
    {
        private readonly ConcurrentDictionary<string, Task> _active = new();

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            receiver.OnTransferStarted(transfer =>
            {
                var task = HandleTransferAsync(transfer, stoppingToken);
                _active[transfer.TransferId] = task;
                _ = task.ContinueWith(
                    _ => _active.TryRemove(transfer.TransferId, out Task? _),
                    TaskScheduler.Default);
                return Task.CompletedTask;
            });

            await receiver.ConnectAsync(["receivers"], stoppingToken);

            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
            }

            await Task.WhenAll(_active.Values);
            await receiver.DisconnectAsync();
        }

        private async Task HandleTransferAsync(Receive<Order> transfer, CancellationToken stoppingToken)
        {
            await using (transfer)
            {
                await foreach (var order in transfer.ReceiveAsync(stoppingToken))
                {
                    logger.LogDebug("Received order {OrderId} from {SenderId}", order.Id, transfer.SenderId);
                }
            }
        }
    }

    private sealed record Order(int Id);
}
