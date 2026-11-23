using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using TransportR.Client;
using TransportR.Hub;
using TransportR.Serialization.MessagePack;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

public class DiagnosticsTests
{
    [Fact]
    public async Task HubAndClientDiagnostics_RecordMetricsAndActivitiesAcrossCompletedTransfer()
    {
        var hubMeterFactory = new TrackingMeterFactory();
        var hubMeasurements = new ConcurrentQueue<(string Name, long LongValue, int IntValue, KeyValuePair<string, object?>[] Tags)>();
        var clientMeasurements = new ConcurrentQueue<(string Name, long LongValue, int IntValue, KeyValuePair<string, object?>[] Tags)>();
        var activities = new ConcurrentQueue<Activity>();

        using var hubMeterListener = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                if (hubMeterFactory.Owns(instrument.Meter))
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            }
        };
        hubMeterListener.SetMeasurementEventCallback<long>((inst, val, tags, _) =>
            hubMeasurements.Enqueue((inst.Name, val, 0, tags.ToArray())));
        hubMeterListener.SetMeasurementEventCallback<int>((inst, val, tags, _) =>
            hubMeasurements.Enqueue((inst.Name, 0, val, tags.ToArray())));
        hubMeterListener.Start();

        using var clientMeterListener = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == TransportRClientDiagnostics.MeterName)
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            }
        };
        clientMeterListener.SetMeasurementEventCallback<long>((inst, val, tags, _) =>
            clientMeasurements.Enqueue((inst.Name, val, 0, tags.ToArray())));
        clientMeterListener.SetMeasurementEventCallback<int>((inst, val, tags, _) =>
            clientMeasurements.Enqueue((inst.Name, 0, val, tags.ToArray())));
        clientMeterListener.Start();

        using var activityListener = new ActivityListener
        {
            ShouldListenTo = source => source.Name is TransportRHubDiagnostics.ActivitySourceName or TransportRClientDiagnostics.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => activities.Enqueue(activity),
        };
        ActivitySource.AddActivityListener(activityListener);

        using var host = TestHubServerFactory.CreateHost(services =>
        {
            services.AddSingleton<IMeterFactory>(hubMeterFactory);
        });
        var handler = host.GetTestServer().CreateHandler();

        await using var sender = new TransportRClientBuilder<string>()
            .WithUrl("http://localhost")
            .WithHttpMessageHandler(handler)
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithMessagePackSerialization()
            .Build();
        await using var receiver = new TransportRClientBuilder<string>()
            .WithUrl("http://localhost")
            .WithHttpMessageHandler(handler)
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithMessagePackSerialization()
            .Build();

        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted(async receive =>
        {
            await using (receive)
            {
                await foreach (var _ in receive.ReceiveAsync()) { }
            }
            received.TrySetResult();
        });

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        // While both clients are connected and before any transfer, record gauge sample.
        hubMeterListener.RecordObservableInstruments();
        Assert.Contains(hubMeasurements, m => m.Name == "transportr.hub.clients.connected" && m.IntValue == 2);

        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 2);
        var transferId = send.TransferId;
        await send.SendAsync("hello");
        await send.SendAsync("world");
        await send.CompleteAsync();
        await received.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // Give Hub RetireAsync a brief moment after sender data EOF completes.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (!hubMeasurements.Any(m => m.Name == "transportr.hub.transfers.ended") && DateTime.UtcNow < deadline)
        {
            await Task.Delay(20);
        }

        hubMeterListener.RecordObservableInstruments();

        // Hub metrics
        Assert.Contains(hubMeasurements, m => m.Name == "transportr.hub.bytes.relayed" && m.LongValue > 0);
        Assert.Contains(hubMeasurements, m =>
            m.Name == "transportr.hub.write_streams.opened" &&
            m.Tags.Any(t => t.Key == "transportr.transport" && (string?)t.Value == "segments") &&
            m.Tags.Any(t => t.Key == "transportr.connection_type" && (string?)t.Value == "data"));
        Assert.Contains(hubMeasurements, m =>
            m.Name == "transportr.hub.transfers.ended" &&
            m.Tags.Any(t => t.Key == "transportr.end_reason" && (string?)t.Value == "completed"));
        Assert.Contains(hubMeasurements, m => m.Name == "transportr.hub.transfers.active" && m.IntValue == 0);

        // Client metrics (bytes & completion)
        Assert.Contains(clientMeasurements, m =>
            m.Name == "transportr.client.bytes.transferred" &&
            m.LongValue > 0 &&
            m.Tags.Any(t => t.Key == "transportr.direction" && (string?)t.Value == "sending"));
        Assert.Contains(clientMeasurements, m =>
            m.Name == "transportr.client.bytes.transferred" &&
            m.LongValue > 0 &&
            m.Tags.Any(t => t.Key == "transportr.direction" && (string?)t.Value == "receiving"));
        Assert.Contains(clientMeasurements, m =>
            m.Name == "transportr.client.transfers.completed" &&
            m.Tags.Any(t => t.Key == "transportr.outcome" && (string?)t.Value == "completed"));

        // Activities for this specific transfer
        Assert.Contains(activities, a =>
            a.Source.Name == TransportRHubDiagnostics.ActivitySourceName &&
            a.OperationName == "transportr.hub.init_transfer" &&
            (string?)a.GetTagItem("transportr.transfer.id") == transferId);
        Assert.Contains(activities, a =>
            a.Source.Name == TransportRClientDiagnostics.ActivitySourceName &&
            a.OperationName == "transportr.client.init_transfer" &&
            (string?)a.GetTagItem("transportr.transfer.id") == transferId);
    }

    [Fact]
    public async Task WebSocketUpgrade_PropagatesW3CTraceParentWhenActivityIsCurrent()
    {
        var seenTraceParents = new ConcurrentQueue<string>();

        var host = new HostBuilder()
            .ConfigureWebHost(webBuilder => webBuilder
                .UseKestrel()
                .UseUrls("http://127.0.0.1:0")
                .ConfigureServices(services =>
                {
                    services.AddDataHub().UseDevMode(o =>
                    {
                        o.AllowAnonymous = true;
                        o.DefaultGroups = ["test"];
                    });
                })
                .Configure(app =>
                {
                    app.Use(async (context, next) =>
                    {
                        if (context.Request.Headers.TryGetValue("traceparent", out var tp)
                            && context.Request.Path.Value?.EndsWith("/send", StringComparison.Ordinal) == true)
                        {
                            seenTraceParents.Enqueue(tp.ToString());
                        }
                        await next();
                    });
                    app.UseRouting();
                    app.UseEndpoints(endpoints => endpoints.MapDataHub(requireAuth: false));
                }))
            .Build();

        await host.StartAsync();
        try
        {
            var baseUrl = host.Services.GetRequiredService<IServer>()
                .Features.Get<IServerAddressesFeature>()!.Addresses.First();

            await using var sender = new TransportRClientBuilder<string>()
                .WithUrl(baseUrl)
                .WithWriteTransport(WriteTransportPolicy.RequireWebSocket)
                .WithMessagePackSerialization()
                .Build();
            await using var receiver = new TransportRClientBuilder<string>()
                .WithUrl(baseUrl)
                .WithWriteTransport(WriteTransportPolicy.RequireWebSocket)
                .WithMessagePackSerialization()
                .Build();

            var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            receiver.OnTransferStarted(async receive =>
            {
                await using (receive)
                {
                    await foreach (var _ in receive.ReceiveAsync()) { }
                }
                done.TrySetResult();
            });

            await sender.ConnectAsync();
            await receiver.ConnectAsync();

            using var parentActivity = new Activity("test-root").SetIdFormat(ActivityIdFormat.W3C).Start();
            await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 1);
            await send.SendAsync("traced");
            await send.CompleteAsync();
            await done.Task.WaitAsync(TimeSpan.FromSeconds(10));

            Assert.Contains(seenTraceParents, tp => tp.StartsWith($"00-{parentActivity.TraceId}-", StringComparison.Ordinal));
        }
        finally
        {
            await host.StopAsync();
            host.Dispose();
        }
    }

    [Fact]
    public async Task HostTermination_RecordsHubEndReasonAndClientFailureCategory()
    {
        var hubMeterFactory = new TrackingMeterFactory();
        var hubEndReasons = new ConcurrentQueue<string?>();
        var clientCompletedTags = new ConcurrentQueue<KeyValuePair<string, object?>[]>();

        using var hubListener = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                if (hubMeterFactory.Owns(instrument.Meter) && instrument.Name == "transportr.hub.transfers.ended")
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            }
        };
        hubListener.SetMeasurementEventCallback<long>((_, _, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (tag.Key == "transportr.end_reason")
                {
                    hubEndReasons.Enqueue(tag.Value?.ToString());
                }
            }
        });
        hubListener.Start();

        using var clientListener = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == TransportRClientDiagnostics.MeterName && instrument.Name == "transportr.client.transfers.completed")
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            }
        };
        clientListener.SetMeasurementEventCallback<long>((_, _, tags, _) => clientCompletedTags.Enqueue(tags.ToArray()));
        clientListener.Start();

        using var host = TestHubServerFactory.CreateHost(services =>
        {
            services.AddSingleton<IMeterFactory>(hubMeterFactory);
        });
        var handler = host.GetTestServer().CreateHandler();

        await using var sender = new TransportRClientBuilder<string>()
            .WithUrl("http://localhost")
            .WithHttpMessageHandler(handler)
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithMessagePackSerialization()
            .Build();
        await using var receiver = new TransportRClientBuilder<string>()
            .WithUrl("http://localhost")
            .WithHttpMessageHandler(handler)
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithMessagePackSerialization()
            .Build();

        var started = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted(r =>
        {
            started.TrySetResult(r);
            return Task.CompletedTask;
        });

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 10);
        await using var receive = await started.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await host.Services.GetRequiredService<ITransferTerminator>().TerminateAsync(send.TransferId);

        await Assert.ThrowsAsync<TransferFailedException>(async () =>
        {
            await foreach (var _ in receive.ReceiveAsync()) { }
        });

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (!clientCompletedTags.Any(tags =>
                   tags.Any(t => t.Key == "transportr.outcome" && (string?)t.Value == "failed")
                   && tags.Any(t => t.Key == "transportr.failure_category" && (string?)t.Value == nameof(TransferFailureCategory.EndedByHub)))
               && DateTime.UtcNow < deadline)
        {
            await Task.Delay(20);
        }

        Assert.Contains("host", hubEndReasons);
        Assert.Contains(clientCompletedTags, tags =>
            tags.Any(t => t.Key == "transportr.outcome" && (string?)t.Value == "failed")
            && tags.Any(t => t.Key == "transportr.failure_category" && (string?)t.Value == nameof(TransferFailureCategory.EndedByHub)));
    }

    private sealed class TrackingMeterFactory : IMeterFactory
    {
        private readonly List<Meter> _meters = [];

        public Meter Create(MeterOptions options)
        {
            var meter = new Meter(options);
            lock (_meters) _meters.Add(meter);
            return meter;
        }

        public bool Owns(Meter meter)
        {
            lock (_meters) return _meters.Contains(meter);
        }

        public void Dispose()
        {
            lock (_meters)
            {
                foreach (var m in _meters) m.Dispose();
                _meters.Clear();
            }
        }
    }
}
