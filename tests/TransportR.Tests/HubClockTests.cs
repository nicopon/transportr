using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using TransportR.Hub;
using TransportR.Interfaces;
using TransportR.Services;

namespace TransportR.Tests;

/// <summary>
/// The hub reads time through the <see cref="TimeProvider"/> it is given, so that a timeout or an
/// expiry can be tested by moving a clock rather than by waiting for one.
/// </summary>
public class HubClockTests
{
    [Fact]
    public async Task AddDataHub_KeepsATimeProviderTheHostRegistered()
    {
        var clock = new FakeTimeProvider(new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero));

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<TimeProvider>(clock);
        services.AddDataHub().UseDevMode().Build();
        await using var provider = services.BuildServiceProvider();

        var transfers = provider.GetRequiredService<ITransferManager>();
        await transfers.RegisterParticipantsAsync("transfer-1", Guid.NewGuid(), Guid.NewGuid());

        Assert.Equal(clock.GetUtcNow().UtcDateTime, transfers.GetTransfer("transfer-1")!.LastActivityUtc);
    }

    [Fact]
    public async Task DroppedRegistration_IsEvictedWhenTheGraceRunsOutOnTheClock()
    {
        var clock = new FakeTimeProvider();
        var grace = TimeSpan.FromSeconds(new ClientRegistryConfiguration().DisconnectedGraceSeconds);
        var store = new InMemoryStateStore(Options.Create(new ClientRegistryConfiguration()), clock);
        var clientId = Guid.NewGuid();

        await store.SaveClientAsync(new ClientRegistration(clientId, "connection-1", ["test"], null, null, clock.GetUtcNow().UtcDateTime));
        await store.MarkDisconnectedAsync("connection-1", clock.GetUtcNow().UtcDateTime);

        clock.Advance(grace);
        Assert.NotNull(await store.GetClientAsync(clientId));

        clock.Advance(TimeSpan.FromSeconds(1));
        Assert.Null(await store.GetClientAsync(clientId));
    }

    [Fact]
    public Task InactiveTransfer_IsTerminatedWhenTheClockPassesTheTimeout() =>
        AssertTerminatedOnTheClockAsync(async transfers =>
        {
            await transfers.RegisterParticipantsAsync("transfer-1", Guid.NewGuid(), Guid.NewGuid());
            await transfers.UpdateSenderStatsAsync("transfer-1", itemsSent: 1, acksReceived: 0);
        });

    [Fact]
    public Task ATransferThatNeverReported_IsTerminatedWhenTheClockPassesTheTimeout() =>
        AssertTerminatedOnTheClockAsync(transfers =>
            transfers.RegisterParticipantsAsync("transfer-1", Guid.NewGuid(), Guid.NewGuid()));

    private static async Task AssertTerminatedOnTheClockAsync(Func<TransferManager, Task> startTheTransfer)
    {
        var clock = new FakeTimeProvider();
        var config = new DataPlaneConfiguration();
        var transfers = new TransferManager(new InMemoryStateStore(timeProvider: clock), clock);
        var terminator = new RecordingTerminator();
        using var cleanup = new TransferCleanupService(
            transfers, terminator, Options.Create(config), NullLogger<TransferCleanupService>.Instance, clock);

        await startTheTransfer(transfers);
        await cleanup.StartAsync(CancellationToken.None);

        // The service creates its timer on a background thread, at a moment the test cannot see, so the
        // clock moves in check-sized steps until the timer has run past the inactivity timeout. The
        // bound is on the clock: a service reading the system clock would never get there.
        var step = TimeSpan.FromSeconds(10);
        var limit = TimeSpan.FromSeconds(config.TransferInactivityTimeoutSeconds * 3);
        for (var moved = TimeSpan.Zero; moved < limit && !terminator.Terminated.Task.IsCompleted; moved += step)
        {
            clock.Advance(step);
            await Task.WhenAny(terminator.Terminated.Task, Task.Delay(100));
        }

        Assert.True(terminator.Terminated.Task.IsCompleted, "the transfer was not terminated on the injected clock");
        Assert.Equal("transfer-1", await terminator.Terminated.Task);
        await cleanup.StopAsync(CancellationToken.None);
    }

    private sealed class RecordingTerminator : ITransferTerminator
    {
        public TaskCompletionSource<string> Terminated { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task TerminateAsync(string transferId, CancellationToken cancellationToken = default)
        {
            Terminated.TrySetResult(transferId);
            return Task.CompletedTask;
        }

    }
}
