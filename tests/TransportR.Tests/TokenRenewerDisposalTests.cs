using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Client.Configuration;
using TransportR.Client.Internal;

namespace TransportR.Tests;

public class TokenRenewerDisposalTests
{
    private static TokenRenewer Create(out ControlChannel channel, HttpMessageHandler? hub = null)
    {
        channel = new ControlChannel("http://unused", Guid.NewGuid(), new HttpClient(hub ?? new HttpClientHandler()), new ClientOptions(), NullLogger.Instance);
        return new TokenRenewer(channel, TimeSpan.FromSeconds(30), NullLogger.Instance);
    }

    [Fact]
    public async Task DisposeAsync_CalledTwice_IsNotAnError()
    {
        var renewer = Create(out var channel);
        await using var _ = channel;

        await renewer.DisposeAsync();
        await renewer.DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_AfterStart_CalledTwice_IsNotAnError()
    {
        var renewer = Create(out var channel);
        await using var _ = channel;

        renewer.Start();
        await renewer.DisposeAsync();
        await renewer.DisposeAsync();
    }

    [Fact]
    public async Task StartAndPushNow_AfterDisposal_StartNothing()
    {
        var renewer = Create(out var channel);
        await using var _ = channel;

        await renewer.DisposeAsync();

        renewer.Start();
        renewer.PushNow();
    }

    [Fact]
    public async Task PushNowAsync_AfterDisposal_IsRefused()
    {
        var renewer = Create(out var channel);
        await using var _ = channel;

        await renewer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => renewer.PushNowAsync("token", CancellationToken.None));
    }

    [Fact]
    public async Task PushNowAsync_InFlightAtDisposal_IsCancelled()
    {
        // The caller of a push is waiting on the hub; a client that goes meanwhile must not leave it
        // waiting on a loop that has ended.
        var hub = new SilentHub();
        var renewer = Create(out var channel, hub);
        await using var _ = channel;

        var push = renewer.PushNowAsync("token", CancellationToken.None);
        await hub.Asked.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await renewer.DisposeAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => push.WaitAsync(TimeSpan.FromSeconds(10)));
    }

    /// <summary>A hub that takes the request and never answers it.</summary>
    private sealed class SilentHub : HttpMessageHandler
    {
        public TaskCompletionSource Asked { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Asked.TrySetResult();
            await Task.Delay(Timeout.Infinite, cancellationToken);
            throw new InvalidOperationException("unreachable");
        }
    }
}
