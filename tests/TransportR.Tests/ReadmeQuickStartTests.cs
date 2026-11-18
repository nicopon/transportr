using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using TransportR.DocSnippets;
using TransportR.Hub;
using TransportR.Tests.Infrastructure;
using Xunit.Abstractions;

namespace TransportR.Tests;

/// <summary>
/// Console.Out is process-wide: a test that replaces it to read what a snippet prints runs alone.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class ConsoleOutCollection
{
    public const string Name = "Console.Out";
}

/// <summary>
/// Runs the sender and the receiver of the README's Quick Start against a local hub, as a reader
/// who pastes them into a new project would.
/// </summary>
[Collection(ConsoleOutCollection.Name)]
public sealed class ReadmeQuickStartTests(ITestOutputHelper output)
{
    private static readonly TimeSpan Ceiling = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task The_Readme_Sender_And_Receiver_Deliver_Every_Order()
    {
        const int expected = ReadmeQuickStart.OrderCount;
        using var ceiling = new CancellationTokenSource(Ceiling);
        using var stop = new CancellationTokenSource();
        var store = new WatchedStore();
        var clock = Stopwatch.StartNew();

        // The hub as the README builds it: UseDevMode() with no default group. The snippets never
        // dispose their clients, so their open requests would hold the hub's shutdown for its full
        // timeout.
        await using var hub = await TestHubServerFactory.StartLoopbackAsync(services =>
        {
            services.AddDataHub().UseDevMode();
            services.Replace(ServiceDescriptor.Singleton<IStateStore>(store));
            services.Configure<HostOptions>(options => options.ShutdownTimeout = TimeSpan.FromSeconds(1));
        });

        var original = Console.Out;
        var printed = new ReceivedLines(expected);
        Console.SetOut(printed);
        try
        {
            var receiver = Task.Run(() => ReadmeQuickStart.Receiver(hub.BaseUrl, stop.Token));

            // The sender takes the first receiver that is not disconnected and throws if none: the receiver must already be registered.
            await Task.WhenAny(store.ReceiverRegistered, receiver).WaitAsync(ceiling.Token);
            if (receiver.IsCompleted)
            {
                await receiver;
            }
            Assert.True(store.ReceiverRegistered.IsCompleted, "the receiver ended before it registered");

            await Task.Run(() => ReadmeQuickStart.Sender(hub.BaseUrl)).WaitAsync(ceiling.Token);

            await printed.Complete.WaitAsync(ceiling.Token);
            await store.TransferRetired.WaitAsync(ceiling.Token);
            Assert.Equal(Enumerable.Range(0, expected), printed.Ids.Order());
            Assert.False(receiver.IsCompleted, "the receiver ended on its own, before it was told to stop");

            await stop.CancelAsync();
            await receiver.WaitAsync(ceiling.Token);
        }
        finally
        {
            await stop.CancelAsync();
            Console.SetOut(original);
            output.WriteLine($"received {printed.Ids.Count} of {expected} after {clock.Elapsed.TotalSeconds:F1} s");
        }
    }

    /// <summary>Reads the receiver snippet's <c>Received: {id}</c> lines out of what the process prints.</summary>
    private sealed class ReceivedLines(int expected) : TextWriter
    {
        private const string Prefix = "Received: ";
        private readonly object _gate = new();
        private readonly StringBuilder _line = new();
        private readonly List<int> _ids = [];
        private readonly TaskCompletionSource _complete = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override Encoding Encoding => Encoding.UTF8;

        public Task Complete => _complete.Task;

        public IReadOnlyList<int> Ids
        {
            get
            {
                lock (_gate)
                {
                    return [.. _ids];
                }
            }
        }

        public override void Write(char value)
        {
            lock (_gate)
            {
                if (value != '\n')
                {
                    _line.Append(value);
                    return;
                }

                var line = _line.ToString().TrimEnd('\r');
                _line.Clear();
                if (line.StartsWith(Prefix, StringComparison.Ordinal) && int.TryParse(line.AsSpan(Prefix.Length), out var id))
                {
                    _ids.Add(id);
                    if (_ids.Count == expected)
                    {
                        _complete.TrySetResult();
                    }
                }
            }
        }
    }

    /// <summary>
    /// An in-memory store that says when the receiver has registered and when the hub has retired
    /// the transfer, so that the test waits on those instead of on a delay.
    /// </summary>
    private sealed class WatchedStore : IStateStore
    {
        private readonly InMemoryStateStore _inner = new();
        private readonly TaskCompletionSource _receiverRegistered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _transferRetired = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task ReceiverRegistered => _receiverRegistered.Task;

        public Task TransferRetired => _transferRetired.Task;

        public async Task SaveClientAsync(ClientRegistration client)
        {
            await _inner.SaveClientAsync(client);
            if (client.Groups.Contains("receivers"))
            {
                _receiverRegistered.TrySetResult();
            }
        }

        public async Task DeleteSessionAsync(string transferId)
        {
            await _inner.DeleteSessionAsync(transferId);
            _transferRetired.TrySetResult();
        }

        public Task SaveSessionAsync(TransferSession session) => _inner.SaveSessionAsync(session);
        public Task<TransferSession?> GetSessionAsync(string transferId) => _inner.GetSessionAsync(transferId);
        public Task<int> CountTransfersOpenedByAsync(Guid senderId) => _inner.CountTransfersOpenedByAsync(senderId);
        public Task ExtendSessionExpiryAsync(string transferId, DateTime newExpiry) => _inner.ExtendSessionExpiryAsync(transferId, newExpiry);
        public Task<ClientRegistration?> RenewClientAsync(Guid clientId, string? expectedSubject, IReadOnlySet<string> groups, DateTime? tokenExpiry) =>
            _inner.RenewClientAsync(clientId, expectedSubject, groups, tokenExpiry);
        public Task<ClientRegistration?> GetClientAsync(Guid clientId) => _inner.GetClientAsync(clientId);
        public Task<ClientRegistration?> GetClientByConnectionIdAsync(string connectionId) => _inner.GetClientByConnectionIdAsync(connectionId);
        public Task<IReadOnlyList<ClientRegistration>> GetAllClientsAsync() => _inner.GetAllClientsAsync();
        public Task MarkDisconnectedAsync(string connectionId, DateTime whenUtc) => _inner.MarkDisconnectedAsync(connectionId, whenUtc);
        public Task DeleteClientAsync(Guid clientId) => _inner.DeleteClientAsync(clientId);
    }
}
