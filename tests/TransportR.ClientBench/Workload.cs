using TransportR.Client;

namespace TransportR.ClientBench;

/// <summary>
/// What one mode of the harness carries: a sender and a receiver of this process, registered with the
/// hub, and a transfer between them whose receiver checks every byte. Both ends go through the public
/// API of the client only.
/// </summary>
internal abstract class Workload : IAsyncDisposable
{
    protected const string SenderGroup = "senders";
    protected const string ReceiverGroup = "receivers";

    private static readonly TimeSpan FailureGrace = TimeSpan.FromSeconds(5);

    public abstract string Mode { get; }

    /// <summary>Registers both ends with the hub, so that a transfer can be opened at once.</summary>
    public abstract Task ConnectAsync(string hubUrl, int sendWindow, CancellationToken cancellationToken);

    /// <summary>
    /// Carries about <paramref name="targetBodyBytes"/> of batch body from the sender to the receiver and
    /// returns once the sender's handle is disposed and the receiver has verified everything.
    /// </summary>
    /// <returns>The bytes of batch body the transfer carried.</returns>
    public abstract Task<long> TransferAsync(long targetBodyBytes, CancellationToken cancellationToken);

    public abstract ValueTask DisposeAsync();

    /// <summary>
    /// Waits for both ends, and fails at once when the receiver does: a sender whose receiver rejected
    /// the data would otherwise wait out its own stall bound before the run said so.
    /// </summary>
    protected static async Task AwaitBothAsync(Task sending, Task receiving, CancellationToken cancellationToken)
    {
        await Task.WhenAny(sending, receiving).WaitAsync(cancellationToken).ConfigureAwait(false);
        if (receiving.IsFaulted)
            await receiving.ConfigureAwait(false);

        // A sender fails with a generic message when its receiver rejects the data; the receiver's own
        // exception, which says what differed, follows within moments.
        if (sending.IsFaulted)
            await Task.WhenAny(receiving, Task.Delay(FailureGrace, cancellationToken)).ConfigureAwait(false);
        if (receiving.IsFaulted)
            await receiving.ConfigureAwait(false);

        await Task.WhenAll(sending, receiving).WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    protected static TaskCompletionSource<bool> NewReceived()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);

    protected static async ValueTask DisposeClientAsync<T>(TransportRClient<T>? client)
    {
        if (client is not null)
            await client.DisposeAsync().ConfigureAwait(false);
    }
}
