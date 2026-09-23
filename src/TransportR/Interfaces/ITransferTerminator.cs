namespace TransportR.Interfaces;

/// <summary>
/// Ends a transfer unilaterally: state removed, bridges removed, both participants told to cancel.
/// </summary>
/// <remarks>
/// The only two callers are <see cref="Services.TransferCleanupService"/> (a transfer gone stale or
/// stalled) and a host terminating a run it owns. One implementation, on the model of the single
/// handshake extracted in C0: the sequence cannot drift between the two the way a hand-rolled copy
/// in each caller would let it.
/// </remarks>
public interface ITransferTerminator
{
    /// <summary>
    /// Removes <paramref name="transferId"/>'s state and bridges, and tells both participants to
    /// cancel. Never throws: a failure at any step is logged and swallowed, since the caller has no
    /// action to take beyond what it already did.
    /// </summary>
    Task TerminateAsync(string transferId, CancellationToken cancellationToken = default);
}
