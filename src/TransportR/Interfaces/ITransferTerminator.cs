namespace TransportR.Interfaces;

/// <summary>
/// Ends a transfer unilaterally: state removed, bridges removed, both participants told to cancel.
/// </summary>
/// <remarks>
/// The callers are <c>TransferCleanupService</c> (a transfer gone silent or stalled),
/// <c>BridgeCleanupService</c> (a bridge left waiting for a peer), the access reconciler (a
/// participant lost its right to act), the stream route's abort request (a participant whose
/// in-stream Abort cannot reach the wire) and a host terminating a run it owns. One implementation,
/// so the sequence cannot drift between them the way a hand-rolled copy in each caller would let it.
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
