namespace TransportR.Interfaces;

/// <summary>
/// Ends a transfer unilaterally: state removed, bridges removed, both participants told to cancel.
/// Or, for one that ended well, forgets it without telling anyone.
/// </summary>
/// <remarks>
/// The callers of <see cref="TerminateAsync"/> are <c>TransferCleanupService</c> (a
/// transfer gone silent or stalled), <c>BridgeCleanupService</c> (a bridge left
/// waiting for a peer), the access reconciler (a participant lost its right to act), the stream
/// route's abort request (a participant whose in-stream Abort cannot reach the wire) and a host
/// terminating a run it owns. The hub's data server calls <see cref="RetireAsync"/> when a data
/// stream ends cleanly. One implementation, so the sequence cannot drift between them the way a
/// hand-rolled copy in each caller would let it.
/// </remarks>
public interface ITransferTerminator
{
    /// <summary>
    /// Removes <paramref name="transferId"/>'s state and bridges, and tells both participants to
    /// cancel. Never throws: a failure at any step is logged and swallowed, since the caller has no
    /// action to take beyond what it already did.
    /// </summary>
    Task TerminateAsync(string transferId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes <paramref name="transferId"/>'s state and bridges, and tells nobody: both participants
    /// have seen the transfer end, and a cancellation would only reach sessions that are done. Never
    /// throws: a failure is logged, and the bridges and state left behind are the idle sweeps' to find.
    /// </summary>
    Task RetireAsync(string transferId);
}
