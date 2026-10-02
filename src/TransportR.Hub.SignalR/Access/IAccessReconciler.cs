namespace TransportR.Hub.SignalR.Access;

/// <summary>
/// Makes what is running agree with the policy now in force. The same pass runs on a schedule, after
/// a renewal, and when the host calls it, so that a right withdrawn by the host applies without
/// waiting for anyone to renew.
/// </summary>
/// <remarks>
/// For a client whose token has run out or that holds no group, it ends the transfers the client
/// takes part in and then closes the connection, if this instance holds it, so that the client is
/// told before it is cut. For a transfer whose participants the policy no longer lets reach each
/// other, it ends the transfer. It never refuses anything new: that is the policy's, at the
/// handshake. A failure of the host's flow control service is logged and ends nothing: an error is
/// not a verdict.
/// </remarks>
public interface IAccessReconciler
{
    /// <summary>Judges one client, and the transfers it takes part in.</summary>
    Task ReconcileAsync(Guid clientId, CancellationToken cancellationToken = default);

    /// <summary>Judges every registered client, and every transfer this instance runs.</summary>
    Task ReconcileAllAsync(CancellationToken cancellationToken = default);
}
