namespace TransportR.Interfaces;

/// <summary>
/// Forgets a transfer that ended well. The data server calls it when the sender's data stream ends
/// cleanly, the one signal that a transfer is over.
/// </summary>
/// <remarks>
/// Kept off <see cref="TransportR.Hub.ITransferTerminator"/>, which a host resolves: a host calling it would drop a
/// live transfer without either client hearing of it.
/// </remarks>
internal interface ITransferRetirer
{
    /// <summary>
    /// Removes <paramref name="transferId"/>'s state and bridges, and tells nobody: both participants
    /// have seen the transfer end, and a cancellation would only reach sessions that are done. Never
    /// throws: a failure is logged, and the bridges and state left behind are the idle sweeps' to find.
    /// </summary>
    Task RetireAsync(string transferId);
}
