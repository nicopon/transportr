namespace TransportR.Client.Sessions;

/// <summary>
/// What a session asks of the control plane. Each request carries the session's affinity, so that it
/// reaches the hub instance holding the transfer's bridge.
/// </summary>
internal interface IControlPlane
{
    /// <summary>Reports the session's statistics, which also show the transfer active.</summary>
    Task ReportTransferAsync(string transferId, string? affinity, long items, long acks, CancellationToken cancellationToken);

    /// <summary>
    /// Whether the hub answers, before the session reopens its streams. A refusal is an answer: what
    /// the hub knows of the transfer is for the streams to find out.
    /// </summary>
    Task<bool> ProbeAsync(string transferId, string? affinity, CancellationToken cancellationToken);

    void RemoveSession(string transferId);
}
