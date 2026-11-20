namespace TransportR.Client.Sessions;

/// <summary>
/// The two clients of a transfer as this end holds them: the sender, its own id and the target it named
/// or the hub announced; the receiver, the sender the hub announced and its own. End-to-end encryption
/// binds both into its keys, so two ends that do not hold the same pair do not agree on them.
/// </summary>
internal readonly record struct TransferParties(Guid SenderId, Guid ReceiverId);
