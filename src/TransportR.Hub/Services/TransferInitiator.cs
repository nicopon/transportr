using TransportR.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TransportR.Hub.Access;
using TransportR.Hub.Configuration;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Protocol;

namespace TransportR.Hub.Services;

/// <summary>
/// The transfer handshake, driven either by a sender through <c>POST {ClientBase}/{clientId}/transfers</c>
/// or by the host through <see cref="ITransferInitiator"/>.
/// </summary>
/// <remarks>
/// One implementation for both, so that authorization, the order in which the two clients are
/// readied and the cleanup of a handshake that fails halfway cannot drift apart. Every failure is a
/// <see cref="HubRequestFailedException"/>, whose message the control route forwards to a calling
/// client.
/// </remarks>
internal class TransferInitiator : ITransferInitiator
{
    private readonly ClientStreams _streams;
    private readonly IStateStore _stateStore;
    private readonly ITransferManager _transferManager;
    private readonly AccessPolicy _policy;
    private readonly AffinityOptions _affinityOptions;
    private readonly ILogger<TransferInitiator> _logger;
    private readonly TimeProvider _time;

    public TransferInitiator(
        ClientStreams streams,
        IStateStore stateStore,
        ITransferManager transferManager,
        AccessPolicy policy,
        IOptions<AffinityOptions> affinityOptions,
        ILogger<TransferInitiator> logger,
        TimeProvider timeProvider)
    {
        _time = timeProvider;
        _streams = streams;
        _stateStore = stateStore;
        _transferManager = transferManager;
        _policy = policy;
        _affinityOptions = affinityOptions.Value;
        _logger = logger;
    }

    public async Task<string> InitTransferAsync(
        Guid senderClientId,
        Guid receiverClientId,
        int batchSize,
        int timeoutMs,
        CancellationToken cancellationToken = default)
    {
        if (senderClientId == receiverClientId)
        {
            throw new ArgumentException("A transfer needs two distinct clients.", nameof(receiverClientId));
        }

        var sender = await _stateStore.GetClientAsync(senderClientId);
        if (sender == null)
        {
            _logger.LogWarning("InitTransfer by host failed: Source {SourceClientId} not found.", senderClientId);
            throw new HubRequestFailedException($"Source client {senderClientId} not found.");
        }

        var receiver = await _stateStore.GetClientAsync(receiverClientId);
        if (receiver == null)
        {
            _logger.LogWarning("InitTransfer by host failed: Target {TargetClientId} not found.", receiverClientId);
            throw new HubRequestFailedException($"Target client {receiverClientId} not found.");
        }

        ValidateTransferRequest(sender, receiver);

        // A sender that asks for a transfer is redirected to its receiver's hub instance. The host
        // has nothing to redirect, and a bridge lives on one instance, so the same condition that would
        // redirect is a refusal here: both clients' streams have to be on this instance.
        if (_affinityOptions.Enabled && !(_streams.Holds(sender.ConnectionId) && _streams.Holds(receiver.ConnectionId)))
        {
            _logger.LogWarning("InitTransfer by host denied: Source {SourceClientId} and Target {TargetClientId} are not both connected to this instance",
                sender.ClientId, receiver.ClientId);
            throw new HubRequestFailedException("Source and target clients are not both connected to this hub instance.");
        }

        return await HandshakeAsync(sender, receiver, batchSize, timeoutMs, requestSender: true, cancellationToken);
    }

    internal void ValidateTransferRequest(ClientRegistration sourceClient, ClientRegistration targetClient)
    {
        var verdict = _policy.Evaluate(sourceClient, targetClient);
        if (verdict == AccessVerdict.Allowed)
        {
            _logger.LogInformation("Transfer authorized: [{SourceGroups}] -> [{TargetGroups}]",
                string.Join(", ", sourceClient.Groups), string.Join(", ", targetClient.Groups));
            return;
        }

        // Who is at fault stays out of the verdict: only an expired token has two possible owners,
        // and the caller is told which one it is.
        var sourceAtFault = _policy.Evaluate(sourceClient) != AccessVerdict.Allowed;
        _logger.LogWarning(
            "InitTransfer denied ({Verdict}): Source {SourceClientId} [{SourceGroups}], Target {TargetClientId} [{TargetGroups}]",
            verdict, sourceClient.ClientId, string.Join(", ", sourceClient.Groups),
            targetClient.ClientId, string.Join(", ", targetClient.Groups));

        throw new HubRequestFailedException(verdict switch
        {
            AccessVerdict.TokenExpired when sourceAtFault => "Source client token has expired. Please reconnect with a fresh token.",
            AccessVerdict.TokenExpired => "Target client token has expired.",
            AccessVerdict.NoGroups => "Client groups not found.",
            _ => "Transfer not authorized."
        });
    }

    /// <summary>
    /// Readies the receiver, then, for a transfer the host opens, the sender, and returns the transfer
    /// id once they have accepted.
    /// </summary>
    /// <param name="requestSender">
    /// True when the host opens the transfer: the sender learns of it through a
    /// <see cref="ControlPlane.RequestSend"/> event. False when the sender asked for it: it learns the
    /// id from the answer to its request and starts its session from there.
    /// </param>
    internal async Task<string> HandshakeAsync(
        ClientRegistration sender,
        ClientRegistration receiver,
        int batchSize,
        int timeoutMs,
        bool requestSender,
        CancellationToken cancellationToken = default)
    {
        var transferId = Guid.NewGuid().ToString();

        _logger.LogInformation("Orchestrating Transfer: {TransferId}. Source: {SourceId} -> Target: {TargetId} (BatchSize={Size}, opened by {Opener})",
            transferId, sender.ClientId, receiver.ClientId, batchSize, requestSender ? "the host" : "the sender");

        await _transferManager.RegisterParticipantsAsync(transferId, sender.ClientId, receiver.ClientId);

        using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(timeoutMs), _time);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
        try
        {
            CallbackOutcome receiverReady;
            try
            {
                receiverReady = await _streams.AskAsync(receiver.ConnectionId, ControlPlane.ReadyToReceive,
                    requestId => new ReadyToReceiveEvent(requestId, transferId, sender.ClientId), cts.Token);
            }
            catch (IOException ex)
            {
                // The usual cause is a registration that outlived its process: the client is still in
                // the state store, but no stream of its is open here. Detail stays in the hub log; the
                // client gets something it can act on.
                _logger.LogWarning("Receiver {TargetClientId} unreachable on connection {ConnectionId} for {TransferId}: {Message}. Registration is likely stale.",
                    receiver.ClientId, receiver.ConnectionId, transferId, ex.Message);
                await _transferManager.RemoveTransferAsync(transferId);
                throw new HubRequestFailedException($"Target client {receiver.ClientId} is not reachable.");
            }

            if (receiverReady != CallbackOutcome.Accepted)
            {
                _logger.LogWarning("Receiver {TargetClientId} answered {Outcome} to {TransferId}.", receiver.ClientId, receiverReady, transferId);
                await _transferManager.RemoveTransferAsync(transferId);
                throw new HubRequestFailedException(receiverReady == CallbackOutcome.Refused
                    ? $"Target client {receiver.ClientId} refused the transfer."
                    : $"Target client {receiver.ClientId} failed to prepare for the transfer.");
            }

            if (!requestSender)
            {
                return StillOpen(transferId);
            }

            CallbackOutcome senderReady;
            try
            {
                senderReady = await _streams.AskAsync(sender.ConnectionId, ControlPlane.RequestSend,
                    requestId => new RequestSendEvent(requestId, transferId, batchSize, receiver.ClientId), cts.Token);
            }
            catch (IOException ex)
            {
                _logger.LogWarning("Sender unreachable for {TransferId}: {Message}. Cancelling Receiver.", transferId, ex.Message);
                await AbandonAsync(transferId, receiver);
                throw new HubRequestFailedException("Source client failed to prepare for the transfer.");
            }

            if (senderReady != CallbackOutcome.Accepted)
            {
                _logger.LogWarning("Sender {SourceClientId} answered {Outcome} to {TransferId}. Cancelling Receiver.",
                    sender.ClientId, senderReady, transferId);
                await AbandonAsync(transferId, receiver);
                throw new HubRequestFailedException(senderReady == CallbackOutcome.Refused
                    ? "Source client refused the transfer."
                    : "Source client failed to prepare for the transfer.");
            }
        }
        catch (OperationCanceledException)
        {
            await AbandonAsync(transferId, receiver);
            if (cancellationToken.IsCancellationRequested) throw;

            _logger.LogWarning("Handshake timed out for Transfer: {TransferId}", transferId);
            throw new HubRequestFailedException($"Handshake timed out after {timeoutMs}ms");
        }

        return StillOpen(transferId);
    }

    /// <summary>
    /// <paramref name="transferId"/>, unless the transfer was ended while its participants were being
    /// asked: a renewal that withdrew a right, the access sweep or the inactivity check can end it
    /// then, and its id would send the caller after a transfer nobody will open again.
    /// </summary>
    private string StillOpen(string transferId)
    {
        if (_transferManager.GetTransfer(transferId) != null) return transferId;

        _logger.LogWarning("Transfer {TransferId} was ended during its handshake", transferId);
        throw new HubRequestFailedException("The transfer was ended before its handshake completed.");
    }

    private async Task AbandonAsync(string transferId, ClientRegistration receiver)
    {
        await _transferManager.RemoveTransferAsync(transferId);
        _streams.TrySend(receiver.ConnectionId, ControlPlane.CancelTransfer, new CancelTransferEvent(transferId));
    }
}
