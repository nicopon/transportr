using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TransportR.Hub.SignalR.Configuration;
using TransportR.Hub.SignalR.Hubs;
using TransportR.Interfaces;
using TransportR.Models;

namespace TransportR.Hub.SignalR.Services;

/// <summary>
/// The transfer handshake, driven either by a sender through <see cref="CommandHub.InitTransfer"/>
/// or by the host through <see cref="ITransferInitiator"/>.
/// </summary>
/// <remarks>
/// One implementation for both, so that authorization, the order in which the two clients are
/// readied and the cleanup of a handshake that fails halfway cannot drift apart. Every failure is a
/// <see cref="HubException"/>, whose message SignalR forwards to a calling client.
/// </remarks>
public class SignalRTransferInitiator : ITransferInitiator
{
    private readonly IHubContext<CommandHub> _hubContext;
    private readonly IStateStore _stateStore;
    private readonly ITransferManager _transferManager;
    private readonly IFlowControlService? _flowControlService;
    private readonly AffinityOptions _affinityOptions;
    private readonly ILogger<SignalRTransferInitiator> _logger;

    public SignalRTransferInitiator(
        IHubContext<CommandHub> hubContext,
        IStateStore stateStore,
        ITransferManager transferManager,
        IOptions<AffinityOptions> affinityOptions,
        ILogger<SignalRTransferInitiator> logger,
        IFlowControlService? flowControlService = null)
    {
        _hubContext = hubContext;
        _stateStore = stateStore;
        _transferManager = transferManager;
        _affinityOptions = affinityOptions.Value;
        _logger = logger;
        _flowControlService = flowControlService;
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
            throw new HubException($"Source client {senderClientId} not found.");
        }

        var receiver = await _stateStore.GetClientAsync(receiverClientId);
        if (receiver == null)
        {
            _logger.LogWarning("InitTransfer by host failed: Target {TargetClientId} not found.", receiverClientId);
            throw new HubException($"Target client {receiverClientId} not found.");
        }

        ValidateTransferRequest(sender, receiver);

        // A sender that asks for a transfer is redirected to its receiver's hub instance. The host
        // has no connection to redirect, and a bridge lives on one instance, so the same condition
        // that would redirect is a refusal here.
        if (_affinityOptions.Enabled &&
            !string.IsNullOrEmpty(receiver.AffinityToken) &&
            sender.AffinityToken != receiver.AffinityToken)
        {
            _logger.LogWarning("InitTransfer by host denied: Source {SourceClientId} on {SourceAffinity}, Target {TargetClientId} on {TargetAffinity}",
                sender.ClientId, sender.AffinityToken, receiver.ClientId, receiver.AffinityToken);
            throw new HubException("Source and target clients are connected to different hub instances.");
        }

        return await HandshakeAsync(
            sender, sender.ConnectionId, receiver, batchSize, timeoutMs,
            nameof(ICommandClient.RequestSend), cancellationToken);
    }

    internal void ValidateTransferRequest(ClientRegistration sourceClient, ClientRegistration targetClient)
    {
        if (IsTokenExpired(sourceClient))
        {
            _logger.LogWarning("InitTransfer denied: Source client {SourceClientId} token has expired", sourceClient.ClientId);
            throw new HubException("Source client token has expired. Please reconnect with a fresh token.");
        }

        if (IsTokenExpired(targetClient))
        {
            _logger.LogWarning("InitTransfer denied: Target client {TargetClientId} token has expired", targetClient.ClientId);
            throw new HubException("Target client token has expired.");
        }

        if (_flowControlService == null) return;

        if (sourceClient.Groups.Count == 0 || targetClient.Groups.Count == 0)
        {
            _logger.LogWarning("InitTransfer failed: Client groups not found. Source={SourceGroups}, Target={TargetGroups}",
                string.Join(", ", sourceClient.Groups), string.Join(", ", targetClient.Groups));
            throw new HubException("Client groups not found.");
        }

        if (!_flowControlService.CanSendTo(sourceClient.Groups, targetClient.Groups))
        {
            _logger.LogWarning("InitTransfer denied: Groups [{SourceGroups}] are not authorized to send to groups [{TargetGroups}]",
                string.Join(", ", sourceClient.Groups), string.Join(", ", targetClient.Groups));
            throw new HubException("Transfer not authorized.");
        }

        _logger.LogInformation("Flow control authorized: [{SourceGroups}] -> [{TargetGroups}]",
            string.Join(", ", sourceClient.Groups), string.Join(", ", targetClient.Groups));
    }

    /// <summary>
    /// Readies the receiver, then the sender, and returns the transfer id once both have accepted.
    /// </summary>
    /// <param name="senderConnectionId">
    /// The connection the sender is invoked on. A sender asking for the transfer is invoked on the
    /// connection it asked from, which a stale registration could otherwise contradict.
    /// </param>
    /// <param name="senderMethod">
    /// <see cref="ICommandClient.GetReadyToSend"/> when the sender asked for the transfer and picks
    /// its session up by id afterwards; <see cref="ICommandClient.RequestSend"/> when it did not.
    /// </param>
    internal async Task<string> HandshakeAsync(
        ClientRegistration sender,
        string senderConnectionId,
        ClientRegistration receiver,
        int batchSize,
        int timeoutMs,
        string senderMethod,
        CancellationToken cancellationToken = default)
    {
        var transferId = Guid.NewGuid().ToString();

        _logger.LogInformation("Orchestrating Transfer: {TransferId}. Source: {SourceId} -> Target: {TargetId} (BatchSize={Size}, via {SenderMethod})",
            transferId, sender.ClientId, receiver.ClientId, batchSize, senderMethod);

        await _transferManager.RegisterParticipantsAsync(transferId, sender.ClientId, receiver.ClientId);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(timeoutMs);
        try
        {
            bool receiverReady;
            try
            {
                receiverReady = await _hubContext.Clients.Client(receiver.ConnectionId)
                    .InvokeAsync<bool>(nameof(ICommandClient.GetReadyToReceive), transferId, sender.ClientId, cts.Token);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // The usual cause is a registration that outlived its process: the client is still
                // in the state store, but SignalR has no such connection any more. Left untranslated
                // this escapes as a non-HubException and reaches the caller as "An unexpected error
                // occurred invoking 'InitTransfer'", which says nothing and costs an hour to chase.
                // Detail stays in the hub log; the client gets something it can act on.
                _logger.LogWarning(ex, "Receiver {TargetClientId} unreachable on connection {ConnectionId} for {TransferId}. Registration is likely stale.",
                    receiver.ClientId, receiver.ConnectionId, transferId);
                await _transferManager.UnregisterParticipantsAsync(transferId);
                throw new HubException($"Target client {receiver.ClientId} is not reachable.");
            }

            if (!receiverReady)
            {
                _logger.LogWarning("Receiver {TargetClientId} refused {TransferId}.", receiver.ClientId, transferId);
                await _transferManager.UnregisterParticipantsAsync(transferId);
                throw new HubException($"Target client {receiver.ClientId} refused the transfer.");
            }

            bool senderReady;
            try
            {
                senderReady = await _hubContext.Clients.Client(senderConnectionId)
                    .InvokeAsync<bool>(senderMethod, transferId, batchSize, receiver.ClientId, cts.Token);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(ex, "Sender failed to ready for {TransferId}. Cancelling Receiver.", transferId);
                await AbandonAsync(transferId, receiver);
                throw new HubException("Source client failed to prepare for the transfer.");
            }

            if (!senderReady)
            {
                _logger.LogWarning("Sender {SourceClientId} refused {TransferId} ({SenderMethod}). Cancelling Receiver.",
                    sender.ClientId, transferId, senderMethod);
                await AbandonAsync(transferId, receiver);
                throw new HubException("Source client refused the transfer.");
            }
        }
        catch (OperationCanceledException)
        {
            await AbandonAsync(transferId, receiver);
            if (cancellationToken.IsCancellationRequested) throw;

            _logger.LogWarning("Handshake timed out for Transfer: {TransferId}", transferId);
            throw new HubException($"Handshake timed out after {timeoutMs}ms");
        }

        return transferId;
    }

    private async Task AbandonAsync(string transferId, ClientRegistration receiver)
    {
        await _transferManager.UnregisterParticipantsAsync(transferId);
        await _hubContext.Clients.Client(receiver.ConnectionId).SendAsync(nameof(ICommandClient.CancelTransfer), transferId);
    }

    private static bool IsTokenExpired(ClientRegistration client)
    {
        return client.TokenExpiry.HasValue && DateTime.UtcNow > client.TokenExpiry.Value;
    }
}
