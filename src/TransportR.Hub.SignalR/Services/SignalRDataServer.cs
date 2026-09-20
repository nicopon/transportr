using TransportR.Interfaces;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using TransportR.Hub.SignalR.Hubs;
using TransportR.Hub.SignalR.Streams;
using TransportR.Models;
using TransportR.Services;
using System.Buffers;

namespace TransportR.Hub.SignalR.Services;

public class SignalRDataServer : ITransportRServer
{
    private readonly ILogger<SignalRDataServer> _logger;
    private readonly IHubContext<CommandHub> _hubContext;
    private readonly ITransferManager _transferManager;
    private readonly IHubProgressMonitor _progressMonitor;
    private readonly IBridgeManager _bridgeManager;

    public SignalRDataServer(
        ILogger<SignalRDataServer> logger, 
        IHubProgressMonitor progressMonitor,
        IHubContext<CommandHub> hubContext,
        ITransferManager transferManager,
        IBridgeManager bridgeManager)
    {
        _logger = logger;
        _progressMonitor = progressMonitor;
        _hubContext = hubContext;
        _transferManager = transferManager;
        _bridgeManager = bridgeManager;
    }

    public async Task HandleSendAsync(string transferId, ConnectionType type, Stream input, CancellationToken cancellationToken = default)
    {
        Guid connectionId = Guid.NewGuid();
        string bridgeKey = IBridgeManager.KeyFor(transferId, type);
        _logger.LogInformation("BRIDGE: Source Connecting {BridgeKey} (ID={ConnectionId})", bridgeKey, connectionId);
        
        // HA Validation: Ensure transfer exists in distributed state
        var participants = await _transferManager.GetParticipantsAsync(transferId).ConfigureAwait(false);
        if (participants == null)
        {
            _logger.LogWarning("BRIDGE: Transfer {TransferId} not found in state store (expired or invalid). Rejecting connection.", transferId);
            throw new KeyNotFoundException($"Transfer {transferId} not found or expired.");
        }

        var bridge = _bridgeManager.GetOrCreate(bridgeKey);

        if (bridge.State == BridgeState.Cancelled)
        {
            _logger.LogWarning("BRIDGE: Source attempted to connect to cancelled bridge {BridgeKey}", bridgeKey);
            throw new OperationCanceledException($"Transfer {transferId} was cancelled");
        }

        try
        {
            if (!await bridge.RegisterSenderAsync(input, connectionId).ConfigureAwait(false))
            {
                throw new OperationCanceledException($"Transfer {transferId} was cancelled during connection");
            }

            var receiverOutput = await bridge.WaitForReceiverStream().ConfigureAwait(false);
            _logger.LogInformation("BRIDGE: Piping {BridgeKey}", bridgeKey);

            Stream inputToCopy = input;
            if (type == ConnectionType.Data)
            {
                inputToCopy = new CountingStream(input, bytes => _progressMonitor.OnBytesTransferred(bytes));
            }

            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, bridge.SenderToken);
            
            try 
            {
                _logger.LogInformation("BRIDGE: Starting CopyToAsync {BridgeKey}", bridgeKey);
                // Use custom copy loop with Flush to prevent buffering of small messages (ACKs)
                var endReason = await CopyAndFlushAsync(inputToCopy, receiverOutput, 256 * 1024, linkedCts.Token).ConfigureAwait(false);
                
                if (endReason == StreamEndReason.CleanEOF)
                {
                    _logger.LogInformation("BRIDGE: Sender POST ended with clean EOF {BridgeKey}", bridgeKey);
                    
                    // If this was the Data channel, advance to domino step 5
                    if (type == ConnectionType.Data)
                    {
                        _logger.LogInformation("BRIDGE: Data stream for {TransferId} finished cleanly. Signaling Bridge Completion (Step 5).", transferId);
                        bridge.Complete();
                    }
                }
                else if (endReason == StreamEndReason.Rupture)
                {
                    _logger.LogWarning("BRIDGE: Sender POST ended with RUPTURE {BridgeKey}. Client should reconnect.", bridgeKey);
                    bridge.OnDisconnected(connectionId);
                    return;
                }
                else // Cancelled
                {
                    _logger.LogInformation("BRIDGE: Sender POST was cancelled {BridgeKey}", bridgeKey);
                    return;
                }
            }
            catch (Exception copyEx)
            {
                _logger.LogWarning(copyEx, "BRIDGE: CopyToAsync failed/interrupted for {BridgeKey}", bridgeKey);
                throw;
            }
            
            // Do NOT remove the bridge here. We leave it active so that:
            // 1. Reconnecting clients can still find it (and see the completion signal).
            // 2. CommandHub.TransferComplete will explicitly clean it up when the lifecycle is fully done.

            _logger.LogInformation("BRIDGE: Completed normally {BridgeKey}", bridgeKey);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("BRIDGE: Source cancelled {BridgeKey}", bridgeKey);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning("BRIDGE: Source Disconnected {BridgeKey} ({Message})", bridgeKey, ex.Message);
            bridge.OnDisconnected(connectionId);
            
            // Note: We do NOT send StreamDisconnected notifications anymore.
            // Clients are expected to detect IOException and reconnect autonomously.
        }
    }

    public async Task HandleReceiveAsync(string transferId, ConnectionType type, Stream output, CancellationToken cancellationToken = default)
    {
        Guid connectionId = Guid.NewGuid();
        string bridgeKey = IBridgeManager.KeyFor(transferId, type);
        _logger.LogInformation("BRIDGE: Sink Connecting {BridgeKey} (ID={ConnectionId})", bridgeKey, connectionId);

        var bridge = _bridgeManager.GetOrCreate(bridgeKey);

        if (bridge.State == BridgeState.Cancelled)
        {
            _logger.LogWarning("BRIDGE: Sink attempted to connect to cancelled bridge {BridgeKey}", bridgeKey);
            throw new OperationCanceledException($"Transfer {transferId} was cancelled");
        }

        try
        {
            if (!await bridge.RegisterReceiverAsync(output, connectionId).ConfigureAwait(false))
            {
                throw new OperationCanceledException($"Transfer {transferId} was cancelled during connection");
            }
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, bridge.ReceiverToken);
            
            // Wait for completion signal (from Sender finishing) or cancellation
            var completed = await Task.WhenAny(bridge.CompletionTask, Task.Delay(-1, linkedCts.Token))
                                      .ConfigureAwait(false);
            
            if (completed == bridge.CompletionTask)
            {
                 _logger.LogInformation("BRIDGE: Sink Closing cleanly (Sender finished) {BridgeKey}", bridgeKey);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("BRIDGE: Sink cancelled {BridgeKey}", bridgeKey);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning("BRIDGE: Sink Disconnected {BridgeKey} ({Message})", bridgeKey, ex.Message);
            bridge.OnDisconnected(connectionId);
            
            // Note: We do NOT send StreamDisconnected notifications anymore.
        }
    }
    private async Task<StreamEndReason> CopyAndFlushAsync(Stream source, Stream destination, int bufferSize, CancellationToken token)
    {
        byte[] buffer = ArrayPool<byte>.Shared.Rent(bufferSize);
        try
        {
            int bytesRead;
            while ((bytesRead = await source.ReadAsync(new Memory<byte>(buffer), token).ConfigureAwait(false)) != 0)
            {
                await destination.WriteAsync(new ReadOnlyMemory<byte>(buffer, 0, bytesRead), token).ConfigureAwait(false);
                await destination.FlushAsync(token).ConfigureAwait(false);
            }
            // ReadAsync returned 0 = Clean EOF
            return StreamEndReason.CleanEOF;
        }
        catch (OperationCanceledException)
        {
            return StreamEndReason.Cancelled;
        }
        catch (IOException)
        {
            return StreamEndReason.Rupture;
        }
        catch (HttpRequestException)
        {
            return StreamEndReason.Rupture;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}

