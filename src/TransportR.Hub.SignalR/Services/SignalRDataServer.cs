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
    private const string DestinationLostMessage = "The destination of the stream is no longer connected.";

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

            // Taken before the stream: a reset landing between the two reads then cuts this source
            // instead of leaving it writing to a stream that nobody reads.
            var receiverToken = bridge.ReceiverToken;
            var receiverOutput = await bridge.WaitForReceiverStream().ConfigureAwait(false);
            _logger.LogInformation("BRIDGE: Piping {BridgeKey}", bridgeKey);

            Stream inputToCopy = input;
            if (type == ConnectionType.Data)
            {
                inputToCopy = new CountingStream(input, bytes => _progressMonitor.OnBytesTransferred(bytes));
            }

            // The bridge does not reach across to the source when its destination leaves; a source
            // parked on an input that stays silent learns of it only through this token.
            var senderToken = bridge.SenderToken;
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, senderToken, receiverToken);
            
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
                    // Told apart by exclusion: a write to a response that has gone can fail with a
                    // cancellation of its own before the destination's token has been cancelled.
                    if (cancellationToken.IsCancellationRequested || senderToken.IsCancellationRequested)
                    {
                        _logger.LogInformation("BRIDGE: Sender POST was cancelled {BridgeKey}", bridgeKey);

                        // Ends quietly, but the registration must not outlive the request: kept, the
                        // bridge stays Active on a source nobody is on and is never judged idle. A
                        // source that was replaced is no longer the current one, so this is a no-op.
                        bridge.OnDisconnected(connectionId);
                        return;
                    }

                    throw new IOException(DestinationLostMessage);
                }
            }
            // An IOException here is a lost destination, reported once by the handler below.
            catch (Exception copyEx) when (copyEx is not IOException)
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
            
            // The source is healthy and answers for nothing here, but a normal end would tell its
            // client the stream is over rather than that it has to reopen. A failing read never gets
            // this far (it is a Rupture above), so an IOException is the destination's.
            if (ex is IOException) throw;

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
            else
            {
                // WhenAny hands back the cancelled delay rather than throwing, so nothing below
                // releases the registration. Left in place, the bridge stays Active on a response
                // nobody reads and is never judged idle.
                _logger.LogInformation("BRIDGE: Sink cancelled {BridgeKey}", bridgeKey);
                bridge.OnDisconnected(connectionId);
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
        var writing = false;
        try
        {
            int bytesRead;
            while ((bytesRead = await source.ReadAsync(new Memory<byte>(buffer), token).ConfigureAwait(false)) != 0)
            {
                writing = true;
                await destination.WriteAsync(new ReadOnlyMemory<byte>(buffer, 0, bytesRead), token).ConfigureAwait(false);
                await destination.FlushAsync(token).ConfigureAwait(false);
                writing = false;
            }
            // ReadAsync returned 0 = Clean EOF
            return StreamEndReason.CleanEOF;
        }
        catch (OperationCanceledException)
        {
            return StreamEndReason.Cancelled;
        }
        // Thrown from a handler, so the Rupture handlers below do not see it. A Rupture is the source's
        // own connection failing; here the source is fine and the peer it writes to is not.
        catch (Exception ex) when (writing && (ex is IOException or ObjectDisposedException))
        {
            throw new IOException(DestinationLostMessage, ex);
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

