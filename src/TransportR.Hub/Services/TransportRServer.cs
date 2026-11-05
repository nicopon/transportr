using TransportR.Interfaces;
using Microsoft.Extensions.Logging;
using TransportR.Hub.Streams;
using TransportR.Models;
using System.Buffers;

namespace TransportR.Hub.Services;

internal class TransportRServer : ITransportRServer
{
    private const string DestinationLostMessage = "The destination of the stream is no longer connected.";

    private readonly ILogger<TransportRServer> _logger;
    private readonly ITransferManager _transferManager;
    private readonly IHubProgressMonitor _progressMonitor;
    private readonly IBridgeManager _bridgeManager;
    private readonly ITransferRetirer _retirer;

    public TransportRServer(
        ILogger<TransportRServer> logger, 
        IHubProgressMonitor progressMonitor,
        ITransferManager transferManager,
        IBridgeManager bridgeManager,
        ITransferRetirer retirer)
    {
        _logger = logger;
        _progressMonitor = progressMonitor;
        _transferManager = transferManager;
        _bridgeManager = bridgeManager;
        _retirer = retirer;
    }

    public async Task HandleSendAsync(string transferId, ConnectionType type, Stream input, CancellationToken cancellationToken = default)
    {
        Guid connectionId = Guid.NewGuid();
        string bridgeKey = IBridgeManager.KeyFor(transferId, type);
        _logger.LogInformation("BRIDGE: Source Connecting {BridgeKey} (ID={ConnectionId})", bridgeKey, connectionId);
        
        var transfer = await _transferManager.TrackAsync(transferId).ConfigureAwait(false);
        if (transfer == null)
        {
            _logger.LogWarning("BRIDGE: Transfer {TransferId} not found in state store (expired or invalid). Rejecting connection.", transferId);
            throw new KeyNotFoundException($"Transfer {transferId} not found or expired.");
        }

        var bridge = _bridgeManager.GetOrCreate(transferId, type);

        if (bridge.State == BridgeState.Cancelled)
        {
            _logger.LogWarning("BRIDGE: Source attempted to connect to cancelled bridge {BridgeKey}", bridgeKey);
            throw new OperationCanceledException($"Transfer {transferId} was cancelled");
        }

        try
        {
            if (!await bridge.RegisterSenderAsync(input, connectionId, cancellationToken).ConfigureAwait(false))
            {
                throw new OperationCanceledException($"Transfer {transferId} was cancelled during connection");
            }

            // Taken before the stream: a reset landing between the two reads then cuts this source
            // instead of leaving it writing to a stream that nobody reads.
            var receiverToken = bridge.ReceiverToken;
            Stream receiverOutput;
            try
            {
                receiverOutput = await bridge.WaitForReceiverStream().WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // The destination that ended the registration's wait may have left again before this
                // got to it, which leaves a wait for the next one that only a cancelled bridge ends.
                bridge.OnDisconnected(connectionId);
                throw;
            }
            _logger.LogInformation("BRIDGE: Piping {BridgeKey}", bridgeKey);

            // Bytes on either stream show the transfer alive, whatever its control requests say.
            var inputToCopy = type == ConnectionType.Data
                ? new CountingStream(input, bytes =>
                {
                    KeepAlive(transferId);
                    _progressMonitor.OnBytesTransferred(bytes);
                })
                : new CountingStream(input, _ => KeepAlive(transferId));

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

                        // A clean end of the sender's data stream comes after the CompleteAck, or after an
                        // Abort that was written, so both ends are done with the transfer.
                        if (bridge.Complete())
                        {
                            await _retirer.RetireAsync(transferId).ConfigureAwait(false);
                        }
                    }
                }
                else if (endReason == StreamEndReason.Rupture)
                {
                    bridge.OnDisconnected(connectionId);

                    if (await HoldsAsync(transferId).ConfigureAwait(false))
                    {
                        _logger.LogWarning("BRIDGE: Sender POST ended with RUPTURE {BridgeKey}. Client should reconnect.", bridgeKey);
                    }
                    else
                    {
                        _logger.LogInformation("BRIDGE: Sender POST ended with RUPTURE {BridgeKey}: the hub no longer holds the transfer.", bridgeKey);
                    }

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
            
            _logger.LogInformation("BRIDGE: Completed normally {BridgeKey}", bridgeKey);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("BRIDGE: Source cancelled {BridgeKey}", bridgeKey);
            throw;
        }
        catch (Exception ex)
        {
            bridge.OnDisconnected(connectionId);

            if (await HoldsAsync(transferId).ConfigureAwait(false))
            {
                _logger.LogWarning("BRIDGE: Source Disconnected {BridgeKey} ({Message})", bridgeKey, ex.Message);
            }
            else
            {
                _logger.LogInformation("BRIDGE: Source Disconnected {BridgeKey} ({Message}): the hub no longer holds the transfer.", bridgeKey, ex.Message);
            }

            // The source is healthy and answers for nothing here, but a normal end would tell its
            // client the stream is over rather than that it has to reopen. A failing read never gets
            // this far (it is a Rupture above), so an IOException is the destination's.
            if (ex is IOException) throw;

            // Nothing tells the client: it learns of the drop from its own stream and reopens it.
        }
    }

    public async Task HandleReceiveAsync(string transferId, ConnectionType type, Stream output, CancellationToken cancellationToken = default)
    {
        Guid connectionId = Guid.NewGuid();
        string bridgeKey = IBridgeManager.KeyFor(transferId, type);
        _logger.LogInformation("BRIDGE: Sink Connecting {BridgeKey} (ID={ConnectionId})", bridgeKey, connectionId);

        // Admitted a moment ago, so a transfer missing now ended in between. The response ends empty,
        // and the client's next attempt meets the 404 that says so.
        if (await _transferManager.TrackAsync(transferId).ConfigureAwait(false) == null)
        {
            _logger.LogInformation("BRIDGE: Sink for transfer {TransferId} closed: the hub no longer holds the transfer.", transferId);
            return;
        }

        var bridge = _bridgeManager.GetOrCreate(transferId, type);

        if (bridge.State == BridgeState.Cancelled)
        {
            _logger.LogWarning("BRIDGE: Sink attempted to connect to cancelled bridge {BridgeKey}", bridgeKey);
            throw new OperationCanceledException($"Transfer {transferId} was cancelled");
        }

        try
        {
            if (!await bridge.RegisterReceiverAsync(output, connectionId, cancellationToken).ConfigureAwait(false))
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
        }
    }
    /// <summary>
    /// Whether the hub still holds the transfer. A stream that breaks on one it does not (finished,
    /// ended by the hub, expired) is that transfer's end and not a client to wait for: the hub removes
    /// the state before it tells anyone to cut, so a cut always finds it gone. Only the level of a log
    /// line depends on this, so a store that cannot answer leaves the louder one.
    /// </summary>
    private async Task<bool> HoldsAsync(string transferId)
    {
        try
        {
            return await _transferManager.GetParticipantsAsync(transferId).ConfigureAwait(false) != null;
        }
        catch (Exception)
        {
            return true;
        }
    }

    /// <summary>
    /// Refreshes the transfer's activity and renews its shared session, as a heartbeat does: a
    /// heartbeat is a request of its own, which can fail while the streams carry on. Called on every
    /// read, so it never waits on the store, and a store that fails costs a log line rather than the
    /// stream.
    /// </summary>
    private void KeepAlive(string transferId) => _ = KeepAliveAsync(transferId);

    private async Task KeepAliveAsync(string transferId)
    {
        try
        {
            await _transferManager.KeepAliveAsync(transferId).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning("BRIDGE: Could not renew the session of {TransferId}: {Message}", transferId, ex.Message);
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

