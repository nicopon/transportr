using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Logging;
using TransportR.Interfaces;
using TransportR.Protocol;

namespace TransportR.Client.SignalR.Sessions;

/// <summary>
/// Encapsulates logic for handling disconnection, probing connectivity via SignalR,
/// fetching checkpoints, and performing the reconnection handshake (Ping/Pong).
/// </summary>
internal class ReconnectionHandler<T> : IDisposable
{
    private readonly ISessionContext _context;
    private readonly string _transferId;
    // Pure service - no callback
    public ReconnectionHandler(ISessionContext context, string transferId)
    {
        _context = context;
        _transferId = transferId;
    }

    /// <summary>
    /// Triggers the reconnection flow: debounce, cancel streams, probe SignalR.
    /// </summary>
    /// <summary>
    /// Probes SignalR connectivity. Returns true if SignalR is back online and Hub responds to Ping.
    /// </summary>
    public async Task<bool> ProbeConnectivityAsync(CancellationToken token)
    {
        try
        {
            // 1. Wait for SignalR Connection
            int retry = 0;
            while (_context.HubConnection.State != HubConnectionState.Connected)
            {
                if (token.IsCancellationRequested || retry++ > 20) return false;
                await Task.Delay(500, token);
            }

            // 2. Ping Hub
            _signalRProbeTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            
            // Invoke Ping
            await _context.HubConnection.InvokeAsync(nameof(ICommandHub.Ping), _transferId, token);
            
            // Wait for Pong
            // We use a small timeout for the Pong itself
            using var timeoutCts = new CancellationTokenSource(5000);
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(token, timeoutCts.Token);
            
            await _signalRProbeTcs.Task.WaitAsync(linkedCts.Token);
            return true;
        }
        catch
        {
            return false;
        }
    }
    
    /// <summary>
    /// Called when SignalR Pong is received (confirming connectivity with Hub).
    /// </summary>
    public void HandleSignalRPong()
    {
        _signalRProbeTcs?.TrySetResult();
    }
    
    /// <summary>
    /// Called when Stream Pong is received (handshake on new stream).
    /// </summary>
    public void HandleStreamPong(int nextExpectedBatchId)
    {
        _pongNextExpectedBatchId = nextExpectedBatchId;
        _streamPongReceived?.TrySetResult();
    }

    // Handshake State (Set by DataSession before starting WriteLoop)
    private TaskCompletionSource? _streamPongReceived;
    private volatile int _pongNextExpectedBatchId;

    // Connectivity Probe State (SignalR-based)
    private TaskCompletionSource? _signalRProbeTcs;

    public void PrepareForHandshake()
    {
        _streamPongReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _pongNextExpectedBatchId = -1;
    }

    public async Task<int> ExecuteHandshakeAsync(Stream outputStream, Guid transferGuid, CancellationToken token)
    {
        try
        {
             await ChunkIO.WriteControlAsync(outputStream, transferGuid, 0, ChunkType.Ping, token);
             await outputStream.FlushAsync(token);
             
             if (_streamPongReceived == null) return -1;
             
             // Wait for Handshake Pong
             using var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
             cts.CancelAfter(_context.Options.PongTimeout);
             
             try
             {
                 await _streamPongReceived.Task.WaitAsync(cts.Token);
                 return _pongNextExpectedBatchId;
             }
             catch(OperationCanceledException)
             {
                 return -1; // Fail
             }
        }
        catch
        {
            throw;
        }
    }

    public void Dispose()
    {
        // No heavy resources to dispose, maybe CTS if stored?
        // _signalRProbeTcs etc are task sources, GC handles them.
    }
}
