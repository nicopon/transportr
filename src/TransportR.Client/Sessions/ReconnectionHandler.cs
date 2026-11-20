using TransportR.Client.Protocol;

namespace TransportR.Client.Sessions;

/// <summary>
/// Encapsulates logic for handling disconnection, probing the hub with a request,
/// and performing the reconnection handshake (Ping/Pong) on the new stream.
/// </summary>
internal class ReconnectionHandler : IDisposable
{
    // Short: the probe is a request, and a hub that answers does so well within it.
    private static readonly TimeSpan ProbeTimeout = TimeSpan.FromSeconds(5);

    private readonly ISessionContext _context;
    private readonly string _transferId;
    private readonly string? _affinity;

    public ReconnectionHandler(ISessionContext context, string transferId, string? affinity)
    {
        _context = context;
        _transferId = transferId;
        _affinity = affinity;
    }

    /// <summary>
    /// Returns true once the hub instance holding the transfer answers a request, which is when its
    /// streams are worth reopening.
    /// </summary>
    public async Task<bool> ProbeConnectivityAsync(CancellationToken token)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
        timeout.CancelAfter(ProbeTimeout);
        try
        {
            return await _context.ProbeAsync(_transferId, _affinity, timeout.Token);
        }
        catch (Exception) when (!token.IsCancellationRequested)
        {
            return false;
        }
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

    public void PrepareForHandshake()
    {
        _streamPongReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _pongNextExpectedBatchId = -1;
    }

    /// <summary>
    /// Writes the Ping at the head of a sender's data stream and, on a stream reopened after a cut, waits
    /// for the Pong. Returns the receiver's next expected batch, or -1 when no Pong is waited for or none
    /// came in time.
    /// </summary>
    /// <param name="pingPayload">The sender's share in an encrypted transfer; empty in clear.</param>
    public async Task<int> ExecuteHandshakeAsync(Stream outputStream, Guid transferGuid, ReadOnlyMemory<byte> pingPayload, CancellationToken token)
    {
        await ChunkIO.WriteControlAsync(outputStream, transferGuid, 0, ChunkType.Ping, pingPayload, token);
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
        catch (OperationCanceledException)
        {
            return -1; // Fail
        }
    }

    /// <summary>
    /// Lets a handshake that waits for a Pong go on without one: the session refused what came instead,
    /// and its write loop has an Abort to write.
    /// </summary>
    public void AbandonHandshake() => _streamPongReceived?.TrySetResult();

    public void Dispose()
    {
        // Nothing to release: the handshake state is a task completion source, which the collector
        // takes. Kept as IDisposable so a caller's using block does not change shape if that
        // stops being true.
    }
}
