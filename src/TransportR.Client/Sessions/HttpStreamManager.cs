using System.Net;
using System.Net.Http.Headers;
using Microsoft.Extensions.Logging;
using TransportR.Client.Internal;
using TransportR.Protocol;
using TransportR.Interfaces;

namespace TransportR.Client.Sessions;

/// <summary>
/// How a stream attempt ended, which is what the session picks its next state on.
/// </summary>
internal enum StreamOutcome
{
    /// <summary>The request ran to its end, or the read side stopped reading.</summary>
    Ended,

    /// <summary>The attempt was cancelled, by the session or by its other stream failing.</summary>
    Cancelled,

    /// <summary>The hub does not know the transfer (404 or 410).</summary>
    Gone,

    /// <summary>The connection failed, and a new attempt may get past it.</summary>
    Retryable,

    /// <summary>A new attempt would fail the same way.</summary>
    Fatal,
}

/// <summary>
/// Manages the underlying HTTP streams for a data session.
/// Handles connection establishment and the single token-refresh retry; a stream that fails
/// for any other reason is handed back to the session, which decides whether to reconnect.
/// </summary>
internal class HttpStreamManager : IAsyncDisposable
{
    private readonly IStreamTransport _transport;
    private readonly ISessionConfig _config;
    private readonly string _transferId;
    private readonly string _dataUrl;
    private readonly string? _affinity;
    
    public readonly record struct StreamResult(StreamOutcome Outcome, string? ErrorMessage = null)
    {
        public bool IsSuccess => Outcome is StreamOutcome.Ended or StreamOutcome.Cancelled;
    }

    private CancellationTokenSource _streamCts;
    private Task<StreamResult>? _senderTask;
    private Task<StreamResult>? _receiverTask;
    
    // No more Events - passive component
    public Task<StreamResult>? SenderTask => _senderTask;
    public Task<StreamResult>? ReceiverTask => _receiverTask;

    /// <summary>
    /// The current attempt's task for whichever direction this side writes on (Data for a sender,
    /// Control for a receiver). Unlike <see cref="SenderTask"/>/<see cref="ReceiverTask"/>, which
    /// only resolve once <b>both</b> directions finish (or one fails), this resolves as soon as the
    /// write side's own HTTP request completes - which a caller waiting to know a specific message
    /// actually reached the server needs, since the read side has no reason to end on its own.
    /// </summary>
    public Task<StreamResult>? WriteTask { get; private set; }

    /// <summary>
    /// The current attempt's task for whichever direction this side reads on (Control for a sender,
    /// Data for a receiver), which resolves when the read loop returns. A receiver in its closing
    /// phase reads its data GET to the end, so for it this is the hub ending that request.
    /// </summary>
    public Task<StreamResult>? ReadTask { get; private set; }

    /// <param name="affinity">The cookies of the hub instance holding the transfer's bridge, on every stream.</param>
    public HttpStreamManager(IStreamTransport transport, ISessionConfig config, string transferId, string dataUrl, string? affinity)
    {
        _transport = transport;
        _config = config;
        _transferId = transferId;
        _dataUrl = dataUrl;
        _affinity = affinity;
        _streamCts = new CancellationTokenSource();
    }

    public void StartStreams(bool isSender, Func<Stream, CancellationToken, Task> writeAction, Func<Stream, CancellationToken, Task> readAction, CancellationToken sessionToken)
    {
        // The attempt being replaced is cancelled but not disposed: it owns its source now and
        // still has streams unwinding that will cancel it on their way out.
        _streamCts.Cancel();
        var attempt = CancellationTokenSource.CreateLinkedTokenSource(sessionToken);
        _streamCts = attempt;

        if (isSender)
        {
             // Sender: Data=Write, Control=Read
             _senderTask = RunDualStreamAsync(true, writeAction, readAction, attempt);
             _receiverTask = null;
        }
        else
        {
             // Receiver: Data=Read, Control=Write
             _receiverTask = RunDualStreamAsync(false, writeAction, readAction, attempt);
             _senderTask = null;
        }
    }
    
    /// <summary>
    /// Runs one attempt's pair of streams. The attempt owns <paramref name="attempt"/> for its
    /// whole life, including after the manager has moved on to the next one.
    /// </summary>
    private async Task<StreamResult> RunDualStreamAsync(bool isSender, Func<Stream, CancellationToken, Task> writeAction, Func<Stream, CancellationToken, Task> readAction, CancellationTokenSource attempt)
    {
         var token = attempt.Token;
         // Trigger: Data Stream
         // Trigger: Control Stream
         
         Task<StreamResult> tData;
         Task<StreamResult> tControl;
         
         if (isSender)
         {
             // Sender writes Data, reads Control
             tData = RunHttpStreamAsync(ConnectionType.Data, isWrite: true, isRead: false, writeAction, readAction, token);
             tControl = RunHttpStreamAsync(ConnectionType.Control, isWrite: false, isRead: true, writeAction, readAction, token);
             WriteTask = tData;
             ReadTask = tControl;
         }
         else
         {
             // Receiver reads Data, writes Control
             tData = RunHttpStreamAsync(ConnectionType.Data, isWrite: false, isRead: true, writeAction, readAction, token);
             tControl = RunHttpStreamAsync(ConnectionType.Control, isWrite: true, isRead: false, writeAction, readAction, token);
             WriteTask = tControl;
             ReadTask = tData;
         }

         var tasks = new List<Task<StreamResult>> { tData, tControl };

         while (tasks.Count > 0)
         {
             var completedTask = await Task.WhenAny(tasks);
             tasks.Remove(completedTask);
             
             var result = await completedTask;
             if (!result.IsSuccess)
             {
                 // Cancels this attempt's other stream, never whatever attempt the manager is on
                 // now: an attempt that has already been replaced is still unwinding, and reaching
                 // for the shared field made it cancel the streams that succeeded it — every
                 // reopened stream then died within a fraction of a millisecond.
                 attempt.Cancel();
                 return result;
             }
         }
         
         return new StreamResult(StreamOutcome.Ended);
    }

    public void CancelStreams()
    {
        try { _streamCts.Cancel(); } catch (ObjectDisposedException) { }
    }

    public async ValueTask DisposeAsync()
    {
        CancelStreams();
        if (_senderTask != null) try { await _senderTask; } catch { }
        if (_receiverTask != null) try { await _receiverTask; } catch { }
        _streamCts.Dispose();
    }

    private async Task<StreamResult> RunHttpStreamAsync(
        ConnectionType type, 
        bool isWrite, 
        bool isRead,
        Func<Stream, CancellationToken, Task> writeAction, 
        Func<Stream, CancellationToken, Task> readAction,
        CancellationToken token)
    {
        bool forceRefresh = false;
        
        while (!token.IsCancellationRequested)
        {
            try
            {
                var request = CreateStreamRequest(type, isWrite, writeAction, token);
                
                // Add Authorization Header
                var tokenStr = await _transport.GetAccessTokenAsync(forceRefresh);
                if (!string.IsNullOrEmpty(tokenStr))
                {
                    request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", tokenStr);
                }

                using var response = await _transport.HttpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token);
                
                // 1. Handle Gone/NotFound (Fatal)
                if (response.StatusCode == HttpStatusCode.Gone || response.StatusCode == HttpStatusCode.NotFound)
                {
                    return new StreamResult(StreamOutcome.Gone, $"Transfer Gone ({response.StatusCode})");
                }
                
                // 2. Handle Unauthorized (401) - Token expired
                if (response.StatusCode == HttpStatusCode.Unauthorized)
                {
                    if (!forceRefresh)
                    {
                        // Retry once with forced refresh
                        _config.Logger.LogWarning("Stream Unauthorized (401). Refreshing token and retrying...");
                        forceRefresh = true;
                        continue; 
                    }
                    else
                    {
                         // Second failure -> Retryable (force Session reconnect)
                         return new StreamResult(StreamOutcome.Retryable, "Unauthorized (401) - Check credentials");
                    }
                }

                response.EnsureSuccessStatusCode();

                // 3. Success Execution
                if (isRead)
                {
                    using var stream = await response.Content.ReadAsStreamAsync(token);
                    await readAction(stream, token);
                }
                
                // If Write, the request.Content callback has finished
                return new StreamResult(StreamOutcome.Ended);
            }
            catch (OperationCanceledException)
            {
                 return new StreamResult(StreamOutcome.Cancelled);
            }
            catch (WireProtocolViolationException ex)
            {
                 // The peer sent something this version cannot interpret; reconnecting would only
                 // repeat the violation, so this is fatal rather than retryable.
                 _config.Logger.LogError("Protocol violation: {Message}. Session failed.", ex.Message);
                 return new StreamResult(StreamOutcome.Fatal, ex.Message);
            }
            catch (Exception ex)
            {
                 // Network errors allow retry
                 _config.Logger.LogWarning("Stream failed: {Message}. Returning Retryable.", ex.Message);
                 return new StreamResult(StreamOutcome.Retryable, ex.Message);
            }
            
            // Only a 401 loops here, to retry once with a refreshed token. Every other path
            // returns, including an exception: restarting a broken stream is DataSession's, so
            // that the decision to retry is taken where the session state is known.
        }
        
        return new StreamResult(StreamOutcome.Cancelled);
    }

    /// <summary>
    /// Asks the hub to end the transfer, on a request of its own. This is for an Abort that could not
    /// be written to the data stream: behind data nobody reads the stream is full, and a request is
    /// the only thing that still gets through. Best effort and bounded by <paramref name="timeout"/>;
    /// never throws. A hub that predates the route answers 404, which is no reason to say anything.
    /// </summary>
    /// <returns>True when the hub accepted the request.</returns>
    public async Task<bool> RequestAbortAsync(TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, $"{_dataUrl}/{_transferId}/{WireRoutes.AbortSegment}");
            request.Version = HttpVersion.Version11;
            request.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
            AddAffinity(request);

            var token = await _transport.GetAccessTokenAsync(forceRefresh: false).WaitAsync(cts.Token);
            if (!string.IsNullOrEmpty(token))
            {
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            }

            using var response = await _transport.HttpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cts.Token);
            if (response.IsSuccessStatusCode)
            {
                _config.Logger.LogInformation("Asked the hub to end Transfer={TransferId}: the Abort could not be written to the data stream", _transferId);
                return true;
            }

            _config.Logger.LogDebug("The hub did not end Transfer={TransferId}: {Status}", _transferId, response.StatusCode);
        }
        catch (Exception ex)
        {
            _config.Logger.LogWarning("Could not ask the hub to end Transfer={TransferId} ({Message}); the receiver learns of it when the hub's bridge timeout expires", _transferId, ex.Message);
        }

        return false;
    }

    // Only the bridge's instance can carry the streams: one the client reached for another transfer
    // since, or for its own stream, is the wrong one.
    private void AddAffinity(HttpRequestMessage request)
    {
        if (!string.IsNullOrEmpty(_affinity))
        {
            request.Headers.TryAddWithoutValidation("Cookie", _affinity);
        }
    }

    private HttpRequestMessage CreateStreamRequest(ConnectionType type, bool isWrite, Func<Stream, CancellationToken, Task> writeAction, CancellationToken token)
    {
        string method = isWrite ? "send" : "receive";
        string url = $"{_dataUrl}/{_transferId}/{method}";
        
        var request = new HttpRequestMessage(isWrite ? HttpMethod.Post : HttpMethod.Get, url);
        request.Headers.Add("X-Connection-Type", type.ToString());

        // Stated rather than inherited: the write side below reads a response as the end of the
        // request body, which only HTTP/1.1 guarantees, and HTTP/2 caps an upload at its flow-control
        // window. See "Data plane transport" in TransportRArchitecture.md.
        request.Version = HttpVersion.Version11;
        request.VersionPolicy = HttpVersionPolicy.RequestVersionExact;

        AddAffinity(request);

        if (isWrite)
        {
            // Over HTTP/1.1 nothing reads the socket while a request body is being sent, so a
            // connection the hub closes is only noticed by the next write - which a sender with a
            // full window, or a receiver with nothing to acknowledge, may not make for minutes. A
            // 404 answered before the body is read is lost the same way: the request stays
            // pending. Expect makes the handler read the response alongside the body, and sends
            // the request headers at once, so the hub also learns of a stream whose body is still
            // empty.
            request.Headers.ExpectContinue = true;

            request.Content = new PushStreamContent(async (stream, _, _) =>
            {
                // Flushed but never disposed, and the stream underneath is never closed: HttpClient
                // owns the request body and writes its terminating chunk after this returns.
                // Closing it here made every POST that finished cleanly come back as a failed
                // request, which is why a receiver could never end its control POST on purpose.
                var bufferedStream = new BufferedStream(stream, bufferSize: _config.Options.StreamBufferSize);
                try
                {
                    await writeAction(bufferedStream, token);
                    await bufferedStream.FlushAsync(token);
                }
                catch (Exception ex)
                {
                    if (token.IsCancellationRequested)
                        _config.Logger.LogDebug("WriteLoop closed ({Type}) - Transfer={TransferId}", type, _transferId);
                    else
                        _config.Logger.LogWarning("WriteLoop interrupted ({Type}) - Transfer={TransferId}: {Message}", type, _transferId, ex.Message);
                    throw;
                }
            });
        }
        
        return request;
    }

}
