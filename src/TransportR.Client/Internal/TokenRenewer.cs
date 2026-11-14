using System.Net.Http.Headers;
using System.Net.Http.Json;
using Microsoft.Extensions.Logging;

namespace TransportR.Client.Internal;

/// <summary>
/// Keeps the hub holding a token that has not run out. Nothing sent over an open connection can
/// carry a token, so it goes to the hub as a request of its own, ahead of the expiry the hub
/// enforces: the hub closes a connection whose token it was not given a new one for.
/// </summary>
/// <remarks>
/// <para>
/// One attempt at a time, on its own clock: the renewal is armed from the expiry the hub recorded,
/// not from the presence heartbeat, so a short-lived token does not depend on how often that beats.
/// A failed attempt (no token from the provider, the same one again, a refusal, a hub out of reach)
/// leaves the old token in place and is tried again with a growing, jittered delay until the token
/// the hub holds has run out. After that the connection is the hub's to close, and the restart
/// that follows registers the client again and starts this over.
/// </para>
/// <para>
/// A token whose expiry the client cannot read (an encrypted or opaque one) is renewed all the
/// same: the hub answers with the expiry it recorded, and that is what the next attempt is armed on.
/// </para>
/// </remarks>
internal sealed class TokenRenewer : IAsyncDisposable
{
    private static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan FirstRetry = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxRetry = TimeSpan.FromSeconds(30);

    // Task.Delay and SemaphoreSlim cap a wait at about 24 days; a token lives longer than a wait
    // is worth trusting, so a long one is taken in pieces.
    private static readonly TimeSpan MaxWait = TimeSpan.FromHours(6);

    private readonly ControlChannel _tokens;
    private readonly string _url;
    private readonly TimeSpan _threshold;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _stop = new();
    private readonly SemaphoreSlim _kick = new(0);
    private readonly object _lock = new();
    private readonly List<PushRequest> _requests = [];
    private Task? _loop;
    private bool _stopped;

    // Touched by the loop only.
    private string? _pending;
    private string? _accepted;
    private DateTime? _heldExpiry;
    private int _unchanged;

    /// <param name="channel">
    /// Where tokens come from, and what the renewal goes through: it carries the client's own
    /// affinity, so that the instance holding the client's stream judges what the token changes.
    /// </param>
    public TokenRenewer(ControlChannel channel, TimeSpan threshold, ILogger logger)
    {
        _tokens = channel;
        _url = channel.UrlOf(WireRoutes.TokenSegment);
        _threshold = threshold;
        _logger = logger;
    }

    /// <summary>Starts renewing, beginning with the token the client holds now.</summary>
    public void Start()
    {
        lock (_lock)
        {
            if (!_stopped) _loop ??= Task.Run(() => RunAsync(_stop.Token));
        }
    }

    /// <summary>
    /// Presents the token held now, whatever was accepted before: the hub has just registered this
    /// client afresh, on a connection whose own token is older than this one.
    /// </summary>
    public void PushNow()
    {
        lock (_lock)
        {
            if (_stopped) return;
            _loop ??= Task.Run(() => RunAsync(_stop.Token));
            _kick.Release();
        }
    }

    /// <summary>
    /// Presents <paramref name="token"/> to the hub, whatever was accepted before, and returns once the
    /// hub has recorded it. A refusal, a timeout or a hub out of reach is raised here, not retried for
    /// the caller: the loop goes on to present the token the client holds, as it does after
    /// <see cref="PushNow"/>.
    /// </summary>
    /// <remarks>
    /// It goes through the loop, which runs one attempt at a time and is the only one to touch what
    /// the renewer remembers: a push of its own would have to be ordered against a renewal in flight.
    /// </remarks>
    public Task PushNowAsync(string token, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(token);

        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_stopped, this);
            _requests.Add(new PushRequest(token, done));
            _loop ??= Task.Run(() => RunAsync(_stop.Token));
            _kick.Release();
        }

        return done.Task.WaitAsync(cancellationToken);
    }

    /// <summary>
    /// How long before its expiry a token is renewed: <paramref name="threshold"/>, and no more than
    /// half of what is left of it, so that a token shorter than two thresholds is renewed once and
    /// not in a loop.
    /// </summary>
    internal static TimeSpan Advance(TimeSpan threshold, TimeSpan remaining) =>
        remaining <= TimeSpan.Zero ? TimeSpan.Zero : TimeSpan.FromTicks(Math.Min(threshold.Ticks, remaining.Ticks / 2));

    private async Task RunAsync(CancellationToken ct)
    {
        DateTime? due = DateTime.UtcNow;
        var pushCurrent = true;
        var failures = 0;

        while (!ct.IsCancellationRequested)
        {
            List<PushRequest> requested = [];
            try
            {
                if (await WaitUntilAsync(due, ct))
                {
                    pushCurrent = true;
                    failures = 0;
                    _pending = null;
                    requested = TakeRequests();
                }
                else if (due is { } at && DateTime.UtcNow < at)
                {
                    continue;
                }

                // Several callers at once are answered by the newest token.
                var expiry = await AttemptAsync(pushCurrent, requested.LastOrDefault()?.Token, ct);
                Settle(requested, expiry);
                if (expiry.Accepted)
                {
                    failures = 0;
                    pushCurrent = false;
                    due = NextDue(expiry.Expiry);
                }
                else
                {
                    due = RetryAt(++failures);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                foreach (var request in requested) request.Done.TrySetCanceled(ct);
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Token renewal failed unexpectedly");
                foreach (var request in requested) request.Done.TrySetException(ex);
                due = RetryAt(++failures);
            }
        }
    }

    private List<PushRequest> TakeRequests()
    {
        lock (_lock)
        {
            var taken = _requests.ToList();
            _requests.Clear();
            return taken;
        }
    }

    private static void Settle(List<PushRequest> requests, Attempt attempt)
    {
        foreach (var request in requests)
        {
            if (attempt.Accepted)
            {
                request.Done.TrySetResult();
            }
            else
            {
                request.Done.TrySetException(attempt.Error ?? new InvalidOperationException("The hub did not record the token."));
            }
        }
    }

    /// <returns>True when the wait was ended by <see cref="PushNow"/>, false when it ran out.</returns>
    private async Task<bool> WaitUntilAsync(DateTime? due, CancellationToken ct)
    {
        var wait = due is { } at ? at - DateTime.UtcNow : Timeout.InfiniteTimeSpan;
        if (wait != Timeout.InfiniteTimeSpan)
        {
            wait = TimeSpan.FromTicks(Math.Clamp(wait.Ticks, 0, MaxWait.Ticks));
        }

        var kicked = await _kick.WaitAsync(wait, ct);
        if (kicked)
        {
            while (_kick.CurrentCount > 0) _kick.Wait(0);
        }

        return kicked;
    }

    private readonly record struct Attempt(bool Accepted, DateTime? Expiry, Exception? Error = null);

    private sealed record PushRequest(string Token, TaskCompletionSource Done);

    private static Attempt Failed(Exception error) => new(false, null, error);

    private async Task<Attempt> AttemptAsync(bool pushCurrent, string? requested, CancellationToken ct)
    {
        var token = requested ?? _pending;

        if (token == null && pushCurrent)
        {
            token = await _tokens.GetAccessTokenAsync();
        }
        else if (token == null)
        {
            try
            {
                token = await _tokens.AcquireTokenAsync();
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(ex, "Token renewal: the token provider failed, keeping the current token");
                return Failed(ex);
            }

            // A provider that caches until its own threshold hands the same token back until then.
            if (token == _accepted)
            {
                if (++_unchanged > 1)
                {
                    _logger.LogWarning("Token renewal: the token provider keeps returning the token the hub already holds");
                }

                return Failed(new InvalidOperationException("The token provider returned the token the hub already holds."));
            }
        }

        if (string.IsNullOrEmpty(token))
        {
            return Failed(new InvalidOperationException("The token provider returned no token."));
        }

        _unchanged = 0;
        _pending = token;
        return await PushAsync(token, ct);
    }

    private async Task<Attempt> PushAsync(string token, CancellationToken ct)
    {
        var request = new HttpRequestMessage(HttpMethod.Put, _url);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(RequestTimeout);

        TokenRenewalResponse? body;
        try
        {
            using var response = await _tokens.SendAsync(request, affinity: null, timeout.Token);
            ControlChannel.EnsureFromHub(response);
            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning("Token renewal refused by the hub with {Status}. The old token stays in place.", (int)response.StatusCode);

                // A token past its own expiry cannot be accepted by anyone; asking the provider again is the way on.
                if (JwtHelper.ExtractExpiry(token) is { } own && own <= DateTime.UtcNow) _pending = null;
                return Failed(new HttpRequestException($"The hub refused the token ({(int)response.StatusCode}).", null, response.StatusCode));
            }

            body = await response.Content.ReadFromJsonAsync<TokenRenewalResponse>(timeout.Token);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            _logger.LogWarning("Token renewal timed out after {Seconds}s. The old token stays in place.", RequestTimeout.TotalSeconds);
            return Failed(new TimeoutException($"The hub did not answer within {RequestTimeout.TotalSeconds}s."));
        }
        catch (Exception ex) when (ex is HttpRequestException or System.Text.Json.JsonException or NotSupportedException)
        {
            _logger.LogWarning(ex, "Token renewal failed. The old token stays in place.");
            return Failed(ex);
        }

        if (body == null)
        {
            _logger.LogWarning("Token renewal: the hub answered without the expiry it recorded. The old token stays in place.");
            return Failed(new InvalidOperationException("The hub answered without the expiry it recorded."));
        }

        _accepted = token;
        _pending = null;
        _heldExpiry = body.TokenExpiry;
        return new Attempt(true, body.TokenExpiry);
    }

    private DateTime? NextDue(DateTime? expiry)
    {
        if (expiry is not { } at)
        {
            _logger.LogInformation("Token renewal: the hub enforces no expiry on this token, so none is armed");
            return null;
        }

        var advance = Advance(_threshold, at - DateTime.UtcNow);
        return at - advance;
    }

    private DateTime? RetryAt(int failures)
    {
        // Past the expiry the hub holds, there is nothing left to keep alive: the connection is the
        // hub's to close, and the restart that follows starts this over.
        var bound = _heldExpiry ?? JwtHelper.ExtractExpiry(_pending);
        if (bound is { } at && DateTime.UtcNow > at)
        {
            return null;
        }

        var delay = TimeSpan.FromTicks(Math.Min(FirstRetry.Ticks << Math.Min(failures - 1, 6), MaxRetry.Ticks));
        return DateTime.UtcNow + delay * (0.5 + Random.Shared.NextDouble() * 0.5);
    }

    public async ValueTask DisposeAsync()
    {
        // The client drops its reference after disposing, but a second call on the same instance
        // would Cancel() a source already disposed. A second caller does not wait for the first.
        lock (_lock)
        {
            if (_stopped) return;
            _stopped = true;
        }

        _stop.Cancel();

        if (_loop != null)
        {
            try { await _loop; } catch (OperationCanceledException) { }
        }

        foreach (var request in TakeRequests()) request.Done.TrySetCanceled();

        _stop.Dispose();
        _kick.Dispose();
    }
}
