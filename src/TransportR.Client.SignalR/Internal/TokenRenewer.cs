using System.Net.Http.Headers;
using System.Net.Http.Json;
using Microsoft.Extensions.Logging;
using TransportR.Protocol;

namespace TransportR.Client.SignalR.Internal;

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

    private readonly HubConnectionManager _tokens;
    private readonly HttpClient _http;
    private readonly string _url;
    private readonly TimeSpan _threshold;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _stop = new();
    private readonly SemaphoreSlim _kick = new(0);
    private readonly object _startLock = new();
    private Task? _loop;
    private bool _stopped;

    // Touched by the loop only.
    private string? _pending;
    private string? _accepted;
    private DateTime? _heldExpiry;
    private int _unchanged;

    public TokenRenewer(HubConnectionManager tokens, HttpClient http, string clientUrl, Guid clientId, TimeSpan threshold, ILogger logger)
    {
        _tokens = tokens;
        _http = http;
        _url = $"{clientUrl.TrimEnd('/')}/{clientId}/token";
        _threshold = threshold;
        _logger = logger;
    }

    /// <summary>Starts renewing, beginning with the token the client holds now.</summary>
    public void Start()
    {
        lock (_startLock)
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
        lock (_startLock)
        {
            if (_stopped) return;
            _loop ??= Task.Run(() => RunAsync(_stop.Token));
            _kick.Release();
        }
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
            try
            {
                if (await WaitUntilAsync(due, ct))
                {
                    pushCurrent = true;
                    failures = 0;
                    _pending = null;
                }
                else if (due is { } at && DateTime.UtcNow < at)
                {
                    continue;
                }

                var expiry = await AttemptAsync(pushCurrent, ct);
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
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Token renewal failed unexpectedly");
                due = RetryAt(++failures);
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

    private readonly record struct Attempt(bool Accepted, DateTime? Expiry);

    private async Task<Attempt> AttemptAsync(bool pushCurrent, CancellationToken ct)
    {
        var token = _pending;

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
                return new Attempt(false, null);
            }

            // A provider that caches until its own threshold hands the same token back until then.
            if (token == _accepted)
            {
                if (++_unchanged > 1)
                {
                    _logger.LogWarning("Token renewal: the token provider keeps returning the token the hub already holds");
                }

                return new Attempt(false, null);
            }
        }

        if (string.IsNullOrEmpty(token))
        {
            return new Attempt(false, null);
        }

        _unchanged = 0;
        _pending = token;
        return await PushAsync(token, ct);
    }

    private async Task<Attempt> PushAsync(string token, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Put, _url);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(RequestTimeout);

        TokenRenewalResponse? body;
        try
        {
            using var response = await _http.SendAsync(request, timeout.Token);
            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning("Token renewal refused by the hub with {Status}. The old token stays in place.", (int)response.StatusCode);

                // A token past its own expiry cannot be accepted by anyone; asking the provider again is the way on.
                if (JwtHelper.ExtractExpiry(token) is { } own && own <= DateTime.UtcNow) _pending = null;
                return new Attempt(false, null);
            }

            body = await response.Content.ReadFromJsonAsync<TokenRenewalResponse>(timeout.Token);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            _logger.LogWarning("Token renewal timed out after {Seconds}s. The old token stays in place.", RequestTimeout.TotalSeconds);
            return new Attempt(false, null);
        }
        catch (Exception ex) when (ex is HttpRequestException or System.Text.Json.JsonException or NotSupportedException)
        {
            _logger.LogWarning(ex, "Token renewal failed. The old token stays in place.");
            return new Attempt(false, null);
        }

        if (body == null)
        {
            _logger.LogWarning("Token renewal: the hub answered without the expiry it recorded. The old token stays in place.");
            return new Attempt(false, null);
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
        lock (_startLock) _stopped = true;
        _stop.Cancel();

        if (_loop != null)
        {
            try { await _loop; } catch (OperationCanceledException) { }
        }

        _stop.Dispose();
        _kick.Dispose();
    }
}
