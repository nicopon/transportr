namespace TransportR.Client.Internal;

/// <summary>
/// Links a caller <see cref="CancellationToken"/> with a <see cref="TimeProvider"/>-backed timeout.
/// </summary>
internal readonly struct LinkedTimeout : IDisposable
{
    private readonly CancellationTokenSource _timer;
    private readonly CancellationTokenSource _linked;

    public LinkedTimeout(TimeSpan timeout, TimeProvider timeProvider, CancellationToken cancellationToken)
    {
        _timer = new CancellationTokenSource(timeout, timeProvider);
        _linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _timer.Token);
    }

    public CancellationToken Token => _linked.Token;

    public bool TimedOut => _timer.IsCancellationRequested;

    public void Dispose()
    {
        _linked.Dispose();
        _timer.Dispose();
    }
}
