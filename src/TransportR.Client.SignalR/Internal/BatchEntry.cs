using System.Buffers;

namespace TransportR.Client.SignalR.Internal;

/// <summary>
/// Thread-safe wrapper for unacked batch payload.
/// Uses reference counting to prevent disposal while being resent.
/// </summary>
internal class BatchEntry
{
    private IMemoryOwner<byte>? _payload;
    private readonly long _timestamp;
    private int _refCount = 1; // 1 reference from the Dictionary / Creator
    private readonly object _lock = new();

    public BatchEntry(IMemoryOwner<byte> payload, long timestamp)
    {
        _payload = payload;
        _timestamp = timestamp;
    }

    public long Timestamp => _timestamp;

    /// <summary>
    /// Tries to obtain a reference for usage (e.g. resending).
    /// Returns null if already disposed.
    /// Caller must call Release() when done.
    /// </summary>
    public IMemoryOwner<byte>? TryRetain()
    {
        lock (_lock)
        {
            if (_refCount <= 0 || _payload == null) return null;
            _refCount++;
            return _payload;
        }
    }

    /// <summary>
    /// Releases a reference. If count reaches 0, disposes the payload.
    /// </summary>
    /// <remarks>
    /// The field is cleared <b>inside</b> the lock and the payload disposed from the local copy
    /// outside it. Clearing it outside would let two releases that cross see a non-null field and
    /// both dispose: the payload comes from a pool, so a second disposal is a second
    /// <c>Return</c>, which hands one buffer to two renters and corrupts data elsewhere without
    /// throwing here.
    /// </remarks>
    public void Release()
    {
        IMemoryOwner<byte>? toDispose = null;
        lock (_lock)
        {
            _refCount--;
            if (_refCount <= 0)
            {
                toDispose = _payload;
                _payload = null;
            }
        }

        toDispose?.Dispose();
    }
}
