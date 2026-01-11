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
    public void Release()
    {
        bool shouldDispose = false;
        lock (_lock)
        {
            _refCount--;
            if (_refCount <= 0 && _payload != null)
            {
                shouldDispose = true;
            }
        }

        if (shouldDispose)
        {
            // Dispose outside lock to be safe
            _payload?.Dispose();
            _payload = null;
        }
    }
}
