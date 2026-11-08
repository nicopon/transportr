using System.Buffers;

namespace TransportR.Client.Internal;

/// <summary>
/// The body of an unacknowledged batch, shared by reference count between the dictionary that keeps
/// it until its acknowledgement and every write of it in progress. The last release disposes what the
/// body lives in.
/// </summary>
internal class BatchEntry
{
    private IDisposable? _owner;
    private ReadOnlySequence<byte> _body;
    private int _refCount = 1; // 1 reference from the Dictionary / Creator
    private readonly object _lock = new();

    /// <param name="owner">What <paramref name="body"/> lives in. The entry owns it from here on.</param>
    public BatchEntry(IDisposable owner, ReadOnlySequence<byte> body)
    {
        _owner = owner;
        _body = body;
    }

    /// <summary>
    /// Tries to obtain a reference for usage (e.g. resending).
    /// Returns null if already disposed.
    /// Caller must call Release() when done.
    /// </summary>
    public ReadOnlySequence<byte>? TryRetain()
    {
        lock (_lock)
        {
            if (_refCount <= 0 || _owner == null) return null;
            _refCount++;
            return _body;
        }
    }

    /// <summary>
    /// Releases a reference. If count reaches 0, disposes the owner of the body.
    /// </summary>
    /// <remarks>
    /// The fields are cleared <b>inside</b> the lock and the owner disposed from the local copy
    /// outside it. Clearing them outside would let two releases that cross see a non-null field and
    /// both dispose: the body's memory comes from a pool, and a <c>RecyclableMemoryStream</c>'s own
    /// guard against a second disposal is not atomic, so a second disposal can be a second
    /// <c>Return</c>, which hands one buffer to two renters and corrupts data elsewhere without
    /// throwing here.
    /// </remarks>
    public void Release()
    {
        IDisposable? toDispose = null;
        lock (_lock)
        {
            _refCount--;
            if (_refCount <= 0)
            {
                toDispose = _owner;
                _owner = null;
                _body = default;
            }
        }

        toDispose?.Dispose();
    }
}
