namespace TransportR.Storage.Redis;

/// <summary>
/// Options for <see cref="RedisStateStore"/>.
/// </summary>
public class RedisStateStoreOptions
{
    /// <summary>
    /// Prefix prepended to all Redis keys managed by <see cref="RedisStateStore"/>.
    /// Defaults to <c>"transportr:"</c>. Must not be <c>null</c>; an empty string stores keys without a prefix.
    /// </summary>
    public string KeyPrefix { get; set; } = "transportr:";
}
