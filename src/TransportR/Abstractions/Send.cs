namespace TransportR.Abstractions;

/// <summary>
/// Handle for sending items through a transfer.
/// </summary>
/// <typeparam name="T">The type of items to send.</typeparam>
public abstract class Send<T> : Transfer, IAsyncDisposable
{
    /// <summary>
    /// Creates a new send handle.
    /// </summary>
    /// <param name="transferId">The unique transfer identifier.</param>
    protected Send(string transferId) : base(transferId) { }

    /// <summary>
    /// Sends a single item through the transfer.
    /// </summary>
    /// <param name="item">The item to send.</param>
    /// <param name="cancellationToken">Token to cancel the send operation.</param>
    public abstract Task SendAsync(T item, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Sends multiple items through the transfer.
    /// </summary>
    /// <param name="items">The items to send.</param>
    /// <param name="cancellationToken">Token to cancel the send operation.</param>
    public virtual async Task SendAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)
    {
        foreach (var item in items)
        {
            await SendAsync(item, cancellationToken);
        }
    }

    /// <summary>
    /// Signals that all items have been sent and waits for acknowledgment.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the completion wait.</param>
    public abstract Task CompleteAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Disposes the send handle and releases resources.
    /// </summary>
    public ValueTask DisposeAsync() 
    {
        GC.SuppressFinalize(this);
        return DisposeAsyncCore();
    }
    
    /// <summary>
    /// Override to provide custom dispose logic.
    /// </summary>
    protected virtual ValueTask DisposeAsyncCore() => ValueTask.CompletedTask;
}
