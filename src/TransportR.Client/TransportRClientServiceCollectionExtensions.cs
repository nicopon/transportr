using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace TransportR.Client;

/// <summary>
/// Dependency injection registration extensions for TransportR clients.
/// </summary>
public static class TransportRClientServiceCollectionExtensions
{
    /// <summary>
    /// Registers a singleton <see cref="TransportRClient{T}"/> along with <see cref="IDataSender{T}"/>
    /// and <see cref="IDataReceiver{T}"/>.
    /// </summary>
    /// <remarks>
    /// Automatically wires <see cref="ILoggerFactory"/> and <see cref="TimeProvider"/> from the container
    /// when present before invoking <paramref name="configure"/>. The container disposes the client
    /// when it shuts down; the host connects and disconnects it (for instance from a hosted service).
    /// </remarks>
    public static IServiceCollection AddTransportRClient<T>(
        this IServiceCollection services,
        Action<TransportRClientBuilder<T>> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        return services.AddTransportRClient<T>((_, builder) => configure(builder));
    }

    /// <summary>
    /// Registers a singleton <see cref="TransportRClient{T}"/> along with <see cref="IDataSender{T}"/>
    /// and <see cref="IDataReceiver{T}"/>, giving <paramref name="configure"/> access to the
    /// <see cref="IServiceProvider"/> (for <c>IOptions&lt;T&gt;</c> or handler dependencies).
    /// </summary>
    /// <remarks>
    /// Automatically wires <see cref="ILoggerFactory"/> and <see cref="TimeProvider"/> from the container
    /// when present before invoking <paramref name="configure"/>, so calls inside
    /// <paramref name="configure"/> win.
    /// </remarks>
    public static IServiceCollection AddTransportRClient<T>(
        this IServiceCollection services,
        Action<IServiceProvider, TransportRClientBuilder<T>> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.TryAddSingleton(sp =>
        {
            var builder = new TransportRClientBuilder<T>();
            if (sp.GetService<ILoggerFactory>() is { } loggerFactory)
            {
                builder.WithLoggerFactory(loggerFactory);
            }

            if (sp.GetService<TimeProvider>() is { } timeProvider)
            {
                builder.WithTimeProvider(timeProvider);
            }

            configure(sp, builder);
            return builder.Build();
        });

        services.TryAddSingleton<IDataSender<T>>(sp => sp.GetRequiredService<TransportRClient<T>>());
        services.TryAddSingleton<IDataReceiver<T>>(sp => sp.GetRequiredService<TransportRClient<T>>());
        return services;
    }

    /// <summary>
    /// Registers a singleton <see cref="ByteStreamClient"/>.
    /// </summary>
    /// <remarks>
    /// Automatically wires <see cref="ILoggerFactory"/> and <see cref="TimeProvider"/> from the container
    /// when present before invoking <paramref name="configure"/>. The container disposes the client
    /// when it shuts down.
    /// </remarks>
    public static IServiceCollection AddByteStreamClient(
        this IServiceCollection services,
        int batchBytes,
        Action<ByteStreamClientBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        return services.AddByteStreamClient(batchBytes, (_, builder) => configure(builder));
    }

    /// <summary>
    /// Registers a singleton <see cref="ByteStreamClient"/>, giving <paramref name="configure"/> access
    /// to the <see cref="IServiceProvider"/>.
    /// </summary>
    public static IServiceCollection AddByteStreamClient(
        this IServiceCollection services,
        int batchBytes,
        Action<IServiceProvider, ByteStreamClientBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        // Validate batchBytes eagerly so invalid bounds fail at registration time.
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(batchBytes, 0, nameof(batchBytes));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(batchBytes, int.MaxValue - sizeof(int), nameof(batchBytes));

        services.TryAddSingleton(sp =>
        {
            var builder = new ByteStreamClientBuilder(batchBytes);
            if (sp.GetService<ILoggerFactory>() is { } loggerFactory)
            {
                builder.WithLoggerFactory(loggerFactory);
            }

            if (sp.GetService<TimeProvider>() is { } timeProvider)
            {
                builder.WithTimeProvider(timeProvider);
            }

            configure(sp, builder);
            return builder.Build();
        });

        return services;
    }
}
