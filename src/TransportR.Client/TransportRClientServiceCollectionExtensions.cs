using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace TransportR.Client;

/// <summary>
/// Dependency injection registration extensions for TransportR clients.
/// </summary>
public static class TransportRClientServiceCollectionExtensions
{
    /// <inheritdoc cref="AddTransportRClient{T}(IServiceCollection, Action{IServiceProvider, TransportRClientBuilder{T}})"/>
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
    /// <see cref="IServiceProvider"/>.
    /// </summary>
    /// <remarks>
    /// Automatically wires <see cref="ILoggerFactory"/> and <see cref="TimeProvider"/> from the container
    /// when present before invoking <paramref name="configure"/>, so calls inside
    /// <paramref name="configure"/> win. Uses <c>TryAddSingleton</c> (the first registration for a given
    /// <typeparamref name="T"/> wins). The container disposes the client when it shuts down; the host
    /// connects and disconnects it (for instance from a hosted service).
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
            WireContainerServices(sp, b => builder.WithLoggerFactory(b), t => builder.WithTimeProvider(t));
            configure(sp, builder);
            return builder.Build();
        });

        services.TryAddSingleton<IDataSender<T>>(sp => sp.GetRequiredService<TransportRClient<T>>());
        services.TryAddSingleton<IDataReceiver<T>>(sp => sp.GetRequiredService<TransportRClient<T>>());
        return services;
    }

    /// <inheritdoc cref="AddByteStreamClient(IServiceCollection, int, Action{IServiceProvider, ByteStreamClientBuilder})"/>
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
    /// <remarks>
    /// Automatically wires <see cref="ILoggerFactory"/> and <see cref="TimeProvider"/> from the container
    /// when present before invoking <paramref name="configure"/>. Uses <c>TryAddSingleton</c> (the first
    /// registration wins). The container disposes the client when it shuts down.
    /// </remarks>
    public static IServiceCollection AddByteStreamClient(
        this IServiceCollection services,
        int batchBytes,
        Action<IServiceProvider, ByteStreamClientBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        ByteStreamClientBuilder.ValidateBatchBytes(batchBytes);

        services.TryAddSingleton(sp =>
        {
            var builder = new ByteStreamClientBuilder(batchBytes);
            WireContainerServices(sp, b => builder.WithLoggerFactory(b), t => builder.WithTimeProvider(t));
            configure(sp, builder);
            return builder.Build();
        });

        return services;
    }

    private static void WireContainerServices(
        IServiceProvider sp,
        Action<ILoggerFactory> setLogger,
        Action<TimeProvider> setTime)
    {
        if (sp.GetService<ILoggerFactory>() is { } loggerFactory)
        {
            setLogger(loggerFactory);
        }

        if (sp.GetService<TimeProvider>() is { } timeProvider)
        {
            setTime(timeProvider);
        }
    }
}
