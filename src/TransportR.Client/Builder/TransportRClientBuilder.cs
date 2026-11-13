using Microsoft.Extensions.Logging;
using TransportR.Client.Configuration;
using TransportR.Client.Internal;
using TransportR.Client.Services;
using TransportR.Interfaces;
using TransportR.Models;

namespace TransportR.Client;

/// <summary>
/// A fluent builder for configuring and creating a <see cref="TransportRClient{T}"/> carrying
/// items of type <typeparamref name="T"/>.
/// </summary>
/// <remarks>
/// The item type is declared here rather than at <see cref="Build"/> so that the serializer and
/// the client agree at compile time: a serializer that cannot carry <typeparamref name="T"/>
/// — <c>WithArrowSerialization</c> on anything but <c>RecordBatch</c> — is not an overload that
/// exists, so the mismatch is a compile error rather than a throw on the first batch.
/// </remarks>
/// <typeparam name="T">The type of items transfers built by this builder will carry.</typeparam>
public class TransportRClientBuilder<T>
{
    private readonly ClientBuilderState _state = new();
    private int _batchSize = 1000;
    private IBatchSerializer<T>? _serializer;
    private Func<int, IBatchSerializer<T>>? _serializerFactory;

    /// <summary>
    /// Sets the base URL of the TransportR Hub.
    /// </summary>
    public TransportRClientBuilder<T> WithUrl(string url)
    {
        _state.SetUrl(url);
        return this;
    }

    /// <summary>
    /// Sets the default batch size for transfers.
    /// </summary>
    public TransportRClientBuilder<T> WithBatchSize(int batchSize)
    {
        _batchSize = batchSize;
        return this;
    }

    /// <summary>
    /// Sets the batch accumulation timeout.
    /// </summary>
    public TransportRClientBuilder<T> WithBatchTimeout(int timeoutMs)
    {
        _state.SetBatchTimeout(timeoutMs);
        return this;
    }

    /// <summary>
    /// Bounds how many batches may be in flight — flushed but not yet acknowledged — before the
    /// producer is made to wait. Unbounded by default.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the only backpressure path from the receiver back to the producer. Left unset, a
    /// slow receiver makes the sender hold the whole source in memory: TCP throttles the HTTP
    /// write, but nothing upstream of it. The window counts batches, so it bounds roughly
    /// <c>max × batch size</c> — see "Backpressure" in <c>ResilienceArchitecture.md</c> for sizing.
    /// </para>
    /// <para>
    /// On high-latency or WAN links, size the in-flight window to at least the bandwidth-delay product
    /// (<c>RTT × bandwidth</c>) to avoid starving the sender while waiting for acknowledgements across
    /// round-trips.
    /// </para>
    /// </remarks>
    public TransportRClientBuilder<T> WithMaxInFlightBatches(int max)
    {
        _state.SetMaxInFlightBatches(max);
        return this;
    }

    /// <summary>
    /// Bounds what the receiver holds for a consumer that is behind. Unbounded by default — the
    /// counterpart of <see cref="WithMaxInFlightBatches"/> at the other end of the transfer.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Pass a count, a wire-byte budget, or both; the first one reached is the one that binds,
    /// so an Arrow consumer is held by the bytes and a record consumer by the count without
    /// either having to disable the other. At least one is required — calling this to bound
    /// nothing is a contradiction, and not calling it is how you say that.
    /// </para>
    /// <para>
    /// Raise the byte figure for throughput on a fast consumer, lower it to cap the receiver's
    /// footprint. See <see cref="BufferCapacity"/> for what a wire byte does and does not
    /// measure.
    /// </para>
    /// </remarks>
    /// <param name="maxItems">Maximum buffered items, or <c>null</c> not to bound the count.</param>
    /// <param name="maxBytes">Maximum buffered wire bytes, or <c>null</c> not to bound the size.</param>
    public TransportRClientBuilder<T> WithReceiveCapacity(int? maxItems = null, long? maxBytes = null)
        => WithReceiveCapacity(BufferCapacity.Of(maxItems, maxBytes));

    /// <inheritdoc cref="WithReceiveCapacity(int?, long?)"/>
    public TransportRClientBuilder<T> WithReceiveCapacity(BufferCapacity capacity)
    {
        _state.SetReceiveCapacity(capacity);
        return this;
    }

    /// <summary>
    /// Bounds the size of one batch on the wire, in bytes. Default: 256 MiB.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A receiver refuses a chunk that announces a larger payload before it reserves any memory for
    /// it: the transfer fails as a protocol violation, and the error names this setting. A sender
    /// refuses a larger batch before it is sent: an item client fails the transfer on a serialized
    /// batch above its own bound, and a byte client's <c>Build</c> refuses a batch size above it.
    /// </para>
    /// <para>
    /// Both ends of a transfer need a bound at least as large as the largest batch the sender cuts.
    /// The bound can be raised, not removed.
    /// </para>
    /// <para>
    /// It also bounds what a compressed batch may expand to on receipt: the serializers of the
    /// <c>With*Serialization</c> extensions refuse a batch that decompresses to more, without
    /// decompressing the rest, and the transfer fails as a protocol violation.
    /// </para>
    /// </remarks>
    /// <param name="maxBytes">The largest batch payload accepted, in bytes.</param>
    public TransportRClientBuilder<T> WithMaxBatchBytes(int maxBytes)
    {
        _state.SetMaxBatchBytes(maxBytes);
        return this;
    }

    /// <summary>
    /// Sets the logger factory.
    /// </summary>
    public TransportRClientBuilder<T> WithLoggerFactory(ILoggerFactory loggerFactory)
    {
        _state.SetLoggerFactory(loggerFactory);
        return this;
    }

    /// <summary>
    /// Sets the batch serializer.
    /// </summary>
    /// <remarks>
    /// A serializer built by the caller keeps its own bounds: the size a compressed batch may expand
    /// to is not the figure given to <see cref="WithMaxBatchBytes"/> but the 256 MiB default of the
    /// serialization packages' own constructors. The <c>With*Serialization</c> extensions build their
    /// serializer with the figure of <see cref="WithMaxBatchBytes"/>.
    /// </remarks>
    public TransportRClientBuilder<T> WithSerializer(IBatchSerializer<T> serializer)
    {
        _serializer = serializer;
        _serializerFactory = null;
        return this;
    }

    /// <summary>
    /// Sets a serializer built at <see cref="Build"/> from the bound of <see cref="WithMaxBatchBytes"/>.
    /// </summary>
    internal TransportRClientBuilder<T> WithSerializerFactory(Func<int, IBatchSerializer<T>> factory)
    {
        _serializerFactory = factory;
        _serializer = null;
        return this;
    }

    internal IBatchSerializer<T> ResolveSerializer()
    {
        // There is no default: picking a serializer for the caller would pick their wire format, and
        // the serialization packages are separate on purpose.
        if (_serializerFactory is { } factory) return factory(_state.MaxBatchBytes);
        return _serializer ?? throw new InvalidOperationException("Serializer must be configured using WithSerializer().");
    }

    /// <summary>
    /// Sets the authentication token (static).
    /// </summary>
    public TransportRClientBuilder<T> WithAccessToken(string token)
    {
        _state.SetAccessToken(token);
        return this;
    }

    /// <summary>
    /// Sets the authentication token provider (dynamic).
    /// </summary>
    public TransportRClientBuilder<T> WithTokenProvider(Func<Task<string>> provider)
    {
        _state.SetTokenProvider(provider);
        return this;
    }

    /// <summary>
    /// How long before a token expires the client asks its provider for a new one and presents it to
    /// the hub. Default: 5 minutes, and never more than half of what is left of the token, so a token
    /// shorter than two thresholds is renewed once rather than continuously.
    /// </summary>
    /// <remarks>
    /// The hub closes a connection whose token it has not been given a new one for, so this is how
    /// long the identity provider may be out of reach before connections start to close. It also
    /// governs when a transport asks the provider for a token as it connects. A provider that caches
    /// (<c>WithClientCredentials</c> does, until its own <c>RefreshThreshold</c>) returns the same token
    /// until then, and the renewal retries until it does not.
    /// </remarks>
    public TransportRClientBuilder<T> WithTokenRefreshThreshold(TimeSpan threshold)
    {
        _state.SetTokenRefreshThreshold(threshold);
        return this;
    }

    /// <summary>
    /// Chooses what may carry this client's write streams. By default a WebSocket where the path to the
    /// hub carries one, segments otherwise.
    /// </summary>
    /// <remarks>
    /// The transport a transfer ended up on, and why it fell back, are on
    /// <see cref="TransferInfo"/> and in the client's log at each opening of the stream.
    /// </remarks>
    public TransportRClientBuilder<T> WithWriteTransport(WriteTransportPolicy policy)
    {
        _state.SetWriteTransport(policy);
        return this;
    }

    /// <summary>
    /// Builds the <see cref="TransportRClient{T}"/>.
    /// </summary>
    public TransportRClient<T> Build()
    {
        _state.RequireUrl();

        var logger = _state.CreateLogger<TransportRClient<T>>();
        var serializer = ResolveSerializer();

        var wiring = _state.Assemble(logger);

        return new TransportRClient<T>(
            wiring.Channel,
            new SessionRegistry<TransferSession<T>>(logger),
            serializer,
            logger,
            _batchSize,
            wiring.Options,
            wiring.DataUrl,
            wiring.HttpClient,
            wiring.Streams
        );
    }
}
