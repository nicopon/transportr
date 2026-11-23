using Microsoft.Extensions.Logging;
using TransportR.Client.Builder;
using TransportR.Client.Internal;

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
    /// Sets a stable client identifier instead of generating a new <see cref="Guid"/> on each build.
    /// </summary>
    /// <remarks>
    /// Useful when targeting a known receiver or looking up per-pair pre-shared keys in configuration.
    /// With JWT authentication, the hub refuses another subject trying to register under an active or
    /// grace-period client ID; in dev mode, no subject is attested and a second client using the same
    /// ID overwrites the first.
    /// </remarks>
    /// <exception cref="ArgumentException"><paramref name="clientId"/> is <see cref="Guid.Empty"/>.</exception>
    public TransportRClientBuilder<T> WithClientId(Guid clientId)
    {
        _state.SetClientId(clientId);
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
    /// Sets how long a partial batch waits before it is flushed.
    /// </summary>
    /// <param name="timeout">A whole number of milliseconds, at least one and at most <see cref="int.MaxValue"/>.</param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="timeout"/> is none of those. It is refused here, not rounded and not left for the
    /// first transfer to find.
    /// </exception>
    public TransportRClientBuilder<T> WithBatchTimeout(TimeSpan timeout)
    {
        _state.SetBatchTimeout(timeout);
        return this;
    }

    /// <summary>
    /// Sets how long a sender tolerates a receiver that acknowledges nothing before it fails the
    /// transfer. Default: 60 seconds.
    /// </summary>
    /// <remarks>
    /// <para>
    /// It bounds <c>CompleteAsync</c> and a producer parked on a full <see cref="WithMaxInFlightBatches"/>
    /// window. The clock counts silence: each acknowledgement restarts it, so a receiver that is slow but
    /// alive never trips it. When it runs out the call throws <see cref="TransferFailedException"/>.
    /// </para>
    /// <para>
    /// Raise it for a consumer that can stall for longer than a minute on one write (a commit, a lock).
    /// A receiver client uses the same figure for another wait: once it has accepted the sender's
    /// <c>Complete</c>, how long it waits for the transfer to close before its handle throws
    /// <see cref="TransferFailedException"/> where the data ends.
    /// </para>
    /// </remarks>
    /// <param name="timeout">Greater than zero and at most 4,294,967,294 milliseconds, the longest wait the runtime's timers take.</param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="timeout"/> is none of those. It is refused here, not left for the first transfer
    /// to find.
    /// </exception>
    public TransportRClientBuilder<T> WithAcknowledgementTimeout(TimeSpan timeout)
    {
        _state.SetAcknowledgementTimeout(timeout);
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
    /// <c>max × batch size</c>.
    /// </para>
    /// <para>
    /// On a long round trip, the window in bytes (<c>max × batch size</c>) must cover
    /// <c>RTT × bandwidth</c>, or acknowledgements set the rate.
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
    /// either having to disable the other. At least one is required; to bound nothing, do not
    /// call this method.
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
    /// Both ends need a bound at least as large as the largest batch the sender cuts; it can be raised,
    /// not removed. A receiver refuses a chunk that announces more before reserving memory for it, and
    /// the transfer fails as a protocol violation naming this setting. An item client fails on a
    /// serialized batch above its own bound; a byte client's <c>Build</c> refuses a batch size above it.
    /// </para>
    /// <para>
    /// With the <c>With*Serialization</c> extensions it also bounds a batch once decompressed: the
    /// receiver refuses one that expands past it, without decompressing the rest. The Json serializer
    /// checks this when it serializes; the MessagePack and Arrow serializers do not, so their sender
    /// must cut batches the receiver accepts (for Arrow, the compressed body plus every buffer once
    /// decompressed). A serializer passed to <see cref="WithSerializer"/> is not given this figure.
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
    /// Sets the <see cref="TimeProvider"/> used for timeouts, heartbeats, batch flushes, and token
    /// renewal. Defaults to <see cref="TimeProvider.System"/>.
    /// </summary>
    public TransportRClientBuilder<T> WithTimeProvider(TimeProvider timeProvider)
    {
        _state.SetTimeProvider(timeProvider);
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
    /// Sets a token provider made at <see cref="Build"/> from the handler the client's requests go through,
    /// the builder's logger factory, and its time provider.
    /// </summary>
    internal TransportRClientBuilder<T> WithTokenProviderFactory(Func<HttpMessageHandler, ILoggerFactory, TimeProvider, Func<Task<string>>> factory)
    {
        _state.SetTokenProviderFactory(factory);
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
    /// Has the client prove, after each registration with the hub, what the path to the hub carries of a
    /// write stream, so that the hub can tell its host before any transfer. Off by default.
    /// </summary>
    /// <remarks>
    /// It runs in the background, under the policy of <see cref="WithWriteTransport"/>: a WebSocket, then
    /// segments. On a path that accepts the upgrade without relaying its bytes, it costs up to the 5 s a
    /// WebSocket's opening is given, per registration. The hub hands what reached it to its
    /// <c>IHubProgressMonitor</c>; the client logs it. Each transfer still finds its own transport when
    /// its stream opens.
    /// </remarks>
    public TransportRClientBuilder<T> WithWriteTransportProbe()
    {
        _state.EnableWriteTransportProbe();
        return this;
    }

    /// <summary>
    /// Configures the handler every request of the client goes through: its control plane, its data
    /// streams and their WebSockets. For a proxy, a client certificate or a certificate authority.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <see cref="Build"/> calls <paramref name="configure"/> on a new handler, once for each client it
    /// builds, before that client's first request. A later call replaces an earlier one.
    /// <c>WithClientCredentials</c> given no <see cref="HttpClient"/> of its own asks for its tokens through
    /// the same handler.
    /// </para>
    /// <para>
    /// <see cref="Build"/> throws <see cref="InvalidOperationException"/>, naming the setting, when
    /// <paramref name="configure"/> turns on <see cref="SocketsHttpHandler.UseCookies"/> or
    /// <see cref="SocketsHttpHandler.AutomaticDecompression"/>. Every request is HTTP/1.1 whatever the
    /// handler allows. The events stream and each data stream hold a connection for as long as they
    /// last, so a <see cref="SocketsHttpHandler.MaxConnectionsPerServer"/> below what a client has open
    /// at once leaves its other requests waiting.
    /// </para>
    /// </remarks>
    public TransportRClientBuilder<T> ConfigureHttpHandler(Action<SocketsHttpHandler> configure)
    {
        _state.SetHttpHandlerConfiguration(configure);
        return this;
    }

    /// <summary>
    /// Supplies the <see cref="HttpMessageHandler"/> used for the client's HTTP requests, such as
    /// <c>TestServer.CreateHandler()</c> in integration tests or a custom <see cref="DelegatingHandler"/>.
    /// </summary>
    /// <param name="handler">The handler to use.</param>
    /// <param name="disposeHandler">
    /// Whether disposing the client disposes <paramref name="handler"/>. Defaults to <c>false</c>.
    /// </param>
    /// <remarks>
    /// <para>
    /// Mutually exclusive with <see cref="ConfigureHttpHandler"/>: <see cref="Build"/> throws
    /// <see cref="InvalidOperationException"/> if both are configured. Because <c>TestServer</c> does
    /// not relay WebSocket upgrades through <c>CreateHandler()</c>, pair it with
    /// <see cref="WithWriteTransport"/> set to <see cref="WriteTransportPolicy.SegmentsOnly"/> (or use
    /// Kestrel on loopback when testing WebSockets).
    /// </para>
    /// <para>
    /// Any <see cref="SocketsHttpHandler"/> or <see cref="HttpClientHandler"/> in the handler chain
    /// with <c>UseCookies</c> enabled or <c>AutomaticDecompression</c> other than <c>None</c> is
    /// refused at <see cref="Build"/>, as with <see cref="ConfigureHttpHandler"/>.
    /// </para>
    /// </remarks>
    public TransportRClientBuilder<T> WithHttpMessageHandler(HttpMessageHandler handler, bool disposeHandler = false)
    {
        _state.SetHttpMessageHandler(handler, disposeHandler);
        return this;
    }

    /// <summary>
    /// Seals every batch of this client's transfers end to end, so that the hub, and whoever terminates TLS on the way,
    /// forward bytes they cannot read.
    /// </summary>
    /// <remarks>
    /// Both ends of a transfer enable it alike: between an end that seals and one that does not, the transfer fails
    /// before its first batch, with <see cref="TransferFailureCategory.EndToEndEncryptionFailed"/> on the end that finds
    /// it. Without a pre-shared key it protects against whoever reads the path, not against a hub that rewrites it; the
    /// overload that takes a key adds that. <see cref="Build"/> throws <see cref="PlatformNotSupportedException"/> where
    /// the platform has no AES-GCM. A later call replaces an earlier one.
    /// </remarks>
    public TransportRClientBuilder<T> WithEndToEndEncryption()
    {
        _state.SetEndToEndEncryption();
        return this;
    }

    /// <summary>
    /// Seals every batch of this client's transfers end to end, with a pre-shared key mixed into each transfer's keys,
    /// so that a hub that rewrites, truncates or reroutes a transfer makes it fail instead.
    /// </summary>
    /// <param name="preSharedKey">
    /// Called once per transfer, synchronously, with the peer's client id; returns the 32 bytes both ends hold. Ignoring
    /// the id gives one key per trust domain; a key per pair also keeps a node of the domain from standing between two
    /// others. Read it from this node's own configuration, never from a service reached through the hub's front end.
    /// </param>
    /// <remarks>
    /// A provider that returns <c>null</c> or another length than 32 bytes, or throws, fails that transfer with
    /// <see cref="TransferFailureCategory.EndToEndEncryptionFailed"/>: it never falls back to encryption without the key.
    /// The array it returns stays its own, neither kept nor erased. The rest is as for
    /// <see cref="WithEndToEndEncryption()"/>.
    /// </remarks>
    public TransportRClientBuilder<T> WithEndToEndEncryption(Func<Guid, byte[]?> preSharedKey)
    {
        _state.SetEndToEndEncryption(preSharedKey);
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
            new SessionRegistry<ClientSession<T>>(logger),
            serializer,
            logger,
            _batchSize,
            wiring.Options,
            wiring.DataUrl,
            wiring.HttpClient
        );
    }
}
