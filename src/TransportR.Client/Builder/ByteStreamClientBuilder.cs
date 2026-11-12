using Microsoft.Extensions.Logging;
using TransportR.Client.Configuration;
using TransportR.Client.Internal;
using TransportR.Client.Services;
using TransportR.Models;

namespace TransportR.Client;

/// <summary>
/// A fluent builder for configuring and creating a <see cref="ByteStreamClient"/>, which transfers a
/// stream of bytes instead of serialized items.
/// </summary>
/// <remarks>
/// It has no serializer, no item batch size and no receive bound in items, so that a setting that
/// means something only for items is not an overload that exists: the mismatch is a compile error rather
/// than a refusal at <see cref="Build"/>.
/// </remarks>
public sealed class ByteStreamClientBuilder
{
    private readonly ClientBuilderState _state = new();
    private readonly int _batchBytes;

    /// <param name="batchBytes">
    /// The size, in bytes, at which a batch is cut. It sets what an acknowledgement, a send window slot
    /// and a resend cover, so the sender holds at most this much times <c>WithMaxInFlightBatches</c>. There
    /// is no default: the right figure depends on the use. For continuous streaming on WAN or cloud
    /// links, 256 KB or 512 KB is recommended to align with <c>ClientOptions.StreamBufferSize</c> and
    /// minimize framing overhead.
    /// At most <c>int.MaxValue - 4</c>, a batch and its 4-byte length being held in one <c>int</c>, and
    /// at most <see cref="WithMaxBatchBytes"/> (256 MiB by default) once <see cref="Build"/> runs.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="batchBytes"/> is zero or less, or more than <c>int.MaxValue - 4</c>.
    /// </exception>
    public ByteStreamClientBuilder(int batchBytes)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(batchBytes, 0, nameof(batchBytes));
        // A batch is its body and the 4 bytes of its length, held in one int.
        ArgumentOutOfRangeException.ThrowIfGreaterThan(batchBytes, int.MaxValue - sizeof(int), nameof(batchBytes));
        _batchBytes = batchBytes;
    }

    /// <inheritdoc cref="TransportRClientBuilder{T}.WithUrl"/>
    public ByteStreamClientBuilder WithUrl(string url)
    {
        _state.SetUrl(url);
        return this;
    }

    /// <inheritdoc cref="TransportRClientBuilder{T}.WithBatchTimeout"/>
    public ByteStreamClientBuilder WithBatchTimeout(int timeoutMs)
    {
        _state.SetBatchTimeout(timeoutMs);
        return this;
    }

    /// <inheritdoc cref="TransportRClientBuilder{T}.WithMaxInFlightBatches"/>
    public ByteStreamClientBuilder WithMaxInFlightBatches(int max)
    {
        _state.SetMaxInFlightBatches(max);
        return this;
    }

    /// <summary>
    /// Bounds what the receiver holds for a consumer that is behind. Unbounded by default — the
    /// counterpart of <see cref="WithMaxInFlightBatches"/> at the other end of the transfer.
    /// </summary>
    /// <remarks>
    /// Raise the figure for throughput on a fast consumer, lower it to cap the receiver's footprint. See
    /// <see cref="BufferCapacity"/> for what a wire byte does and does not measure.
    /// </remarks>
    /// <param name="maxBytes">Maximum buffered wire bytes.</param>
    public ByteStreamClientBuilder WithReceiveCapacity(long maxBytes)
    {
        _state.SetReceiveCapacity(BufferCapacity.Bytes(maxBytes));
        return this;
    }

    /// <inheritdoc cref="TransportRClientBuilder{T}.WithMaxBatchBytes"/>
    public ByteStreamClientBuilder WithMaxBatchBytes(int maxBytes)
    {
        _state.SetMaxBatchBytes(maxBytes);
        return this;
    }

    /// <inheritdoc cref="TransportRClientBuilder{T}.WithLoggerFactory"/>
    public ByteStreamClientBuilder WithLoggerFactory(ILoggerFactory loggerFactory)
    {
        _state.SetLoggerFactory(loggerFactory);
        return this;
    }

    /// <inheritdoc cref="TransportRClientBuilder{T}.WithAccessToken"/>
    public ByteStreamClientBuilder WithAccessToken(string token)
    {
        _state.SetAccessToken(token);
        return this;
    }

    /// <inheritdoc cref="TransportRClientBuilder{T}.WithTokenProvider"/>
    public ByteStreamClientBuilder WithTokenProvider(Func<Task<string>> provider)
    {
        _state.SetTokenProvider(provider);
        return this;
    }

    /// <inheritdoc cref="TransportRClientBuilder{T}.WithTokenRefreshThreshold"/>
    public ByteStreamClientBuilder WithTokenRefreshThreshold(TimeSpan threshold)
    {
        _state.SetTokenRefreshThreshold(threshold);
        return this;
    }

    /// <inheritdoc cref="TransportRClientBuilder{T}.WithWriteTransport"/>
    public ByteStreamClientBuilder WithWriteTransport(WriteTransportPolicy policy)
    {
        _state.SetWriteTransport(policy);
        return this;
    }

    /// <summary>
    /// Builds the <see cref="ByteStreamClient"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// The batch size given to the constructor is above <see cref="WithMaxBatchBytes"/>.
    /// </exception>
    public ByteStreamClient Build()
    {
        // A receiver at the same bound would refuse every full batch: refused here, before the first.
        if (_batchBytes > _state.MaxBatchBytes)
            throw new InvalidOperationException(
                $"The batch size ({_batchBytes} bytes) is over WithMaxBatchBytes ({_state.MaxBatchBytes} bytes).");

        var logger = _state.CreateLogger<ByteStreamClient>();
        var wiring = _state.Assemble(logger);

        return new ByteStreamClient(
            wiring.Channel,
            new SessionRegistry<ByteTransferSession>(logger),
            logger,
            _batchBytes,
            wiring.Options,
            wiring.DataUrl,
            wiring.HttpClient,
            wiring.Streams);
    }
}
