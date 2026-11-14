using System.IO.Pipelines;
using System.Net;
using TransportR.Abstractions;
using TransportR.Client.Protocol;
using TransportR.Tests.TestHelpers;

namespace TransportR.Tests;

/// <summary>
/// The length a chunk announces comes from the peer, and a client reads it before anything else of
/// the chunk: one above what the stream may carry fails the transfer before any memory is rented for it.
/// </summary>
public class AnnouncedPayloadBoundTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(5);

    private const int DefaultMaxBatchBytes = 256 * 1024 * 1024;

    [Theory]
    [InlineData(false, "WithMaxBatchBytes")]
    [InlineData(true, "control stream")]
    public async Task AChunkAnnouncingMoreThanTheStreamCarries_FailsTheTransfer_WithoutRentingIt(bool sender, string named)
    {
        // Arrange: one past the default bound, which a pool would round up to a 512 MiB array.
        const int announced = DefaultMaxBatchBytes + 1;
        var transferId = Guid.NewGuid();
        var session = new TestSession(
            new StubSessionContext(new HttpClient(new AnnouncingHub(transferId, announced))),
            transferId.ToString(), batchSize: 1, dataUrl: "http://test/data", affinity: null);
        var allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);

        // Act
        if (sender) await session.StartSenderAsync();
        else await session.StartReceiverAsync();
        var failure = await Assert.ThrowsAsync<TransferFailedException>(
            () => session.WaitForCompletionAsync(CancellationToken.None).WaitAsync(Bound));
        var allocated = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
        await session.DisposeAsync();

        // Assert: the whole process is counted, so the margin is wide, and still half the payload.
        Assert.Contains(named, failure.Message);
        Assert.True(allocated < DefaultMaxBatchBytes,
            $"{allocated} bytes were allocated while a chunk announcing {announced} was refused.");
    }

    /// <summary>
    /// A hub whose read streams hand out one chunk header and then nothing: the payload it announces
    /// never comes. Write streams are relayed nowhere.
    /// </summary>
    private sealed class AnnouncingHub(Guid transferId, int payloadLength) : HttpMessageHandler
    {
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request.Method == HttpMethod.Post)
                return await Segments.RelayAsync(request, Stream.Null, cancellationToken);

            var header = new byte[ChunkHeader.Size];
            ChunkHeader.CreateBatch(transferId, batchId: 0, count: 1, payloadLength).WriteTo(header);
            var pipe = new Pipe();
            await pipe.Writer.WriteAsync(header, cancellationToken);
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StreamContent(pipe.Reader.AsStream()) };
        }
    }
}
