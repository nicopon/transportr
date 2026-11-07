using System.IO.Pipelines;
using System.Net;
using TransportR.Abstractions;
using TransportR.Client.Sessions;
using TransportR.Protocol;
using TransportR.Tests.TestHelpers;

namespace TransportR.Tests;

/// <summary>
/// A segment the hub refuses ends the write side, not what the hub has already relayed on the read
/// side: a receiver's acknowledgement refused because the sender's Abort ended the transfer comes
/// back while that Abort is still on its way down the data GET.
/// </summary>
public class RefusedAcknowledgementTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task Receiver_WhoseAcknowledgementIsRefused_FailsOnTheAbortItsDataStreamStillCarries()
    {
        var context = new StubSessionContext(new HttpClient(new AbortedTransferHub())) { ProbeAnswers = true };
        await using var session = new TestSession(context, Guid.NewGuid().ToString(), batchSize: 1, dataUrl: "http://test/data", affinity: null);
        await session.StartReceiverAsync();

        var consumer = Task.Run(async () =>
        {
            await foreach (var _ in session.ReceiveAsync(CancellationToken.None)) { }
        });

        var failure = await Assert.ThrowsAsync<TransferFailedException>(() => consumer.WaitAsync(Bound));
        Assert.Contains(AbortReason.SerializationFailed.Describe(), failure.Message);
    }

    /// <summary>
    /// A transfer whose sender aborted after its first batch. The data GET carries that batch, then the
    /// Abort once the acknowledgement of it has been refused, and ends; the transfer is gone for every
    /// request after the first GET.
    /// </summary>
    private sealed class AbortedTransferHub : HttpMessageHandler
    {
        private readonly TaskCompletionSource _refused = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _gets;

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request.Method == HttpMethod.Post)
            {
                await request.Content!.CopyToAsync(Stream.Null, cancellationToken);
                _refused.TrySetResult();
                return HubResponse.With(HttpStatusCode.Conflict);
            }

            if (Interlocked.Increment(ref _gets) > 1)
            {
                return HubResponse.With(HttpStatusCode.NotFound);
            }

            var transferId = Guid.Parse(request.RequestUri!.Segments[^2].TrimEnd('/'));
            var pipe = new Pipe();
            _ = RelayAsync(pipe.Writer, transferId);
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StreamContent(pipe.Reader.AsStream()) };
        }

        private async Task RelayAsync(PipeWriter writer, Guid transferId)
        {
            var wire = writer.AsStream();

            // What the test serializer writes after the count the session prefixes: its own count, and a byte.
            byte[] body = [.. BitConverter.GetBytes(1), 0];
            await ChunkIO.WriteBatchAsync(wire, transferId, batchId: 0, count: 1, body);

            // Released a moment after the refusal is answered, so that the refusal is read first.
            await _refused.Task;
            await Task.Delay(TimeSpan.FromMilliseconds(200));
            await ChunkIO.WriteControlAsync(wire, transferId, (int)AbortReason.SerializationFailed, ChunkType.Abort);
            await writer.CompleteAsync();
        }
    }
}
