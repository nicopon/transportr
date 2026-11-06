using TransportR.Client.Batching;

namespace TransportR.Tests;

public class ByteChunkTests
{
    [Fact]
    public void Memory_IsTheFirstLengthBytesOfTheOwner()
    {
        using var chunk = new ByteChunk(new CountingOwner([1, 2, 3, 4, 5]), length: 3);

        Assert.Equal(3, chunk.Length);
        Assert.Equal([1, 2, 3], chunk.Memory.ToArray());
    }

    [Fact]
    public void Dispose_ReturnsTheOwnerOnce_HoweverOftenItIsCalled()
    {
        var owner = new CountingOwner([1, 2]);
        var chunk = new ByteChunk(owner, length: 2);

        chunk.Dispose();
        chunk.Dispose();

        Assert.Equal(1, owner.Disposals);
    }

    [Fact]
    public void Memory_AfterDispose_Throws()
    {
        var chunk = new ByteChunk(new CountingOwner([1, 2]), length: 2);
        chunk.Dispose();

        Assert.Throws<ObjectDisposedException>(() => chunk.Memory);
    }

    [Fact]
    public async Task Dispose_FromTwoThreadsAtOnce_ReturnsTheOwnerOnce()
    {
        for (var round = 0; round < 200; round++)
        {
            var owner = new CountingOwner([1]);
            var chunk = new ByteChunk(owner, length: 1);

            await Task.WhenAll(Task.Run(chunk.Dispose), Task.Run(chunk.Dispose));

            Assert.Equal(1, owner.Disposals);
        }
    }
}
