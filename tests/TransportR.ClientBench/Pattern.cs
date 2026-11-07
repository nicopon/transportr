namespace TransportR.ClientBench;

/// <summary>
/// The one block of pseudo-random bytes every source and every check of a run is cut from. Its length
/// is not a power of two, so the position a batch boundary falls on keeps moving across the block.
/// </summary>
internal static class Pattern
{
    public const int Length = (1 << 20) + 13;

    public static byte[] Create(int seed)
    {
        var block = new byte[Length];
        new Random(seed).NextBytes(block);
        return block;
    }

    /// <summary>
    /// Whether <paramref name="data"/> equals the bytes of the repeated block starting at
    /// <paramref name="position"/>.
    /// </summary>
    public static bool Matches(byte[] block, long position, ReadOnlySpan<byte> data)
    {
        var offset = (int)(position % block.Length);
        while (!data.IsEmpty)
        {
            var run = Math.Min(data.Length, block.Length - offset);
            if (!data[..run].SequenceEqual(block.AsSpan(offset, run)))
                return false;

            data = data[run..];
            offset = 0;
        }

        return true;
    }
}
