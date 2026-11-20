using TransportR.Client;
using TransportR.Client.Protocol;

namespace TransportR.Tests;

/// <summary>
/// End-to-end encryption as two sessions run it through a hub that relays, records and, where a test
/// says so, rewrites what they write.
/// </summary>
public class EndToEndSessionTests
{
    [Fact]
    public void TheAbortReasonsOfEncryption_HaveTheirWireValues_AndATextOfTheirOwn()
    {
        Assert.Equal(5, (int)AbortReason.EncryptionModeMismatch);
        Assert.Equal(6, (int)AbortReason.VerificationFailed);
        Assert.NotEqual("The peer aborted the transfer (reason 5).", AbortReason.EncryptionModeMismatch.Describe());
        Assert.NotEqual("The peer aborted the transfer (reason 6).", AbortReason.VerificationFailed.Describe());
    }

    [Fact]
    public void EndToEndEncryptionFailed_ComesAfterEveryCategoryThatWasThereBefore()
    {
        Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, Enum.GetValues<TransferFailureCategory>().Max());
        Assert.Equal((int)TransferFailureCategory.Other + 1, (int)TransferFailureCategory.EndToEndEncryptionFailed);
    }
}
