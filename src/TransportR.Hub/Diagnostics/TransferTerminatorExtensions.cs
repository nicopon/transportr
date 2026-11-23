using TransportR.Hub.Services;

namespace TransportR.Hub.Diagnostics;

internal static class TransferTerminatorExtensions
{
    public static Task TerminateWithReasonAsync(
        this ITransferTerminator terminator,
        string transferId,
        string reason,
        CancellationToken cancellationToken = default) =>
        terminator is TransferTerminator concrete
            ? concrete.TerminateAsync(transferId, reason, cancellationToken)
            : terminator.TerminateAsync(transferId, cancellationToken);
}
