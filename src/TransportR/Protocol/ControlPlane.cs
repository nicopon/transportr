using System.Text.Json;

namespace TransportR.Protocol;

/// <summary>
/// The names on the control plane that are not paths: the events of a client's stream and the
/// parameters of its opening. Shared for the reason <see cref="WireRoutes"/> is.
/// </summary>
internal static class ControlPlane
{
    /// <summary>The first event of every stream: the client is registered under <see cref="RegisteredEvent.ConnectionId"/>.</summary>
    public const string Registered = "registered";

    /// <summary>
    /// Written when nothing else has been for a while. An event rather than a comment, because the
    /// client's parser does not hand it comments and the client judges its stream on what it is handed.
    /// </summary>
    public const string KeepAlive = "keepalive";

    /// <summary>A transfer to this client is being opened; answered on <see cref="WireRoutes.RepliesSegment"/>.</summary>
    public const string ReadyToReceive = "ready-to-receive";

    /// <summary>The host opens a transfer from this client; answered on <see cref="WireRoutes.RepliesSegment"/>.</summary>
    public const string RequestSend = "request-send";

    /// <summary>The hub ended a transfer this client takes part in.</summary>
    public const string CancelTransfer = "cancel-transfer";

    /// <summary>The query parameter that carries a group the client claims, repeated for each; read in dev mode only.</summary>
    public const string GroupsParameter = "group";

    /// <summary>
    /// The query parameter that announces, in milliseconds, how often the client sends its presence
    /// heartbeat: the hub closes a stream it has heard nothing from for two of them.
    /// </summary>
    public const string HeartbeatParameter = DataPlane.HeartbeatParameter;

    /// <summary>Web defaults on both ends, for the bodies and for the data of the events.</summary>
    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
}

internal sealed record RegisteredEvent(string ConnectionId);

internal sealed record ReadyToReceiveEvent(Guid RequestId, string TransferId, Guid SenderId);

internal sealed record RequestSendEvent(Guid RequestId, string TransferId, Guid ReceiverId);

internal sealed record CancelTransferEvent(string TransferId);

/// <summary>What a client answers to an event that asks it to take part in a transfer.</summary>
internal enum CallbackOutcome
{
    Accepted,

    /// <summary>Nothing in the client takes such a transfer.</summary>
    Refused,

    /// <summary>The client's own code failed while taking the transfer.</summary>
    Failed
}

internal sealed record CallbackReply(CallbackOutcome Outcome);

internal sealed record InitTransferRequest(Guid TargetClientId, int TimeoutMs);

internal sealed record InitTransferResponse(string TransferId);

/// <summary>
/// The answer to a transfer whose receiver another hub instance holds, with a 421: the cookie that
/// reaches that instance, for the client to send the same request again with it.
/// </summary>
internal sealed record AffinityRedirect(string CookieName, string Token);

/// <summary>
/// The answer to a request the hub could not carry out, with a 409; the message is written for the
/// client, and <see cref="Code"/> carries its <see cref="HubRequestFailureCategory"/>. A 429, a 503
/// (marked with <see cref="WireRoutes.RefusedHeader"/>), or a 400 carries none: the status (and
/// header on 503) says the first two, and nothing reads the body of the last.
/// </summary>
internal sealed record HubError(string Message, HubErrorCode Code = HubErrorCode.Absent);

/// <summary>
/// The <see cref="HubRequestFailureCategory"/> a <see cref="HubError"/> carries on the wire, numbered
/// apart from it so that the public enum can change without moving a code.
/// </summary>
internal enum HubErrorCode
{
    /// <summary>No code: a client fails a 409 that carries none as an answer it cannot read.</summary>
    Absent = 0,
    Refused = 1,
    HandshakeTimedOut = 2,
    TargetRefusedOrFailedToPrepare = 3,
    EndedDuringHandshake = 4,
}

internal static class HubErrorCodes
{
    /// <summary>
    /// The code a 409 carries for <paramref name="category"/>; <see cref="HubErrorCode.Absent"/> for a
    /// category no handshake on the hub ends in, which the client then fails as unreadable rather than
    /// as a category it was not given.
    /// </summary>
    public static HubErrorCode Of(HubRequestFailureCategory category) => category switch
    {
        HubRequestFailureCategory.Refused => HubErrorCode.Refused,
        HubRequestFailureCategory.HandshakeTimedOut => HubErrorCode.HandshakeTimedOut,
        HubRequestFailureCategory.TargetRefusedOrFailedToPrepare => HubErrorCode.TargetRefusedOrFailedToPrepare,
        HubRequestFailureCategory.EndedDuringHandshake => HubErrorCode.EndedDuringHandshake,
        _ => HubErrorCode.Absent,
    };

    /// <summary>The category <paramref name="code"/> stands for; null for an absent code, or one this version does not know.</summary>
    public static HubRequestFailureCategory? CategoryOf(HubErrorCode code) => code switch
    {
        HubErrorCode.Refused => HubRequestFailureCategory.Refused,
        HubErrorCode.HandshakeTimedOut => HubRequestFailureCategory.HandshakeTimedOut,
        HubErrorCode.TargetRefusedOrFailedToPrepare => HubRequestFailureCategory.TargetRefusedOrFailedToPrepare,
        HubErrorCode.EndedDuringHandshake => HubRequestFailureCategory.EndedDuringHandshake,
        _ => null,
    };
}

internal sealed record TransferProgress(long Items, long Acks);
