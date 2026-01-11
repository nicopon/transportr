using TransportR.Interfaces;
using TransportR.Hub.SignalR.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Server.Kestrel.Core.Features;


namespace TransportR.Hub.SignalR.Controllers;

[ApiController]
[Route("api/stream")]
public class StreamController : ControllerBase
{
    private readonly ITransportRServer _brokerServer;
    private readonly ITransferManager _transferManager;
    private readonly ILogger<StreamController> _logger;

    public StreamController(
        ITransportRServer brokerServer, 
        ITransferManager transferManager,
        ILogger<StreamController> logger)
    {
        _brokerServer = brokerServer;
        _transferManager = transferManager;
        _logger = logger;
    }
    
    /// <summary>
    /// Validates that the transfer exists. Additional auth checks can be added here.
    /// </summary>
    private async Task<bool> ValidateTransfer(string transferId)
    {
        var participants = await _transferManager.GetParticipantsAsync(transferId);
        if (participants == null)
        {
            _logger.LogWarning("Security: Stream access denied - transfer {TransferId} not found", transferId);
            return false;
        }
        return true;
    }

    [HttpPost("{transferId}/send")]
    [DisableRequestSizeLimit]
    public async Task Send(string transferId)
    {
        // Validate transfer exists
        if (!await ValidateTransfer(transferId))
        {
            Response.StatusCode = 404;
            return;
        }
        
        DisableKestrelLimits();
        try
        {
            var type = ParseConnectionType(Request);
            await _brokerServer.HandleSendAsync(transferId, type, Request.Body, HttpContext.RequestAborted);
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in StreamController.Send (Transfer: {TransferId})", transferId);
            Response.StatusCode = 500;
        }
    }

    [HttpGet("{transferId}/receive")]
    public async Task Receive(string transferId)
    {
        // Validate transfer exists
        if (!await ValidateTransfer(transferId))
        {
            Response.StatusCode = 404;
            return;
        }
        
        DisableKestrelLimits();
        Response.StatusCode = 200;
        Response.ContentType = "application/octet-stream";
        await Response.StartAsync();

        try
        {
            var type = ParseConnectionType(Request);
            await _brokerServer.HandleReceiveAsync(transferId, type, Response.Body, HttpContext.RequestAborted);
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in StreamController.Receive (Transfer: {TransferId})", transferId);
        }
    }
    
    private ConnectionType ParseConnectionType(HttpRequest request)
    {
        var connectionTypeString = request.Headers["X-Connection-Type"].ToString();
        return Enum.TryParse<ConnectionType>(connectionTypeString, true, out var type) ? type : ConnectionType.Data;
    }

    /// <summary>
    /// Disables Kestrel's minimum data rate limits for this request.
    /// Streaming connections can be paused or slow, which would otherwise trigger Kestrel's Slowloris protection
    /// and disconnect the client. We explicitly disable this protection for data streams.
    /// </summary>
    private void DisableKestrelLimits()
    {
        var minRequestBodyDataRateFeature = HttpContext.Features.Get<IHttpMinRequestBodyDataRateFeature>();
        if (minRequestBodyDataRateFeature != null)
        {
            minRequestBodyDataRateFeature.MinDataRate = null;
        }

        var minResponseDataRateFeature = HttpContext.Features.Get<IHttpMinResponseDataRateFeature>();
        if (minResponseDataRateFeature != null)
        {
            minResponseDataRateFeature.MinDataRate = null;
            _logger.LogDebug("[StreamController] MinResponseDataRate Disabled for Request {TraceIdentifier}", HttpContext.TraceIdentifier);
        }
        else
        {
            // If the feature is not found, Kestrel limits might not be active (e.g. running behind IIS or different server)
            _logger.LogDebug("[StreamController] IHttpMinResponseDataRateFeature NOT FOUND for Request {TraceIdentifier}", HttpContext.TraceIdentifier);
        }
    }
}
