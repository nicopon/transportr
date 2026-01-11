using System.Net;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Logging;
using Microsoft.IO;
using TransportR.Abstractions;
using TransportR.Client.SignalR.Configuration;
using TransportR.Client.SignalR.Internal;
using TransportR.Client.SignalR.Sessions;
using TransportR.Interfaces;
using TransportR.Models;

namespace TransportR.Client.SignalR.Services;

/// <summary>
/// SignalR-based client for sending and receiving data transfers.
/// Acts as a coordinator between HubConnectionManager, SessionRegistry, and DataSessions.
/// </summary>
public partial class SignalRDataClient<T> : IDataSender<T>, IDataReceiver<T>, ISessionContext, IAsyncDisposable
{
    public Guid ClientId { get; }

    // Dependencies
    private readonly HubConnectionManager _connectionManager;
    private readonly SessionRegistry<T> _sessionRegistry;
    private readonly IBatchSerializer _serializer;
    private readonly ILogger _logger;
    private readonly ClientOptions _options;
    
    // Configuration
    private readonly int _defaultBatchSize;
    private readonly string _dataUrl;

    // Infrastructure
    private readonly RecyclableMemoryStreamManager _streamManager;
    private readonly HttpClient _httpClient;

    public event Func<Receive<T>, Task>? OnTransferStarted;

    // Construtor - Now simplified and dependency injected via Builder
    // NOTE: Made internal so only Builder/Factory instantiates it
    // Construtor - Now simplified and dependency injected via Builder
    // NOTE: Made internal so only Builder/Factory instantiates it
    internal SignalRDataClient(
        HubConnectionManager connectionManager,
        SessionRegistry<T> sessionRegistry,
        IBatchSerializer serializer,
        ILogger logger,
        int defaultBatchSize,
        ClientOptions options,
        string dataUrl,
        HttpClient httpClient,
        RecyclableMemoryStreamManager streamManager)
    {
        ClientId = Guid.NewGuid();
        _connectionManager = connectionManager;
        _sessionRegistry = sessionRegistry;
        _serializer = serializer;
        _logger = logger;
        _defaultBatchSize = defaultBatchSize;
        _options = options;
        _dataUrl = dataUrl;
        
        _streamManager = streamManager;
        _httpClient = httpClient;
        
        // SRP: Extract signaling logic
        var signaling = new SignalRTransferSignaling<T>(
            connectionManager,
            sessionRegistry,
            this, // ISessionContext
            logger,
            defaultBatchSize,
            dataUrl,
            (t) => OnTransferStarted?.Invoke(t) ?? Task.CompletedTask
        );
        signaling.Initialize();
    }


    private IEnumerable<string>? _lastConnectGroups;

    public async Task ConnectAsync(IEnumerable<string>? groups = null, CancellationToken cancellationToken = default)
    {
        _lastConnectGroups = groups;
        await _connectionManager.StartAsync(cancellationToken);
        
        var groupsList = groups?.ToList();
        _logger.LogInformation("Registering Client (ClientId={ClientId}, Groups=[{Groups}])...", 
            ClientId, groupsList != null ? string.Join(", ", groupsList) : "none");

        await _connectionManager.Connection.InvokeAsync(nameof(ICommandHub.Connect), ClientId, groupsList, cancellationToken);
    }

    public async Task DisconnectAsync()
    {
        _logger.LogInformation("Disconnecting Client {ClientId}...", ClientId);
        
        // Stop all sessions
        await _sessionRegistry.DisposeAsync();
        
        // Stop Hub
        await _connectionManager.DisposeAsync();
    }

    public async Task<List<ClientInfo>> GetReceiversAsync(CancellationToken cancellationToken = default)
    {
        return await _connectionManager.Connection.InvokeAsync<List<ClientInfo>>(nameof(ICommandHub.GetReceivers), cancellationToken);
    }

    public async Task<Send<T>> InitTransferAsync(Guid targetClientId, int? batchSize = null)
    {
        int size = batchSize ?? _defaultBatchSize;
        _logger.LogInformation("Initiating Transfer to ReceiverId={TargetId} (Size={Size})...", targetClientId, size);

        try
        {
            var transferId = await _connectionManager.Connection.InvokeAsync<string>(nameof(ICommandHub.InitTransfer), targetClientId, size, 20000);
            
            if (!_sessionRegistry.TryGetSession(transferId, out var session))
            {
                 throw new InvalidOperationException($"Failed to acquire session for {transferId} after Hub confirmation. Logic error?");
            }
            
            return new HttpSend<T>(session!);
        }
        catch (Exception ex) when (ex.Message.Contains("AFFINITY_REDIRECT:"))
        {
            var match = System.Text.RegularExpressions.Regex.Match(ex.Message, @"AFFINITY_REDIRECT:([^:]+):(.+)");
            if (!match.Success) throw new InvalidOperationException($"Invalid affinity redirect format: {ex.Message}");
            
            var cookieName = match.Groups[1].Value;
            var affinityToken = match.Groups[2].Value;
            
            await _connectionManager.HandleAffinityRedirectAsync(cookieName, affinityToken, async () => 
            {
                // Re-register Client
                var groupsList = _lastConnectGroups?.ToList();
                _logger.LogInformation("Re-registering Client after redirect (ClientId={ClientId}, Groups=[{Groups}])...", 
                     ClientId, groupsList != null ? string.Join(", ", groupsList) : "none");

                await _connectionManager.Connection.InvokeAsync(nameof(ICommandHub.Connect), ClientId, groupsList, CancellationToken.None);
            });
            
            // Re-invoke Connect is handled by the callback above.
            // Now retry InitTransfer on new connection
            return await InitTransferAsync(targetClientId, batchSize);
        }
    }

    public IReadOnlyList<TransferInfo> GetTransfers()
    {
        return
        [
            .. _sessionRegistry.Sessions
                .Select(s => new TransferInfo(
                    s.TransferId,
                    s.DataUrl,
                    s.IsSender ? TransferDirection.Sending : TransferDirection.Receiving,
                    s.IsCompleted ? TransferStatus.Completed : TransferStatus.Active,
                    s.ItemsProcessed,
                    0,
                    s.StartedAt,
                    DateTime.UtcNow - s.StartedAt,
                    s.AcksReceived,
                    s.AcksSent
                ))
        ];
    }

    public async ValueTask DisposeAsync()
    {
        await DisconnectAsync();
        _httpClient.Dispose();
        GC.SuppressFinalize(this);
    }
    
    // --- Internal Logic ---
    


    #region ISessionContext Implementation

    Guid ISessionConfig.ClientId => ClientId;
    ILogger ISessionConfig.Logger => _logger;
    IBatchSerializer ISessionConfig.Serializer => _serializer;
    HttpClient IStreamTransport.HttpClient => _httpClient;
    HubConnection ISignalRInteraction.HubConnection => _connectionManager.Connection; // Expose underlying connection
    int ISessionConfig.BatchFlushIntervalMs => _options.BatchFlushIntervalMs;
    RecyclableMemoryStreamManager ISessionConfig.StreamManager => _streamManager;
    ClientOptions ISessionConfig.Options => _options;
    // AffinityCookies removed

    void ISignalRInteraction.RemoveSession(string transferId)
    {
        _sessionRegistry.TryRemove(transferId, out _);
    }

    Task<string?> IStreamTransport.GetAccessTokenAsync(bool forceRefresh)
    {
        return _connectionManager.GetAccessTokenAsync(forceRefresh);
    }

    #endregion
}
