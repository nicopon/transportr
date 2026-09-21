using System.Collections.Concurrent;
using TransportR.Interfaces;
using TransportR.Models;
using Yarp.ReverseProxy.Configuration;
using Yarp.ReverseProxy.Forwarder;

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.ConfigureKestrel(options =>
{
    options.Limits.MinRequestBodyDataRate = null;
    options.Limits.MinResponseDataRate = null;
    options.Limits.MaxRequestBodySize = null;
    options.Limits.KeepAliveTimeout = TimeSpan.FromMinutes(10);
    options.Limits.RequestHeadersTimeout = TimeSpan.FromMinutes(10);
});

// In-memory state store (shared across all hub instances)
var stateStore = new ProxyStateStore();

// YARP configuration with sticky sessions
var routes = new[]
{
    new RouteConfig
    {
        RouteId = "hub-route",
        ClusterId = "hub-cluster",
        Match = new RouteMatch { Path = "{**catch-all}" }
    }
};

var clusters = new[]
{
    new ClusterConfig
    {
        ClusterId = "hub-cluster",
        SessionAffinity = new SessionAffinityConfig
        {
            Enabled = true,
            Policy = "Cookie",
            AffinityKeyName = "affinity"
        },
        HttpRequest = new ForwarderRequestConfig
        {
            ActivityTimeout = TimeSpan.FromHours(1),
            Version = Version.Parse("1.1"),
            VersionPolicy = HttpVersionPolicy.RequestVersionOrLower
        },
        Destinations = new Dictionary<string, DestinationConfig>
        {
            ["hub-a"] = new() { Address = "http://localhost:5001" },
            ["hub-b"] = new() { Address = "http://localhost:5002" }
        }
    }
};

builder.Services.AddReverseProxy()
    .LoadFromMemory(routes, clusters);

builder.Services.AddSingleton<ProxyStateStore>(stateStore);

var app = builder.Build();

// State API endpoints
var stateApi = app.MapGroup("/api/state");

stateApi.MapPost("/clients", async (ClientRegistration client, ProxyStateStore store) =>
{
    await store.SaveClientAsync(client);
    return Results.Created($"/api/state/clients/{client.ClientId}", client);
});

stateApi.MapGet("/clients", async (ProxyStateStore store) =>
{
    return Results.Ok(await store.GetAllClientsAsync());
});

stateApi.MapGet("/clients/{clientId:guid}", async (Guid clientId, ProxyStateStore store) =>
{
    var client = await store.GetClientAsync(clientId);
    return client != null ? Results.Ok(client) : Results.NotFound();
});

stateApi.MapPost("/connections/{connectionId}/disconnected", async (string connectionId, DateTime whenUtc, ProxyStateStore store) =>
{
    await store.MarkDisconnectedAsync(connectionId, whenUtc);
    return Results.NoContent();
});

stateApi.MapDelete("/clients/{clientId:guid}", async (Guid clientId, ProxyStateStore store) =>
{
    await store.DeleteClientAsync(clientId);
    return Results.NoContent();
});

stateApi.MapPut("/clients/{clientId:guid}/expiry", async (Guid clientId, DateTime expiry, ProxyStateStore store) =>
{
    await store.UpdateClientTokenExpiryAsync(clientId, expiry);
    return Results.NoContent();
});

// ========== Sessions (HA) ==========

stateApi.MapPost("/sessions", async (TransferSession session, ProxyStateStore store) =>
{
    await store.SaveSessionAsync(session);
    return Results.Created($"/api/state/sessions/{session.TransferId}", session);
});

stateApi.MapGet("/sessions/{transferId}", async (string transferId, ProxyStateStore store) =>
{
    var session = await store.GetSessionAsync(transferId);
    return session != null ? Results.Ok(session) : Results.NotFound();
});

stateApi.MapPut("/sessions/{transferId}/expiry", async (string transferId, DateTime newExpiry, ProxyStateStore store) =>
{
    await store.ExtendSessionExpiryAsync(transferId, newExpiry);
    return Results.NoContent();
});

stateApi.MapDelete("/sessions/{transferId}", async (string transferId, ProxyStateStore store) =>
{
    await store.DeleteSessionAsync(transferId);
    return Results.NoContent();
});

// YARP proxy
app.MapReverseProxy();

Console.WriteLine("TransportR TestProxy started on http://localhost:5000");
Console.WriteLine("  YARP -> Hub A (5001), Hub B (5002)");
Console.WriteLine("  State API: /api/state/*");

app.Run();

// In-memory state store for the proxy
public class ProxyStateStore : IStateStore
{
    private readonly ConcurrentDictionary<Guid, ClientRegistration> _clients = new();
    private readonly ConcurrentDictionary<string, Guid> _connectionIndex = new();
    private readonly ConcurrentDictionary<string, TransferSession> _sessions = new();

    public Task SaveClientAsync(ClientRegistration client)
    {
        _clients[client.ClientId] = client;
        _connectionIndex[client.ConnectionId] = client.ClientId;
        return Task.CompletedTask;
    }

    public Task<ClientRegistration?> GetClientAsync(Guid clientId)
    {
        return Task.FromResult(_clients.TryGetValue(clientId, out var client) ? client : null);
    }

    public Task MarkDisconnectedAsync(string connectionId, DateTime whenUtc)
    {
        if (_connectionIndex.TryGetValue(connectionId, out var clientId) &&
            _clients.TryGetValue(clientId, out var current) &&
            current.ConnectionId == connectionId)
        {
            _clients.TryUpdate(clientId, current with { DisconnectedAtUtc = whenUtc }, current);
        }

        return Task.CompletedTask;
    }

    public Task<ClientRegistration?> GetClientByConnectionIdAsync(string connectionId)
    {
        if (_connectionIndex.TryGetValue(connectionId, out var clientId) &&
            _clients.TryGetValue(clientId, out var client))
        {
            return Task.FromResult<ClientRegistration?>(client);
        }
        return Task.FromResult<ClientRegistration?>(null);
    }

    public Task<IReadOnlyList<ClientRegistration>> GetAllClientsAsync()
    {
        return Task.FromResult<IReadOnlyList<ClientRegistration>>(_clients.Values.ToList());
    }

    public Task DeleteClientAsync(Guid clientId)
    {
        if (_clients.TryRemove(clientId, out var client))
        {
            _connectionIndex.TryRemove(client.ConnectionId, out _);
        }
        return Task.CompletedTask;
    }

    public Task UpdateClientTokenExpiryAsync(Guid clientId, DateTime expiry)
    {
        if (_clients.TryGetValue(clientId, out var existing))
        {
            _clients[clientId] = existing with { TokenExpiry = expiry };
        }
        return Task.CompletedTask;
    }

    // ========== Sessions ==========

    public Task SaveSessionAsync(TransferSession session)
    {
        _sessions[session.TransferId] = session;
        return Task.CompletedTask;
    }

    public Task<TransferSession?> GetSessionAsync(string transferId)
    {
        if (_sessions.TryGetValue(transferId, out var session))
        {
             if (session.ExpiresAtUtc < DateTime.UtcNow)
             {
                 _sessions.TryRemove(transferId, out _);
                 return Task.FromResult<TransferSession?>(null);
             }
             return Task.FromResult<TransferSession?>(session);
        }
        return Task.FromResult<TransferSession?>(null);
    }

    public Task ExtendSessionExpiryAsync(string transferId, DateTime newExpiry)
    {
        if (_sessions.TryGetValue(transferId, out var session))
        {
             if (session.ExpiresAtUtc > DateTime.UtcNow)
             {
                 _sessions[transferId] = session with { ExpiresAtUtc = newExpiry };
             }
        }
        return Task.CompletedTask;
    }

    public Task DeleteSessionAsync(string transferId)
    {
        _sessions.TryRemove(transferId, out _);
        return Task.CompletedTask;
    }
}
