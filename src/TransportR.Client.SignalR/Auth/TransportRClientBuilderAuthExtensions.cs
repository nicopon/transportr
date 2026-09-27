using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Client.SignalR;
using TransportR.Client.SignalR.Configuration;

namespace TransportR.Auth.ClientCredentials;

/// <summary>
/// Extension methods for configuring OAuth2 client credentials authentication on TransportRClientBuilder.
/// </summary>
public static class TransportRClientBuilderAuthExtensions
{
    private static ClientCredentialsTokenProvider? _provider;

    /// <summary>
    /// Configures the client to acquire tokens using OAuth2 client credentials flow.
    /// </summary>
    public static TransportRClientBuilder<T> WithClientCredentials<T>(
        this TransportRClientBuilder<T> builder, 
        string tokenEndpoint, 
        string clientId, 
        string clientSecret, 
        string? scope,
        ILoggerFactory? loggerFactory = null)
    {
        var options = new ClientCredentialsOptions
        {
            TokenEndpoint = tokenEndpoint,
            ClientId = clientId,
            ClientSecret = clientSecret,
            Scope = scope
        };
        
        _provider = new ClientCredentialsTokenProvider(
            options, 
            loggerFactory?.CreateLogger<ClientCredentialsTokenProvider>() ?? NullLogger<ClientCredentialsTokenProvider>.Instance);
        
        return builder.WithTokenProvider(_provider.AsTokenProvider());
    }
}
