using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Client.Auth;

namespace TransportR.Client;

/// <summary>
/// Extension methods for configuring OAuth2 client credentials authentication on TransportRClientBuilder.
/// </summary>
public static class TransportRClientBuilderAuthExtensions
{
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
        return builder.WithTokenProvider(CreateProvider(tokenEndpoint, clientId, clientSecret, scope, loggerFactory));
    }

    /// <inheritdoc cref="WithClientCredentials{T}(TransportRClientBuilder{T}, string, string, string, string?, ILoggerFactory?)"/>
    public static ByteStreamClientBuilder WithClientCredentials(
        this ByteStreamClientBuilder builder,
        string tokenEndpoint,
        string clientId,
        string clientSecret,
        string? scope,
        ILoggerFactory? loggerFactory = null)
    {
        return builder.WithTokenProvider(CreateProvider(tokenEndpoint, clientId, clientSecret, scope, loggerFactory));
    }

    private static Func<Task<string>> CreateProvider(
        string tokenEndpoint, string clientId, string clientSecret, string? scope, ILoggerFactory? loggerFactory)
    {
        var options = new ClientCredentialsOptions
        {
            TokenEndpoint = tokenEndpoint,
            ClientId = clientId,
            ClientSecret = clientSecret,
            Scope = scope
        };

        var provider = new ClientCredentialsTokenProvider(
            options,
            loggerFactory?.CreateLogger<ClientCredentialsTokenProvider>() ?? NullLogger<ClientCredentialsTokenProvider>.Instance);

        return provider.AsTokenProvider();
    }
}
