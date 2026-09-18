using Microsoft.Extensions.Logging;
using TransportR.Client.SignalR;
using TransportR.Client.SignalR.Configuration;

namespace TransportR.Auth.ClientCredentials;

public static class TransportRClientBuilderAuthExtensions
{
    private static ClientCredentialsTokenProvider? _provider;

    public static TransportRClientBuilder<T> WithClientCredentials<T>(
        this TransportRClientBuilder<T> builder, 
        string tokenEndpoint, 
        string clientId, 
        string clientSecret, 
        string? scope,
        ILoggerFactory? loggerFactory = null) // Optional logger factory explicit pass, or we'd need access to builder's factory (internal)
    {
         var options = new ClientCredentialsOptions
         {
             TokenEndpoint = tokenEndpoint,
             ClientId = clientId,
             ClientSecret = clientSecret,
             Scope = scope
         };
         
         // In a real generic builder pattern, we'd register this as a pending action to run during Build()
         // to use the configured LoggerFactory. 
         // But TransportRClientBuilder doesn't expose internal state or middleware pipeline easily.
         // A common pattern is to just instantiate usage here.
         // BUT wait, builder.WithTokenProvider takes a Func<Task<string>>. 
         // We can defer creation inside that Func if we want, OR just ask the user to pass a loggerFactory if they have one early.
         
         // Simplification: We create the provider immediately.
         _provider = new ClientCredentialsTokenProvider(options, loggerFactory?.CreateLogger<ClientCredentialsTokenProvider>() ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<ClientCredentialsTokenProvider>.Instance);
         
         return builder.WithTokenProvider(_provider.AsTokenProvider());
    }
}
