using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Client.SignalR.Configuration;
using TransportR.Client.SignalR.Internal;
using TransportR.Client.SignalR.Services;
using TransportR.Client.SignalR.Sessions;
using TransportR.Interfaces;
using TransportR.Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;


using TransportR.Serialization.MessagePack; // Extension

namespace TransportR.Tests;

public class SignalRIntegrationTests
{
    private readonly ITestOutputHelper _output;

    public SignalRIntegrationTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public async Task InitTransfer_ShouldSucceed_WhenHandlersReturnValues()
    {
        // 1. Start Server
        using var host = TestHubServerFactory.CreateHost();
        var server = host.GetTestServer();
        var handler = server.CreateHandler();

        // 2. Create Clients
        await using var sender = CreateClient(handler, "http://localhost", "sender");
        await using var receiver = CreateClient(handler, "http://localhost", "receiver");

        // 3. Connect clients
        // Note: In dev mode (TestHubServerFactory), groups are auto-assigned to ["test"]
        await sender.ConnectAsync();
        await receiver.ConnectAsync();
        
        // Wait briefly for registration
        await Task.Delay(500);

        // 4. Setup Receiver to "Accept" the transfer
        bool getReadyCalled = false;
        receiver.OnTransferStarted += (transfer) => 
        {
            getReadyCalled = true;
            return Task.CompletedTask;
        };

        // 5. Init Transfer (This is where it failed with HubException)
        var sendSession = await sender.InitTransferAsync(receiver.ClientId, batchSize: 100);

        // Assert
        Assert.NotNull(sendSession);
        Assert.True(getReadyCalled, "Receiver OnTransferStarted should have been triggered");
    }

    private SignalRDataClient<string> CreateClient(HttpMessageHandler handler, string url, string logPrefix)
    {
        // Custom Connection Manager that uses TestServer's HttpMessageHandler
        var options = new ClientOptions();
        var logger = NullLogger.Instance; // Can use XunitLogger if needed
        
        // We need to bypass the standard HubConnectionBuilder.WithUrl and inject the handler
        // But HubConnectionBuilderExtensions only supports standard HttpClient or options.
        // THE TRICK: Use WithUrl(url, options => options.HttpMessageHandlerFactory = _ => handler)
        
        var connectionManager = new HubConnectionManager(
            $"{url}/hub/command",
            options,
            logger,
            initialAccessToken: null,
            tokenProvider: null,
             configureHttpConnection: (httpOpts) => 
             {
                 httpOpts.HttpMessageHandlerFactory = _ => handler;
             }
        );
        
        var registry = new SessionRegistry<string>(logger);
        var serializer = new TransportR.Serialization.MessagePack.Services.MessagePackBatchSerializer();
        var streamManager = new Microsoft.IO.RecyclableMemoryStreamManager();
        var httpClient = new HttpClient(handler) { BaseAddress = new Uri(url) };

        return new SignalRDataClient<string>(
            connectionManager,
            registry,
            serializer,
            logger,
            defaultBatchSize: 100,
            options,
            url,
            httpClient,
            streamManager
        );
    }
}
