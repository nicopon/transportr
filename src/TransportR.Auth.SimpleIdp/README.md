# TransportR.Auth.SimpleIdp

Embeddable Identity Provider for TransportR authentication testing and development.

This project is not published to NuGet (`IsPackable=false`): reference it from the solution.

## Purpose

Provides a lightweight OpenIddict-based IDP that can be:
1. **Embedded** in an ASP.NET Core application for testing
2. **Standalone** for local development: `samples/TestIdp` is a host built this way

## Usage

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSimpleIdp(options =>
{
    // Groups: the scope `transportr:group:<name>` is derived from each name.
    options.AddClient("sender", "sender-secret", "senders");
    options.AddClient("receiver", "receiver-secret", "receivers");
});

var app = builder.Build();
app.MapSimpleIdp();
app.Run();
```

To grant raw scopes instead of groups, use `AddClientWithScopes(clientId, secret, scopes)`.

## Endpoints

| Endpoint | Purpose |
|----------|---------|
| `/connect/token` | Token endpoint (client_credentials), mapped by `MapSimpleIdp()` |

`samples/TestIdp` adds `/health` and `/` on top of it.

## Token Acquisition

```bash
curl -X POST <idp-url>/connect/token \
  -d "grant_type=client_credentials" \
  -d "client_id=sender" \
  -d "client_secret=sender-secret" \
  -d "scope=transportr:group:senders"
```

## Clients

The package holds no clients of its own. `samples/TestIdp` seeds `sender`, `receiver` and `test-client` from its `appsettings.json`.

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `TokenLifetime` | 1 hour | JWT expiration |

## Dependencies

- `OpenIddict` - OAuth 2.0 / OpenID Connect implementation
- `Microsoft.EntityFrameworkCore.InMemory` - In-memory persistence
