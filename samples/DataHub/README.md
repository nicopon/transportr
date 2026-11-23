# DataHub

Sample TransportR hub server application: it hosts a hub with ASP.NET Core, the control plane and the HTTP streaming data plane.

## Quick Start

```bash
dotnet run --urls http://localhost:5252
```

## Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check |

The hub's own routes are listed under "Communication Channels" in `TransportRArchitecture.md`.

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ASPNETCORE_URLS` | http://localhost:5252 | Server URL |
| `HUB_PORT` | 5252 | Port the hub names itself by (issuer of the embedded IdP) |
| `EnableAuth` | false | JWT authentication; dev mode (default group `dev`) when false |
| `EnableEmbeddedIdp` | false | Hosts SimpleIdp on the hub itself |
| `Oidc__Authority` | http://localhost:5100 | External IdP, when auth is on and the embedded one is off |
| `FLOWCONTROL_MODE` | default | `default` (senders → receivers), `wildcard` (adds `test` → `*`), `none` (no rules) |
| `USE_REDIS` | — | Redis connection string: Redis store and affinity handoff |
| `USE_TEST_PROXY` | — | TestProxy URL: its state store and affinity handoff (ignored when `USE_REDIS` is set) |
| `OUTPUT_DIR` | output | Where `hub.log` is written |
| `Logging__LogLevel__Default` | Information | Log level |

## Structure

```
DataHub/
├── Program.cs                    # Application entry point
├── Adapters/
│   └── ProgressMonitorAdapter.cs # IHubProgressMonitor implementation
├── Middleware/
│   └── TransferIdMiddleware.cs   # Reads the X-Transfer-Id header
└── Services/
    ├── ProgressMonitorService.cs # Progress tracking service
    ├── TestProxyStateStore.cs    # IStateStore kept in the TestProxy (HA harness)
    └── TestProxyStoreExtensions.cs
```
