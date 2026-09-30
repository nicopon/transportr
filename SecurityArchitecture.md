# TransportR Security Architecture

## Overview

TransportR uses **JWT-based authentication** via OAuth 2.0. Groups are derived from token scopes (`transportr:group:*`), preventing spoofing.

```mermaid
sequenceDiagram
    participant Client
    participant IDP as Identity Provider
    participant Hub as DataHub

    Client->>IDP: client_credentials grant
    IDP-->>Client: JWT { scope: "transportr:group:senders" }
    Client->>Hub: Connect with JWT
    Hub-->>Client: Group extracted from token
```

---

## Scope Convention

| Scope | Description |
|-------|-------------|
| `transportr:group:senders` | Can initiate transfers |
| `transportr:group:receivers` | Can receive transfers |

The `transportr:` prefix allows coexistence with other apps on a shared IDP.

---

## Security Guarantees

Both planes are stated separately, because they are armed separately.

| Threat | Protection |
|--------|------------|
| Unauthenticated access to the control plane | JWT required when auth enabled |
| Unauthenticated access to the data plane | JWT required when auth enabled; **open in dev mode** |
| A third party streaming into or out of a transfer | The caller's token subject must match the participant that registered |
| Group spoofing | Group from JWT scope only |
| Unauthorized transfer | FlowControl validates permissions |
| Token tampering | Signature verified against IDP |
| Expired tokens | `exp` claim validated |

In dev mode the data plane serves anyone who knows a transfer id: there is no attested identity to
compare, so the participant check does not run. That is the same trust as the rest of dev mode,
where the hub believes the group a client announces.

The participant check is only as fine-grained as the subject the IdP attests. Two processes sharing
one set of client credentials share one subject, and so can take each other's place in a transfer;
they cannot take the other role's place. Issue one IdP client per participant to separate them.

---

## Token Renewal

- The hub records a token's `exp` claim when the client registers (`Connect`).
- `InitTransfer()` refuses a client whose recorded expiry has passed, and so does a transfer the host
  opens.
- The client gets a fresh token when something asks it for one (a new connection, a stream request),
  and only renews it within 5 minutes of expiry (`TokenRefreshThreshold`). A client that is connected
  and idle asks for none.

**Known limitation.** A client that stays connected past its token's lifetime without opening a
transfer is refused by `InitTransfer()` ("Source client token has expired") until it reconnects, which
registers it again with a fresh token. The client sends no token over an established connection, so
nothing renews the recorded expiry there, and nothing closes a connection whose token has expired: its
streams and its registration stay up, and only new transfers are refused. Keep the token lifetime
longer than the time a client can sit idle, or have an idle client reconnect.

---

## Development vs Production Mode

```mermaid
flowchart LR
    subgraph "Dev Mode"
        A1[Client] --> B1[Hub]
        B1 --> C1[Trust client group]
    end

    subgraph "Production Mode"
        A2[Client + JWT] --> B2[Hub]
        B2 --> D2{Valid?}
        D2 -->|No| E2[401]
        D2 -->|Yes| F2[Use JWT group]
    end
```

| Variable | Effect |
|----------|--------|
| `EnableAuth=false` | Dev mode (default) |
| `EnableAuth=true` | JWT enforced |

---

## TestIdp (Development)

Minimal IDP in `samples/TestIdp` for local testing:

| ClientId | Secret | Scope |
|----------|--------|-------|
| `sender` | `sender-secret` | `transportr:group:senders` |
| `receiver` | `receiver-secret` | `transportr:group:receivers` |

Run: `dotnet run --project samples/TestIdp`

---

## Using External IDP

1. Register scopes: `transportr:group:senders`, `transportr:group:receivers`
2. Create clients with appropriate scopes
3. Configure:
   ```bash
   export EnableAuth=true
   export Oidc__Authority=https://your-idp.example.com
   ```

---

## Benchmark Scripts

| Command | Auth |
|---------|------|
| `./scripts/run-benchmark.sh` | ❌ |
| `./scripts/run-benchmark.sh oidc` | ✅ |
