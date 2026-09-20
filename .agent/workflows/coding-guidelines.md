---
description: Global development guidelines for TransportR
---

# Coding Guidelines

## 💬 Communication & Feedback

- **Be objective and critical** — avoid flattery or excessive positivity
- Challenge ideas constructively when you see issues
- Acknowledge uncertainty honestly rather than speculating confidently

## 🌍 Language

- All code, comments, and documentation must be written in English

## 📋 Planning

- Present an implementation plan before any major refactoring
- Wait for user validation before executing significant changes

## 🔒 Security

- **Error messages must not expose sensitive information**
  - Do not reveal internal system details (group names, paths, configurations)
  - Use generic error messages for client-facing exceptions
  - Log detailed diagnostic information server-side only

- **When trade-offs exist, present options and let user decide**
  - Document the security implications of each option

## 📐 Architecture & Robustness

- **Favor Strictness over Tolerance**: Design APIs and configurations to be strict by default.
  - Do not silently correct or compensate for "dirty" inputs or legacy configurations in the code.
  - If a tolerance mechanism (e.g., auto-correction, backward compatibility) seems useful, **propose it as an option to the user** instead of implementing it silently.

## 🎯 KISS & Scope Protection

TransportR occupies a **specific niche**: hub-mediated bulk transfer for network-segmented environments. It is NOT a message broker.

- **Alert the user** if a proposed feature pushes TransportR toward:
  - Pub/Sub or fan-out patterns (→ use Kafka/RabbitMQ instead)
  - Hub-side data persistence or replay (→ use Kafka instead)
  - Complex routing or topic-based delivery (→ use a message broker)

- **Multi-language SDKs**: Not excluded, but approach with caution:
  - The protocol is simple enough (30-byte header + opaque payload) to port
  - However, the resilience logic (reconnection, Ping/Pong, checkpoint recovery) is subtle
  - Maintenance burden multiplies with each language (bug parity, cross-language testing)
  - **Ideal path**: Leave for external contributors if the project gains adoption
  - If proposing a new SDK, flag the long-term maintenance implications

- **Reject over-engineering**:
  - Question any addition that increases infrastructure requirements significantly
  - Prefer simple solutions over "enterprise-grade" abstractions
  - If a feature requires more than 2-3 new interfaces, challenge its necessity

- **Preserve core simplicity**:
  - Dumb-pipe architecture (Hub forwards bytes, doesn't deserialize)
  - Point-to-point only (one sender → one receiver per transfer)
  - ACK-based reliability without persistence
  - Minimal infrastructure (single .NET app + optional Redis)

## 📝 Documentation

- No generation dates in documentation