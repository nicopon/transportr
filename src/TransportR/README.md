# TransportR

Core library of the TransportR distributed data transfer solution: the few types the client and the hub both read.

## Purpose

This project holds what both ends of a transfer share and neither keeps for itself, in the `TransportR` namespace:
`WireRoutes`, the path segments of the hub's routes; `TokenRenewalResponse`, the answer to a token renewal; `ClientInfo`
and `ConnectionType`, which describe a client and the stream it opens; `WriteTransport`, what carries a write stream; and
`HubRequestFailedException`, raised on the hub by `ITransferInitiator` and on a client by each request it makes of the hub.
The handles and contracts of the client (`Send<T>`, `IBatchSerializer<T>`, `IDataSender<T>`, ...) are in `TransportR.Client`;
those of the application hosting the hub (`IStateStore`, `IFlowControlService`, `ITransferInitiator`, ...) are in
`TransportR.Hub`. It references no ASP.NET Core package and holds no hub service.

## Protocols

The chunk types and the protocol flow are described in [TransportRArchitecture.md](https://github.com/nicopon/TransportR/blob/main/TransportRArchitecture.md).

## Dependencies

None: the core references no package beyond the .NET base library.

