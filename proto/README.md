# Protocol Buffer Definitions

This directory contains protocol buffer definitions used by the obsrvr-stellar-components project.

## flowctl/

Contains the flowctl protocol buffer definitions copied from the ttp-processor-demo project.

### Core Services

- **ControlPlane** (`control_plane.proto`): Service registration, heartbeats, and status management
- **LedgerSource** (`source.proto`): Stellar ledger data streaming
- **EventProcessor** (`processor.proto`): Event processing pipeline
- **EventSink** (`sink.proto`): Event output and storage

### Key Messages

- **ServiceInfo**: Service registration information
- **ServiceHeartbeat**: Periodic service status updates
- **RegistrationAck**: Service registration acknowledgment
- **Event**: Generic event envelope for the pipeline
- **RawLedger**: Stellar ledger data container

### Generated Files

The directory includes both `.proto` source files and generated Go code:
- `*.proto` - Protocol buffer source definitions
- `*.pb.go` - Generated protobuf message code
- `*_grpc.pb.go` - Generated gRPC service code

All generated code uses the `flowctlpb` Go package.

## Usage

Import the generated protobuf code in Go:

```go
import "github.com/withobsrvr/flowctl/proto"
// or
import flowctlpb "github.com/withobsrvr/flowctl/proto"
```

## Source

These files were copied from:
`/home/tillman/Documents/ttp-processor-demo/ttp-processor/go/vendor/github.com/withobsrvr/flowctl/proto/`

Import paths in the `.proto` files have been updated to use relative imports within this directory.