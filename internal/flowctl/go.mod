module github.com/withobsrvr/obsrvr-stellar-components/internal/flowctl

go 1.23

require (
	github.com/golang/protobuf v1.5.4
	github.com/rs/zerolog v1.34.0
	github.com/withobsrvr/obsrvr-stellar-components/proto/gen v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.65.0
	google.golang.org/protobuf v1.34.2
)

require (
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	golang.org/x/net v0.25.0 // indirect
	golang.org/x/sys v0.20.0 // indirect
	golang.org/x/text v0.15.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240528184218-531527333157 // indirect
)

replace github.com/withobsrvr/obsrvr-stellar-components/proto/gen => ../../proto/gen
