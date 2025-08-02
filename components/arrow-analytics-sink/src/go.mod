module github.com/withobsrvr/obsrvr-stellar-components/components/arrow-analytics-sink

go 1.23

require (
	github.com/apache/arrow/go/v17 v17.0.0
	github.com/stellar/go v0.0.0-20241201193725-e3081dbdc0b7
	google.golang.org/grpc v1.65.0
	github.com/prometheus/client_golang v1.19.1
	github.com/gorilla/mux v1.8.1
	github.com/gorilla/websocket v1.5.3
	github.com/rs/zerolog v1.33.0
	github.com/spf13/viper v1.19.0
)

replace github.com/withobsrvr/obsrvr-stellar-components/schemas => ../../../schemas