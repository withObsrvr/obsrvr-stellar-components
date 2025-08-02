module github.com/withobsrvr/obsrvr-stellar-components/components/stellar-arrow-source

go 1.23

require (
	github.com/apache/arrow/go/v17 v17.0.0
	github.com/stellar/go v0.0.0-20241201193725-e3081dbdc0b7
	google.golang.org/grpc v1.65.0
	github.com/prometheus/client_golang v1.19.1
	github.com/gorilla/mux v1.8.1
	github.com/rs/zerolog v1.33.0
	github.com/spf13/viper v1.19.0
	github.com/aws/aws-sdk-go-v2 v1.30.3
	github.com/aws/aws-sdk-go-v2/service/s3 v1.58.2
	cloud.google.com/go/storage v1.43.0
)

replace github.com/withobsrvr/obsrvr-stellar-components/schemas => ../../../schemas