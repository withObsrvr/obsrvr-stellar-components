module github.com/withobsrvr/obsrvr-stellar-components/test

go 1.23

require (
	github.com/apache/arrow/go/v17 v17.0.0
	github.com/stretchr/testify v1.9.0
	github.com/testcontainers/testcontainers-go v0.28.0
	github.com/gorilla/websocket v1.5.3
)

replace github.com/withobsrvr/obsrvr-stellar-components/schemas => ../schemas