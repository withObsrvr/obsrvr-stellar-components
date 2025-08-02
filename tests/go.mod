module github.com/withobsrvr/obsrvr-stellar-components/tests

go 1.23

require (
	github.com/apache/arrow/go/v17 v17.0.0
	github.com/rs/zerolog v1.33.0
	github.com/stretchr/testify v1.9.0
	github.com/withobsrvr/obsrvr-stellar-components/schemas v0.0.0
)

replace github.com/withobsrvr/obsrvr-stellar-components/schemas => ../schemas