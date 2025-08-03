module github.com/withobsrvr/obsrvr-stellar-components/components/arrow-analytics-sink

go 1.23

require (
	github.com/apache/arrow/go/v17 v17.0.0
	github.com/gorilla/mux v1.8.1
	github.com/gorilla/websocket v1.5.3
	github.com/prometheus/client_golang v1.19.1
	github.com/rs/zerolog v1.33.0
	github.com/spf13/viper v1.19.0
	github.com/withobsrvr/obsrvr-stellar-components/internal/config v0.0.0-00010101000000-000000000000
	github.com/withobsrvr/obsrvr-stellar-components/internal/flowctl v0.0.0-00010101000000-000000000000
	github.com/withobsrvr/obsrvr-stellar-components/proto/gen v0.0.0-00010101000000-000000000000
	github.com/withobsrvr/obsrvr-stellar-components/schemas v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.65.0
)

require (
	github.com/JohnCGriffin/overflow v0.0.0-20211019200055-46fa312c352c // indirect
	github.com/andybalholm/brotli v1.1.0 // indirect
	github.com/apache/thrift v0.20.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/goccy/go-json v0.10.4 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/flatbuffers v24.3.25+incompatible // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/klauspost/asmfmt v1.3.2 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/klauspost/cpuid/v2 v2.2.8 // indirect
	github.com/magiconair/properties v1.8.7 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/minio/asm2plan9s v0.0.0-20200509001527-cdd76441f9d8 // indirect
	github.com/minio/c2goasm v0.0.0-20190812172519-36a3d3bbc4f3 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/pelletier/go-toml/v2 v2.2.2 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_model v0.5.0 // indirect
	github.com/prometheus/common v0.48.0 // indirect
	github.com/prometheus/procfs v0.12.0 // indirect
	github.com/sagikazarmark/locafero v0.4.0 // indirect
	github.com/sagikazarmark/slog-shim v0.1.0 // indirect
	github.com/sourcegraph/conc v0.3.0 // indirect
	github.com/spf13/afero v1.11.0 // indirect
	github.com/spf13/cast v1.6.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stellar/go v0.0.0-20250731191627-f4dc82059f0e // indirect
	github.com/stellar/go-xdr v0.0.0-20231122183749-b53fb00bcac2 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/exp v0.0.0-20241217172543-b2144cdd0a67 // indirect
	golang.org/x/mod v0.22.0 // indirect
	golang.org/x/net v0.32.0 // indirect
	golang.org/x/sync v0.10.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	golang.org/x/tools v0.28.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240701130421-f6361c86f094 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/withobsrvr/obsrvr-stellar-components/schemas => ../../../schemas

replace github.com/withobsrvr/obsrvr-stellar-components/internal/config => ../../../internal/config

replace github.com/withobsrvr/obsrvr-stellar-components/internal/flowctl => ../../../internal/flowctl

replace github.com/withobsrvr/obsrvr-stellar-components/proto/gen => ../../../proto/gen

replace github.com/stellar/go => github.com/stellar/go v0.0.0-20250731191627-f4dc82059f0e
