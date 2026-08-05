FROM golang:1.26-bookworm AS builder
WORKDIR /src
COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o /build/ducklake-gatekeeper ./components/ducklake-gatekeeper/cmd/component

FROM gcr.io/distroless/cc-debian12
LABEL org.opencontainers.image.title="ducklake-gatekeeper"
LABEL org.opencontainers.image.version="0.1.0"
LABEL org.opencontainers.image.source="https://github.com/withObsrvr/obsrvr-stellar-components"
COPY --from=builder /build/ducklake-gatekeeper /app/ducklake-gatekeeper
ENTRYPOINT ["/app/ducklake-gatekeeper"]
