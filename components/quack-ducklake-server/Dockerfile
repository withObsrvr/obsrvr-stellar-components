FROM golang:1.26-bookworm AS builder
WORKDIR /src
COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o /build/quack-ducklake-server ./components/quack-ducklake-server/cmd/component

FROM gcr.io/distroless/base-debian12
LABEL org.opencontainers.image.title="quack-ducklake-server"
LABEL org.opencontainers.image.version="0.1.0"
LABEL org.opencontainers.image.source="https://github.com/withObsrvr/obsrvr-stellar-components"
COPY --from=builder /build/quack-ducklake-server /app/quack-ducklake-server
EXPOSE 9494 8088 9495
ENTRYPOINT ["/app/quack-ducklake-server"]
