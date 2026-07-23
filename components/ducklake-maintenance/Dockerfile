FROM golang:1.26-bookworm AS builder
WORKDIR /src
COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o /build/ducklake-maintenance ./components/ducklake-maintenance/cmd/component

FROM gcr.io/distroless/base-debian12
LABEL org.opencontainers.image.title="ducklake-maintenance"
LABEL org.opencontainers.image.version="0.1.0"
LABEL org.opencontainers.image.source="https://github.com/withObsrvr/obsrvr-stellar-components"
COPY --from=builder /build/ducklake-maintenance /app/ducklake-maintenance
EXPOSE 8090
ENTRYPOINT ["/app/ducklake-maintenance"]
