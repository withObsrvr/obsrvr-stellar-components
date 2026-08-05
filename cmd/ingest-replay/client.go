package main

import (
	"context"
	"fmt"
	"time"

	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const ingestTokenMetadataKey = "x-ingest-token"

type batchSender interface {
	Send(context.Context, *componentsv1.LedgerBatch) (*componentsv1.IngestLedgerBatchAck, time.Duration, error)
	Close() error
}

type grpcBatchSender struct {
	conn   *grpc.ClientConn
	stream componentsv1.BronzeIngestService_IngestLedgerBatchesClient
	cancel context.CancelFunc
}

func newGRPCBatchSender(ctx context.Context, endpoint, token string) (*grpcBatchSender, error) {
	conn, err := grpc.NewClient(
		endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(64*1024*1024),
			grpc.MaxCallRecvMsgSize(64*1024*1024),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("configure ingest client: %w", err)
	}
	streamContext, cancel := context.WithCancel(metadata.AppendToOutgoingContext(ctx, ingestTokenMetadataKey, token))
	stream, err := componentsv1.NewBronzeIngestServiceClient(conn).IngestLedgerBatches(streamContext)
	if err != nil {
		cancel()
		_ = conn.Close()
		return nil, fmt.Errorf("open ingest stream: %w", err)
	}
	return &grpcBatchSender{conn: conn, stream: stream, cancel: cancel}, nil
}

func (s *grpcBatchSender) Send(ctx context.Context, batch *componentsv1.LedgerBatch) (*componentsv1.IngestLedgerBatchAck, time.Duration, error) {
	start := time.Now()
	type receiveResult struct {
		ack *componentsv1.IngestLedgerBatchAck
		err error
	}
	received := make(chan receiveResult, 1)
	go func() {
		if err := s.stream.Send(&componentsv1.IngestLedgerBatchRequest{Batch: batch}); err != nil {
			received <- receiveResult{err: fmt.Errorf("send ledger %d: %w", batch.LedgerSequence, err)}
			return
		}
		ack, err := s.stream.Recv()
		received <- receiveResult{ack: ack, err: err}
	}()
	select {
	case <-ctx.Done():
		s.cancel()
		return nil, time.Since(start), fmt.Errorf("ack ledger %d: %w", batch.LedgerSequence, ctx.Err())
	case result := <-received:
		latency := time.Since(start)
		if result.err != nil {
			return nil, latency, fmt.Errorf("ledger %d round trip: %w", batch.LedgerSequence, result.err)
		}
		return result.ack, latency, nil
	}
}

func (s *grpcBatchSender) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	if s.stream != nil {
		_ = s.stream.CloseSend()
	}
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}
