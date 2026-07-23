package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
)

// ingestTokenMetadataKey must match quack-ducklake-server's ingest service.
const ingestTokenMetadataKey = "x-ingest-token"

// newIngestRPCSink forwards batches to quack-ducklake-server's bronze ingest
// service. The server commits each ledger in-process; this sink holds no
// local DuckDB at all.
func newIngestRPCSink(cfg DuckLakeConfig) (*DuckLakeSink, error) {
	if cfg.IngestEndpoint == "" {
		return nil, fmt.Errorf("INGEST_ENDPOINT is required when DUCKLAKE_MODE=ingest-rpc")
	}
	if cfg.QuackToken == "" {
		return nil, fmt.Errorf("QUACK_TOKEN is required when DUCKLAKE_MODE=ingest-rpc")
	}
	return &DuckLakeSink{
		ingestMode:     true,
		ingestEndpoint: cfg.IngestEndpoint,
		ingestToken:    cfg.QuackToken,
	}, nil
}

func (s *DuckLakeSink) ensureIngestStream() (componentsv1.BronzeIngestService_IngestLedgerBatchesClient, error) {
	if s.ingestStream != nil {
		return s.ingestStream, nil
	}
	if s.ingestConn == nil {
		conn, err := grpc.NewClient(
			s.ingestEndpoint,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(64*1024*1024)),
		)
		if err != nil {
			return nil, fmt.Errorf("dial ingest endpoint %s: %w", s.ingestEndpoint, err)
		}
		s.ingestConn = conn
	}
	client := componentsv1.NewBronzeIngestServiceClient(s.ingestConn)
	streamCtx := metadata.AppendToOutgoingContext(context.Background(), ingestTokenMetadataKey, s.ingestToken)
	stream, err := client.IngestLedgerBatches(streamCtx)
	if err != nil {
		return nil, fmt.Errorf("open ingest stream: %w", err)
	}
	s.ingestStream = stream
	return stream, nil
}

// resetIngestStream drops the stream after any error so the next attempt
// starts fresh; the server treats post-error batches as replays.
func (s *DuckLakeSink) resetIngestStream() {
	if s.ingestStream != nil {
		_ = s.ingestStream.CloseSend()
		s.ingestStream = nil
	}
}

func (s *DuckLakeSink) writeBatchIngest(batch *componentsv1.LedgerBatch) error {
	stream, err := s.ensureIngestStream()
	if err != nil {
		return err
	}
	start := time.Now()
	if err := stream.Send(&componentsv1.IngestLedgerBatchRequest{Batch: batch}); err != nil {
		s.resetIngestStream()
		return fmt.Errorf("send ledger %d to ingest service: %w", batch.LedgerSequence, err)
	}
	ack, err := stream.Recv()
	if err != nil {
		s.resetIngestStream()
		return fmt.Errorf("ingest ack for ledger %d: %w", batch.LedgerSequence, err)
	}
	if ack.LedgerSequence != batch.LedgerSequence {
		s.resetIngestStream()
		return fmt.Errorf("ingest ack mismatch: sent %d, acked %d", batch.LedgerSequence, ack.LedgerSequence)
	}
	log.Printf("ingest-rpc committed ledger %d in %s (replayed=%t)",
		batch.LedgerSequence, time.Since(start).Round(time.Millisecond), ack.Replayed)
	return nil
}

func (s *DuckLakeSink) closeIngest() {
	s.resetIngestStream()
	if s.ingestConn != nil {
		_ = s.ingestConn.Close()
		s.ingestConn = nil
	}
}
