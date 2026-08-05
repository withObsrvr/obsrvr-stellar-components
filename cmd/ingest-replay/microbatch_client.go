package main

import (
	"context"
	"fmt"
	"time"

	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"github.com/withObsrvr/obsrvr-stellar-components/internal/ingestbatch"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type microBatchSender interface {
	Send(context.Context, []*componentsv1.LedgerBatch) (*componentsv1.IngestMicroBatchAck, ingestbatch.Descriptor, time.Duration, error)
	Close() error
}

type grpcMicroBatchSender struct {
	conn   *grpc.ClientConn
	stream componentsv1.BronzeIngestService_IngestLedgerMicroBatchesClient
	cancel context.CancelFunc
}

func newGRPCMicroBatchSender(ctx context.Context, endpoint, token string) (*grpcMicroBatchSender, error) {
	conn, err := grpc.NewClient(
		endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(64*1024*1024),
			grpc.MaxCallRecvMsgSize(64*1024*1024),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("configure micro-batch ingest client: %w", err)
	}
	streamContext, cancel := context.WithCancel(metadata.AppendToOutgoingContext(ctx, ingestTokenMetadataKey, token))
	stream, err := componentsv1.NewBronzeIngestServiceClient(conn).IngestLedgerMicroBatches(streamContext)
	if err != nil {
		cancel()
		_ = conn.Close()
		return nil, fmt.Errorf("open micro-batch ingest stream: %w", err)
	}
	return &grpcMicroBatchSender{conn: conn, stream: stream, cancel: cancel}, nil
}

func (s *grpcMicroBatchSender) Send(ctx context.Context, batches []*componentsv1.LedgerBatch) (*componentsv1.IngestMicroBatchAck, ingestbatch.Descriptor, time.Duration, error) {
	descriptor, err := ingestbatch.Describe(batches)
	if err != nil {
		return nil, ingestbatch.Descriptor{}, 0, err
	}
	start := time.Now()
	type receiveResult struct {
		ack *componentsv1.IngestMicroBatchAck
		err error
	}
	received := make(chan receiveResult, 1)
	go func() {
		begin := &componentsv1.IngestMicroBatchBegin{
			MicroBatchId:  descriptor.ID,
			LedgerStart:   descriptor.LedgerStart,
			LedgerEnd:     descriptor.LedgerEnd,
			LedgerCount:   descriptor.LedgerCount,
			EncodedBytes:  descriptor.EncodedBytes,
			BronzeRows:    descriptor.BronzeRows,
			PayloadSha256: descriptor.PayloadSHA256,
		}
		if err := s.stream.Send(&componentsv1.IngestMicroBatchRequest{Payload: &componentsv1.IngestMicroBatchRequest_Begin{Begin: begin}}); err != nil {
			received <- receiveResult{err: fmt.Errorf("send micro-batch begin %d-%d: %w", descriptor.LedgerStart, descriptor.LedgerEnd, err)}
			return
		}
		for _, batch := range batches {
			if err := s.stream.Send(&componentsv1.IngestMicroBatchRequest{Payload: &componentsv1.IngestMicroBatchRequest_Batch{Batch: batch}}); err != nil {
				received <- receiveResult{err: fmt.Errorf("send micro-batch ledger %d: %w", batch.LedgerSequence, err)}
				return
			}
		}
		if err := s.stream.Send(&componentsv1.IngestMicroBatchRequest{Payload: &componentsv1.IngestMicroBatchRequest_Commit{Commit: &componentsv1.IngestMicroBatchCommit{}}}); err != nil {
			received <- receiveResult{err: fmt.Errorf("commit micro-batch %d-%d: %w", descriptor.LedgerStart, descriptor.LedgerEnd, err)}
			return
		}
		ack, err := s.stream.Recv()
		received <- receiveResult{ack: ack, err: err}
	}()

	select {
	case <-ctx.Done():
		s.cancel()
		return nil, descriptor, time.Since(start), fmt.Errorf("ack micro-batch %d-%d: %w", descriptor.LedgerStart, descriptor.LedgerEnd, ctx.Err())
	case result := <-received:
		latency := time.Since(start)
		if result.err != nil {
			return nil, descriptor, latency, fmt.Errorf("micro-batch %d-%d round trip: %w", descriptor.LedgerStart, descriptor.LedgerEnd, result.err)
		}
		return result.ack, descriptor, latency, nil
	}
}

func (s *grpcMicroBatchSender) Close() error {
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
