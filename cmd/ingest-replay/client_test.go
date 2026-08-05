package main

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestGRPCBatchSenderAuthenticatesAndMatchesProtocol(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	service := &echoIngestService{token: "fixture-token"}
	componentsv1.RegisterBronzeIngestServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	sender, err := newGRPCBatchSender(context.Background(), listener.Addr().String(), "fixture-token")
	if err != nil {
		t.Fatalf("new sender: %v", err)
	}
	t.Cleanup(func() { _ = sender.Close() })
	ackContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ack, latency, err := sender.Send(ackContext, &componentsv1.LedgerBatch{LedgerSequence: 123})
	if err != nil {
		t.Fatalf("send batch: %v", err)
	}
	if ack.LedgerSequence != 123 || !ack.Replayed {
		t.Fatalf("ack = %+v", ack)
	}
	if latency <= 0 {
		t.Fatalf("latency = %s, want positive", latency)
	}
}

type echoIngestService struct {
	componentsv1.UnimplementedBronzeIngestServiceServer
	token string
}

func (s *echoIngestService) IngestLedgerBatches(stream componentsv1.BronzeIngestService_IngestLedgerBatchesServer) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	if tokens := md.Get(ingestTokenMetadataKey); len(tokens) != 1 || tokens[0] != s.token {
		return context.Canceled
	}
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&componentsv1.IngestLedgerBatchAck{
			LedgerSequence: request.Batch.LedgerSequence,
			Replayed:       true,
		}); err != nil {
			return err
		}
	}
}
