package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	flowctlv1 "github.com/withObsrvr/flow-proto/go/gen/flowctl/v1"
	"github.com/withObsrvr/flowctl-sdk/pkg/consumer"
	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/contracts"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func main() {
	path := getenv("JSONL_PATH", contracts.DefaultJSONLPath)
	writer := &jsonlWriter{path: path}

	consumer.Run(consumer.ConsumerConfig{
		ConsumerName: "Stellar Ledger JSONL Sink",
		ComponentID:  getenv("COMPONENT_ID", "jsonl-sink"),
		InputTypes:   []string{contracts.LedgerBatchEventType},
		OnEvent: func(ctx context.Context, event *flowctlv1.Event) error {
			if event.Type != contracts.LedgerBatchEventType {
				return nil
			}
			var batch componentsv1.LedgerBatch
			if err := proto.Unmarshal(event.Payload, &batch); err != nil {
				return fmt.Errorf("unmarshal ledger batch: %w", err)
			}
			data, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(&batch)
			if err != nil {
				return fmt.Errorf("marshal ledger batch json: %w", err)
			}
			return writer.Append(data)
		},
	})
}

type jsonlWriter struct {
	path string
	mu   sync.Mutex
}

func (w *jsonlWriter) Append(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(w.path), 0o755); err != nil && filepath.Dir(w.path) != "." {
		return err
	}
	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Write(append(data, '\n')); err != nil {
		return err
	}
	return nil
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
