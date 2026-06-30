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
	sink := &partitionSink{root: getenv("DUCKDB_EXPORT_DIR", contracts.DefaultDuckDBJSONLDir)}
	if err := sink.WriteSchema(); err != nil {
		panic(err)
	}

	consumer.Run(consumer.ConsumerConfig{
		ConsumerName: "Stellar Ledger DuckDB Sink",
		ComponentID:  getenv("COMPONENT_ID", "ducklake-sink"),
		InputTypes:   []string{contracts.LedgerBatchEventType},
		OnEvent: func(ctx context.Context, event *flowctlv1.Event) error {
			if event.Type != contracts.LedgerBatchEventType {
				return nil
			}
			var batch componentsv1.LedgerBatch
			if err := proto.Unmarshal(event.Payload, &batch); err != nil {
				return fmt.Errorf("unmarshal ledger batch: %w", err)
			}
			return sink.WriteBatch(&batch)
		},
	})
}

type partitionSink struct {
	root string
	mu   sync.Mutex
}

func (s *partitionSink) WriteSchema() error {
	if err := os.MkdirAll(s.root, 0o755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(s.root, "schema.sql"), []byte(duckDBSchemaSQL), 0o644)
}

func (s *partitionSink) WriteBatch(batch *componentsv1.LedgerBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	partition := filepath.Join(
		s.root,
		fmt.Sprintf("network=%s", sanitizePartition(batch.NetworkPassphrase)),
		fmt.Sprintf("ledger_range=%010d", (batch.LedgerSequence/1000)*1000),
	)
	if err := os.MkdirAll(partition, 0o755); err != nil {
		return err
	}
	data, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(batch)
	if err != nil {
		return err
	}
	path := filepath.Join(partition, fmt.Sprintf("ledger_%010d.jsonl", batch.LedgerSequence))
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

func sanitizePartition(value string) string {
	if value == "" {
		return "unknown"
	}
	out := make([]rune, 0, len(value))
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			out = append(out, r)
		} else {
			out = append(out, '_')
		}
	}
	return string(out)
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

const duckDBSchemaSQL = `
create table if not exists ledger_batches as
select *
from read_json_auto('network=*/ledger_range=*/ledger_*.jsonl');
`
