package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"time"

	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"github.com/withObsrvr/obsrvr-stellar-components/internal/ingestbatch"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/bronze"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type microBatchReceipt struct {
	network       string
	microBatchID  string
	ledgerStart   uint32
	ledgerEnd     uint32
	ledgerCount   uint32
	payloadSHA256 string
}

func (s *ingestServer) IngestLedgerMicroBatches(stream componentsv1.BronzeIngestService_IngestLedgerMicroBatchesServer) error {
	if err := s.authorize(stream.Context()); err != nil {
		return err
	}
	if s.profile != "backfill" {
		return status.Error(codes.FailedPrecondition, "backfill ingest requires INGEST_PROFILE=backfill")
	}
	if !s.streamMu.TryLock() {
		return status.Error(codes.ResourceExhausted, "another ingest stream is active")
	}
	defer s.streamMu.Unlock()

	for {
		request, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		begin := request.GetBegin()
		if begin == nil {
			return status.Error(codes.InvalidArgument, "micro-batch frame must begin with begin")
		}
		if err := s.validateMicroBatchBegin(begin); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}

		batches, err := s.receiveMicroBatchFrame(stream, begin)
		if err != nil {
			return err
		}
		descriptor, err := ingestbatch.Describe(batches)
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		if err := validateMicroBatchDescriptor(begin, descriptor); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}

		start := time.Now()
		ack, err := s.commitMicroBatch(stream.Context(), batches, descriptor)
		if err != nil {
			return status.Errorf(codes.Internal, "ingest micro-batch %s: %v", descriptor.ID, err)
		}
		if err := stream.Send(ack); err != nil {
			return err
		}
		log.Printf("backfill committed ledgers %d-%d count=%d bytes=%d rows=%d in %s (replayed=%t deduplicated=%t)",
			descriptor.LedgerStart,
			descriptor.LedgerEnd,
			descriptor.LedgerCount,
			descriptor.EncodedBytes,
			descriptor.BronzeRows,
			time.Since(start).Round(time.Millisecond),
			ack.Replayed,
			ack.Deduplicated,
		)
	}
}

func (s *ingestServer) validateMicroBatchBegin(begin *componentsv1.IngestMicroBatchBegin) error {
	if begin.MicroBatchId == "" || begin.PayloadSha256 == "" {
		return fmt.Errorf("micro-batch id and payload sha256 are required")
	}
	if begin.LedgerEnd < begin.LedgerStart {
		return fmt.Errorf("micro-batch end %d precedes start %d", begin.LedgerEnd, begin.LedgerStart)
	}
	wantCount := uint64(begin.LedgerEnd) - uint64(begin.LedgerStart) + 1
	if uint64(begin.LedgerCount) != wantCount {
		return fmt.Errorf("micro-batch declared count %d does not cover %d-%d", begin.LedgerCount, begin.LedgerStart, begin.LedgerEnd)
	}
	if begin.LedgerCount == 0 || int(begin.LedgerCount) > s.backfillMaxLedgers {
		return fmt.Errorf("micro-batch ledger count %d exceeds limit %d", begin.LedgerCount, s.backfillMaxLedgers)
	}
	if begin.EncodedBytes == 0 || begin.EncodedBytes > s.backfillMaxEncodedBytes {
		return fmt.Errorf("micro-batch encoded bytes %d exceeds limit %d", begin.EncodedBytes, s.backfillMaxEncodedBytes)
	}
	if begin.BronzeRows > s.backfillMaxBronzeRows {
		return fmt.Errorf("micro-batch bronze rows %d exceeds limit %d", begin.BronzeRows, s.backfillMaxBronzeRows)
	}
	return nil
}

func (s *ingestServer) receiveMicroBatchFrame(stream componentsv1.BronzeIngestService_IngestLedgerMicroBatchesServer, begin *componentsv1.IngestMicroBatchBegin) ([]*componentsv1.LedgerBatch, error) {
	batches := make([]*componentsv1.LedgerBatch, 0, begin.LedgerCount)
	var encodedBytes uint64
	var bronzeRows uint64
	for {
		request, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil, status.Error(codes.InvalidArgument, "micro-batch stream ended before commit")
		}
		if err != nil {
			return nil, err
		}
		if batch := request.GetBatch(); batch != nil {
			batches = append(batches, batch)
			encodedBytes += uint64(proto.Size(batch))
			bronzeRows += uint64(len(batch.BronzeRows))
			if len(batches) > s.backfillMaxLedgers || encodedBytes > s.backfillMaxEncodedBytes || bronzeRows > s.backfillMaxBronzeRows {
				return nil, status.Error(codes.ResourceExhausted, "micro-batch exceeded a server resource limit")
			}
			if len(batches) > int(begin.LedgerCount) {
				return nil, status.Error(codes.InvalidArgument, "micro-batch contains more ledgers than declared")
			}
			continue
		}
		if request.GetCommit() != nil {
			return batches, nil
		}
		return nil, status.Error(codes.InvalidArgument, "micro-batch frame contains an unexpected begin")
	}
}

func validateMicroBatchDescriptor(begin *componentsv1.IngestMicroBatchBegin, descriptor ingestbatch.Descriptor) error {
	if begin.MicroBatchId != descriptor.ID || begin.PayloadSha256 != descriptor.PayloadSHA256 {
		return fmt.Errorf("micro-batch digest does not match payload")
	}
	if begin.LedgerStart != descriptor.LedgerStart || begin.LedgerEnd != descriptor.LedgerEnd || begin.LedgerCount != descriptor.LedgerCount {
		return fmt.Errorf("micro-batch declared range does not match payload")
	}
	if begin.EncodedBytes != descriptor.EncodedBytes || begin.BronzeRows != descriptor.BronzeRows {
		return fmt.Errorf("micro-batch declared sizes do not match payload")
	}
	return nil
}

func (s *ingestServer) commitMicroBatch(ctx context.Context, batches []*componentsv1.LedgerBatch, descriptor ingestbatch.Descriptor) (*componentsv1.IngestMicroBatchAck, error) {
	receipt, found, err := s.readMicroBatchReceipt(ctx, descriptor.ID)
	if err != nil {
		return nil, err
	}
	if found {
		if err := receipt.matches(batches[0].NetworkPassphrase, descriptor); err != nil {
			return nil, err
		}
		if descriptor.LedgerEnd > s.highWatermark {
			s.highWatermark = descriptor.LedgerEnd
		}
		return microBatchAck(descriptor, false, true), nil
	}
	if s.highWatermark != 0 && descriptor.LedgerStart != s.highWatermark+1 {
		return nil, fmt.Errorf("new micro-batch starts at %d, want high watermark %d plus one", descriptor.LedgerStart, s.highWatermark)
	}

	decodeStart := time.Now()
	decoded := bronze.DecodeTypedRowsBatches(batches, s.backfillDecodeWorkers)
	specs := make(map[string]bronze.TypedTableSpec)
	for _, row := range decoded {
		if row.Err != nil {
			return nil, fmt.Errorf("decode typed rows for micro-batch %d-%d: %w", descriptor.LedgerStart, descriptor.LedgerEnd, row.Err)
		}
		if row.OK {
			specs[row.Spec.TableName] = row.Spec
		}
	}
	decodeDuration := time.Since(decodeStart)

	replay := s.forceReplay || descriptor.LedgerStart <= s.highWatermark
	var commitErr error
	var phases ingestPhaseDurations
	for attempt := 1; attempt <= 2; attempt++ {
		phases, commitErr = s.tryCommitMicroBatch(ctx, batches, descriptor, decoded, specs, replay)
		phases.decode = decodeDuration
		if commitErr == nil {
			s.highWatermark = descriptor.LedgerEnd
			s.forceReplay = false
			if s.metrics != nil {
				s.metrics.ingestLastLedger.Set(float64(descriptor.LedgerEnd))
			}
			log.Printf("backfill phases ledgers %d-%d: decode %s, staging %s, preface %s, transfer %s, commit %s, cleanup %s",
				descriptor.LedgerStart,
				descriptor.LedgerEnd,
				phases.decode.Round(time.Millisecond),
				phases.staging.Round(time.Millisecond),
				phases.preface.Round(time.Millisecond),
				phases.transfer.Round(time.Millisecond),
				phases.commit.Round(time.Millisecond),
				phases.cleanup.Round(time.Millisecond),
			)
			return microBatchAck(descriptor, replay, false), nil
		}
		log.Printf("backfill ledgers %d-%d attempt %d failed: %v", descriptor.LedgerStart, descriptor.LedgerEnd, attempt, commitErr)
		replay = true
	}
	s.forceReplay = true
	return nil, commitErr
}

func (s *ingestServer) tryCommitMicroBatch(ctx context.Context, batches []*componentsv1.LedgerBatch, descriptor ingestbatch.Descriptor, decoded []bronze.DecodedRow, specs map[string]bronze.TypedTableSpec, replay bool) (ingestPhaseDurations, error) {
	s.coordinator.Lock()
	defer func() {
		s.coordinator.MarkIngestDone(time.Now())
		s.coordinator.Unlock()
	}()

	var phases ingestPhaseDurations
	stagingStart := time.Now()
	if err := s.clearStaging(ctx, specs); err != nil {
		phases.staging = time.Since(stagingStart)
		return phases, err
	}
	if err := s.stageWithAppender(decoded); err != nil {
		phases.staging = time.Since(stagingStart)
		return phases, err
	}
	phases.staging = time.Since(stagingStart)

	prefaceStart := time.Now()
	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		phases.preface = time.Since(prefaceStart)
		return phases, fmt.Errorf("begin micro-batch transaction: %w", err)
	}
	defer tx.Rollback()
	if err := bronze.EnsureCatalogNetworkTx(tx, batches[0].NetworkPassphrase); err != nil {
		phases.preface = time.Since(prefaceStart)
		return phases, err
	}
	if replay {
		if err := bronze.DeleteLedgerRangeRowsTx(tx, batches[0].NetworkPassphrase, descriptor.LedgerStart, descriptor.LedgerEnd); err != nil {
			phases.preface = time.Since(prefaceStart)
			return phases, err
		}
	}
	for _, batch := range batches {
		if err := bronze.InsertLedgerBatchRowTx(tx, batch); err != nil {
			phases.preface = time.Since(prefaceStart)
			return phases, err
		}
		if err := bronze.InsertWatermarkTx(tx, batch); err != nil {
			phases.preface = time.Since(prefaceStart)
			return phases, err
		}
	}
	phases.preface = time.Since(prefaceStart)

	transferStart := time.Now()
	tableNames := make([]string, 0, len(specs))
	for tableName := range specs {
		tableNames = append(tableNames, tableName)
	}
	sort.Strings(tableNames)
	for _, tableName := range tableNames {
		spec := specs[tableName]
		columns := make([]string, len(spec.Columns))
		for index, column := range spec.Columns {
			columns[index] = bronze.QuoteIdentifier(column)
		}
		columnList := strings.Join(columns, ", ")
		if _, err := tx.Exec(fmt.Sprintf(
			"INSERT INTO bronze.%s (%s) SELECT %s FROM memory.bronze.%s",
			tableName,
			columnList,
			columnList,
			tableName,
		)); err != nil {
			phases.transfer = time.Since(transferStart)
			return phases, fmt.Errorf("transfer staged micro-batch rows for %s: %w", tableName, err)
		}
	}
	if err := bronze.InsertMicroBatchReceiptTx(
		tx,
		batches[0].NetworkPassphrase,
		descriptor.ID,
		descriptor.LedgerStart,
		descriptor.LedgerEnd,
		descriptor.LedgerCount,
		descriptor.PayloadSHA256,
	); err != nil {
		phases.transfer = time.Since(transferStart)
		return phases, err
	}
	phases.transfer = time.Since(transferStart)

	commitStart := time.Now()
	if err := tx.Commit(); err != nil {
		phases.commit = time.Since(commitStart)
		return phases, fmt.Errorf("commit micro-batch transaction: %w", err)
	}
	phases.commit = time.Since(commitStart)

	cleanupStart := time.Now()
	if err := s.clearStaging(ctx, specs); err != nil {
		log.Printf("staging cleanup after micro-batch %d-%d: %v", descriptor.LedgerStart, descriptor.LedgerEnd, err)
	}
	phases.cleanup = time.Since(cleanupStart)
	return phases, nil
}

func (s *ingestServer) readMicroBatchReceipt(ctx context.Context, id string) (microBatchReceipt, bool, error) {
	rows, err := s.conn.QueryContext(ctx, `SELECT
		network_passphrase,
		micro_batch_id,
		ledger_start,
		ledger_end,
		ledger_count,
		payload_sha256
	FROM bronze.ingest_microbatch_commits
	WHERE micro_batch_id = ?`, id)
	if err != nil {
		return microBatchReceipt{}, false, fmt.Errorf("read micro-batch receipt: %w", err)
	}
	defer rows.Close()
	var receipt microBatchReceipt
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return microBatchReceipt{}, false, fmt.Errorf("read micro-batch receipt: %w", err)
		}
		return microBatchReceipt{}, false, nil
	}
	if err := rows.Scan(&receipt.network, &receipt.microBatchID, &receipt.ledgerStart, &receipt.ledgerEnd, &receipt.ledgerCount, &receipt.payloadSHA256); err != nil {
		return microBatchReceipt{}, false, fmt.Errorf("scan micro-batch receipt: %w", err)
	}
	if rows.Next() {
		return microBatchReceipt{}, false, fmt.Errorf("micro-batch receipt %s is duplicated", id)
	}
	if err := rows.Err(); err != nil {
		return microBatchReceipt{}, false, fmt.Errorf("read micro-batch receipt: %w", err)
	}
	return receipt, true, nil
}

func (r microBatchReceipt) matches(network string, descriptor ingestbatch.Descriptor) error {
	if r.network != network || r.microBatchID != descriptor.ID || r.ledgerStart != descriptor.LedgerStart || r.ledgerEnd != descriptor.LedgerEnd || r.ledgerCount != descriptor.LedgerCount || r.payloadSHA256 != descriptor.PayloadSHA256 {
		return fmt.Errorf("micro-batch receipt %s conflicts with retry payload", descriptor.ID)
	}
	return nil
}

func microBatchAck(descriptor ingestbatch.Descriptor, replayed, deduplicated bool) *componentsv1.IngestMicroBatchAck {
	return &componentsv1.IngestMicroBatchAck{
		MicroBatchId: descriptor.ID,
		LedgerStart:  descriptor.LedgerStart,
		LedgerEnd:    descriptor.LedgerEnd,
		LedgerCount:  descriptor.LedgerCount,
		Replayed:     replayed,
		Deduplicated: deduplicated,
	}
}
