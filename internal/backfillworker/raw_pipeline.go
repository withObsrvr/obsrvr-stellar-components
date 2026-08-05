package backfillworker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

type rawDecodePipelineConfig struct {
	Workers     int
	MaxInFlight int
}

type rawDecodePipelineMetrics struct {
	SourceDuration      time.Duration
	CopyDuration        time.Duration
	WaitDuration        time.Duration
	CopiedBytes         uint64
	PeakInFlight        int
	PeakReorderBuffered int
}

type rawLedgerDecoder func([]byte, RawLedgerOptions) (*RawLedger, error)

type rawDecodeJob struct {
	ordinal uint64
	raw     []byte
}

type rawDecodeResult struct {
	ordinal   uint64
	ledger    *RawLedger
	err       error
	holdsSlot bool
}

type rawDecodePipeline struct {
	ctx     context.Context
	cancel  context.CancelFunc
	sem     chan struct{}
	results chan rawDecodeResult
	done    chan struct{}

	pending  map[uint64]rawDecodeResult
	expected uint64
	held     bool

	sourceNanos atomic.Int64
	copyNanos   atomic.Int64
	waitNanos   atomic.Int64
	copiedBytes atomic.Uint64
	inFlight    atomic.Int64
	peakFlight  atomic.Int64
	peakPending atomic.Int64
	closeOnce   sync.Once
}

func newRawDecodePipeline(parent context.Context, cfg rawDecodePipelineConfig, opts RawLedgerOptions, source RawLedgerSource, decoder rawLedgerDecoder) (*rawDecodePipeline, error) {
	if parent == nil {
		return nil, fmt.Errorf("raw decode pipeline context is required")
	}
	if source == nil || decoder == nil {
		return nil, fmt.Errorf("raw decode pipeline source and decoder are required")
	}
	if cfg.Workers <= 0 {
		return nil, fmt.Errorf("raw extract workers must be positive")
	}
	if cfg.MaxInFlight < cfg.Workers {
		return nil, fmt.Errorf("max in-flight ledgers %d must be at least raw extract workers %d", cfg.MaxInFlight, cfg.Workers)
	}

	ctx, cancel := context.WithCancel(parent)
	pipeline := &rawDecodePipeline{
		ctx: ctx, cancel: cancel,
		sem:     make(chan struct{}, cfg.MaxInFlight),
		results: make(chan rawDecodeResult, cfg.MaxInFlight+1),
		done:    make(chan struct{}),
		pending: make(map[uint64]rawDecodeResult, cfg.MaxInFlight),
	}
	jobs := make(chan rawDecodeJob, cfg.Workers)
	var workers sync.WaitGroup
	workers.Add(cfg.Workers + 1)
	go func() {
		defer workers.Done()
		defer close(jobs)
		pipeline.produce(source, jobs)
	}()
	for range cfg.Workers {
		go func() {
			defer workers.Done()
			pipeline.decode(opts, decoder, jobs)
		}()
	}
	go func() {
		workers.Wait()
		close(pipeline.results)
		close(pipeline.done)
	}()
	return pipeline, nil
}

func (pipeline *rawDecodePipeline) produce(source RawLedgerSource, jobs chan<- rawDecodeJob) {
	for ordinal := uint64(0); ; ordinal++ {
		if !pipeline.acquire() {
			return
		}
		started := time.Now()
		raw, err := source()
		pipeline.sourceNanos.Add(int64(time.Since(started)))
		if err != nil {
			pipeline.release()
			pipeline.send(rawDecodeResult{ordinal: ordinal, err: err})
			return
		}
		copyStarted := time.Now()
		owned := bytes.Clone(raw)
		pipeline.copyNanos.Add(int64(time.Since(copyStarted)))
		pipeline.copiedBytes.Add(uint64(len(owned)))
		select {
		case jobs <- rawDecodeJob{ordinal: ordinal, raw: owned}:
		case <-pipeline.ctx.Done():
			pipeline.release()
			return
		}
	}
}

func (pipeline *rawDecodePipeline) decode(opts RawLedgerOptions, decoder rawLedgerDecoder, jobs <-chan rawDecodeJob) {
	for {
		select {
		case <-pipeline.ctx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}
			ledger, err := decoder(job.raw, opts)
			if err != nil {
				err = fmt.Errorf("decode raw source ordinal %d: %w", job.ordinal, err)
			} else if ledger == nil {
				err = fmt.Errorf("decode raw source ordinal %d returned a nil ledger", job.ordinal)
			}
			if !pipeline.send(rawDecodeResult{
				ordinal: job.ordinal, ledger: ledger, err: err, holdsSlot: true,
			}) {
				pipeline.release()
				return
			}
		}
	}
}

func (pipeline *rawDecodePipeline) send(result rawDecodeResult) bool {
	select {
	case pipeline.results <- result:
		return true
	case <-pipeline.ctx.Done():
		return false
	}
}

func (pipeline *rawDecodePipeline) acquire() bool {
	select {
	case pipeline.sem <- struct{}{}:
		current := pipeline.inFlight.Add(1)
		updateAtomicMaximum(&pipeline.peakFlight, current)
		return true
	case <-pipeline.ctx.Done():
		return false
	}
}

func (pipeline *rawDecodePipeline) release() {
	pipeline.inFlight.Add(-1)
	<-pipeline.sem
}

func (pipeline *rawDecodePipeline) Next() (*RawLedger, error) {
	if pipeline == nil {
		return nil, fmt.Errorf("raw decode pipeline is nil")
	}
	if pipeline.held {
		pipeline.release()
		pipeline.held = false
	}
	waitStarted := time.Now()
	defer func() {
		pipeline.waitNanos.Add(int64(time.Since(waitStarted)))
	}()
	for {
		if result, ok := pipeline.pending[pipeline.expected]; ok {
			delete(pipeline.pending, pipeline.expected)
			return pipeline.consume(result)
		}
		select {
		case <-pipeline.ctx.Done():
			return nil, pipeline.ctx.Err()
		case result, ok := <-pipeline.results:
			if !ok {
				if err := pipeline.ctx.Err(); err != nil {
					return nil, err
				}
				return nil, io.ErrUnexpectedEOF
			}
			if result.ordinal < pipeline.expected {
				if result.holdsSlot {
					pipeline.release()
				}
				return nil, fmt.Errorf("raw decode pipeline repeated ordinal %d", result.ordinal)
			}
			if result.ordinal == pipeline.expected {
				return pipeline.consume(result)
			}
			if _, duplicate := pipeline.pending[result.ordinal]; duplicate {
				if result.holdsSlot {
					pipeline.release()
				}
				return nil, fmt.Errorf("raw decode pipeline repeated pending ordinal %d", result.ordinal)
			}
			pipeline.pending[result.ordinal] = result
			updateAtomicMaximum(&pipeline.peakPending, int64(len(pipeline.pending)))
		}
	}
}

func (pipeline *rawDecodePipeline) consume(result rawDecodeResult) (*RawLedger, error) {
	pipeline.expected++
	if result.err != nil {
		if result.holdsSlot {
			pipeline.release()
		}
		if result.err != io.EOF {
			pipeline.cancel()
		}
		return nil, result.err
	}
	if !result.holdsSlot {
		pipeline.cancel()
		return nil, fmt.Errorf("raw decode pipeline terminal result %d has no error", result.ordinal)
	}
	pipeline.held = true
	return result.ledger, nil
}

func (pipeline *rawDecodePipeline) Metrics() rawDecodePipelineMetrics {
	if pipeline == nil {
		return rawDecodePipelineMetrics{}
	}
	return rawDecodePipelineMetrics{
		SourceDuration:      time.Duration(pipeline.sourceNanos.Load()),
		CopyDuration:        time.Duration(pipeline.copyNanos.Load()),
		WaitDuration:        time.Duration(pipeline.waitNanos.Load()),
		CopiedBytes:         pipeline.copiedBytes.Load(),
		PeakInFlight:        int(pipeline.peakFlight.Load()),
		PeakReorderBuffered: int(pipeline.peakPending.Load()),
	}
}

func (pipeline *rawDecodePipeline) Close() {
	if pipeline == nil {
		return
	}
	pipeline.closeOnce.Do(func() {
		if pipeline.held {
			pipeline.release()
			pipeline.held = false
		}
		pipeline.cancel()
		<-pipeline.done
	})
}

func updateAtomicMaximum(maximum *atomic.Int64, candidate int64) {
	for current := maximum.Load(); candidate > current; current = maximum.Load() {
		if maximum.CompareAndSwap(current, candidate) {
			return
		}
	}
}
