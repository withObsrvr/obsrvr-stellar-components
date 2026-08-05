package backfillworker

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type columnarWriteTask struct {
	rows    uint64
	run     func() error
	release func()
}

type columnarWriteCoordinatorMetrics struct {
	PeakActive           int
	PeakPending          int
	AdmissionWait        time.Duration
	EncodeWorkerDuration time.Duration
	DrainWait            time.Duration
}

type columnarWriteQueueMetrics struct {
	Rows                 uint64
	RecordBatches        uint64
	AdmissionWait        time.Duration
	EncodeWorkerDuration time.Duration
}

type columnarWriteCoordinator struct {
	encodeTokens  chan struct{}
	pendingTokens chan struct{}
	failed        chan struct{}

	queuesMu sync.Mutex
	queues   map[string]*columnarWriteQueue
	wait     sync.WaitGroup

	errorMu   sync.Mutex
	firstErr  error
	failOnce  sync.Once
	closeOnce sync.Once
	closed    atomic.Bool

	active        atomic.Int64
	pending       atomic.Int64
	peakActive    atomic.Int64
	peakPending   atomic.Int64
	admissionWait atomic.Int64
	encodeNanos   atomic.Int64
	drainNanos    atomic.Int64
}

type columnarWriteQueue struct {
	owner *columnarWriteCoordinator
	name  string
	tasks chan columnarWriteTask

	closeMu sync.RWMutex
	closed  bool

	rows          atomic.Uint64
	recordBatches atomic.Uint64
	admissionWait atomic.Int64
	encodeNanos   atomic.Int64
}

func newColumnarWriteCoordinator(workers, maxPending int) (*columnarWriteCoordinator, error) {
	if workers <= 0 {
		return nil, fmt.Errorf("parquet writer workers must be positive")
	}
	if maxPending < workers {
		return nil, fmt.Errorf("max pending row groups %d must be at least parquet writer workers %d", maxPending, workers)
	}
	return &columnarWriteCoordinator{
		encodeTokens:  make(chan struct{}, workers),
		pendingTokens: make(chan struct{}, maxPending),
		failed:        make(chan struct{}),
		queues:        make(map[string]*columnarWriteQueue),
	}, nil
}

func (coordinator *columnarWriteCoordinator) Register(name string) *columnarWriteQueue {
	if coordinator == nil {
		return nil
	}
	coordinator.queuesMu.Lock()
	defer coordinator.queuesMu.Unlock()
	if queue, ok := coordinator.queues[name]; ok {
		return queue
	}
	if coordinator.closed.Load() {
		return nil
	}
	queue := &columnarWriteQueue{
		owner: coordinator,
		name:  name,
		tasks: make(chan columnarWriteTask, cap(coordinator.pendingTokens)),
	}
	coordinator.queues[name] = queue
	coordinator.wait.Add(1)
	go queue.run()
	return queue
}

func (queue *columnarWriteQueue) Enqueue(task columnarWriteTask) error {
	if queue == nil || queue.owner == nil {
		releaseColumnarTask(task)
		return fmt.Errorf("columnar write queue is required")
	}
	if task.run == nil {
		releaseColumnarTask(task)
		return fmt.Errorf("columnar write task for %s has no run function", queue.name)
	}
	if err := queue.owner.Err(); err != nil {
		releaseColumnarTask(task)
		return err
	}
	if queue.owner.closed.Load() {
		releaseColumnarTask(task)
		return fmt.Errorf("columnar write coordinator is closed")
	}

	waitStarted := time.Now()
	select {
	case queue.owner.pendingTokens <- struct{}{}:
		waited := time.Since(waitStarted)
		queue.admissionWait.Add(int64(waited))
		queue.owner.admissionWait.Add(int64(waited))
		current := queue.owner.pending.Add(1)
		updateAtomicMaximum(&queue.owner.peakPending, current)
	case <-queue.owner.failed:
		releaseColumnarTask(task)
		return queue.owner.Err()
	}

	if queue.owner.closed.Load() {
		queue.owner.releasePending()
		releaseColumnarTask(task)
		return fmt.Errorf("columnar write coordinator is closed")
	}
	queue.closeMu.RLock()
	defer queue.closeMu.RUnlock()
	if queue.closed {
		queue.owner.releasePending()
		releaseColumnarTask(task)
		return fmt.Errorf("columnar write queue %s is closed", queue.name)
	}
	select {
	case queue.tasks <- task:
		return nil
	case <-queue.owner.failed:
		queue.owner.releasePending()
		releaseColumnarTask(task)
		return queue.owner.Err()
	}
}

func (queue *columnarWriteQueue) run() {
	defer queue.owner.wait.Done()
	for task := range queue.tasks {
		queue.rows.Add(task.rows)
		queue.recordBatches.Add(1)
		if queue.owner.Err() == nil {
			select {
			case queue.owner.encodeTokens <- struct{}{}:
				if queue.owner.Err() == nil {
					current := queue.owner.active.Add(1)
					updateAtomicMaximum(&queue.owner.peakActive, current)
					started := time.Now()
					err := task.run()
					elapsed := time.Since(started)
					queue.encodeNanos.Add(int64(elapsed))
					queue.owner.encodeNanos.Add(int64(elapsed))
					queue.owner.active.Add(-1)
					if err != nil {
						queue.owner.fail(fmt.Errorf("write %s row group: %w", queue.name, err))
					}
				}
				<-queue.owner.encodeTokens
			case <-queue.owner.failed:
			}
		}
		releaseColumnarTask(task)
		queue.owner.releasePending()
	}
}

func (coordinator *columnarWriteCoordinator) releasePending() {
	coordinator.pending.Add(-1)
	<-coordinator.pendingTokens
}

func releaseColumnarTask(task columnarWriteTask) {
	if task.release != nil {
		task.release()
	}
}

func (coordinator *columnarWriteCoordinator) fail(err error) {
	if coordinator == nil || err == nil {
		return
	}
	coordinator.failOnce.Do(func() {
		coordinator.errorMu.Lock()
		coordinator.firstErr = err
		coordinator.errorMu.Unlock()
		close(coordinator.failed)
	})
}

func (coordinator *columnarWriteCoordinator) Err() error {
	if coordinator == nil {
		return nil
	}
	coordinator.errorMu.Lock()
	defer coordinator.errorMu.Unlock()
	return coordinator.firstErr
}

func (coordinator *columnarWriteCoordinator) Close() error {
	if coordinator == nil {
		return nil
	}
	coordinator.closeOnce.Do(func() {
		drainStarted := time.Now()
		coordinator.closed.Store(true)
		coordinator.queuesMu.Lock()
		queues := make([]*columnarWriteQueue, 0, len(coordinator.queues))
		for _, queue := range coordinator.queues {
			queues = append(queues, queue)
		}
		coordinator.queuesMu.Unlock()
		for _, queue := range queues {
			queue.closeMu.Lock()
			queue.closed = true
			close(queue.tasks)
			queue.closeMu.Unlock()
		}
		coordinator.wait.Wait()
		coordinator.drainNanos.Store(int64(time.Since(drainStarted)))
	})
	return coordinator.Err()
}

func (coordinator *columnarWriteCoordinator) Metrics() columnarWriteCoordinatorMetrics {
	if coordinator == nil {
		return columnarWriteCoordinatorMetrics{}
	}
	return columnarWriteCoordinatorMetrics{
		PeakActive:           int(coordinator.peakActive.Load()),
		PeakPending:          int(coordinator.peakPending.Load()),
		AdmissionWait:        time.Duration(coordinator.admissionWait.Load()),
		EncodeWorkerDuration: time.Duration(coordinator.encodeNanos.Load()),
		DrainWait:            time.Duration(coordinator.drainNanos.Load()),
	}
}

func (queue *columnarWriteQueue) Metrics() columnarWriteQueueMetrics {
	if queue == nil {
		return columnarWriteQueueMetrics{}
	}
	return columnarWriteQueueMetrics{
		Rows:                 queue.rows.Load(),
		RecordBatches:        queue.recordBatches.Load(),
		AdmissionWait:        time.Duration(queue.admissionWait.Load()),
		EncodeWorkerDuration: time.Duration(queue.encodeNanos.Load()),
	}
}
