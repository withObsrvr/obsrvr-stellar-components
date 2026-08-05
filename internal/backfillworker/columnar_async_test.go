package backfillworker

import (
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestColumnarWriteCoordinatorBoundsConcurrencyAndPreservesTableOrder(t *testing.T) {
	coordinator, err := newColumnarWriteCoordinator(2, 4)
	if err != nil {
		t.Fatal(err)
	}
	left := coordinator.Register("bronze.left")
	right := coordinator.Register("bronze.right")
	var active atomic.Int64
	var peak atomic.Int64
	var lock sync.Mutex
	orders := map[string][]int{}
	enqueue := func(queue *columnarWriteQueue, table string, ordinal int) {
		t.Helper()
		err := queue.Enqueue(columnarWriteTask{
			rows: 1,
			run: func() error {
				current := active.Add(1)
				updateAtomicMaximum(&peak, current)
				time.Sleep(2 * time.Millisecond)
				lock.Lock()
				orders[table] = append(orders[table], ordinal)
				lock.Unlock()
				active.Add(-1)
				return nil
			},
		})
		if err != nil {
			t.Fatalf("enqueue %s/%d: %v", table, ordinal, err)
		}
	}
	for ordinal := range 12 {
		enqueue(left, "left", ordinal)
		enqueue(right, "right", ordinal)
	}
	if err := coordinator.Close(); err != nil {
		t.Fatal(err)
	}
	if got := peak.Load(); got != 2 {
		t.Fatalf("peak active encoders = %d, want 2", got)
	}
	want := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
	if !reflect.DeepEqual(orders["left"], want) || !reflect.DeepEqual(orders["right"], want) {
		t.Fatalf("table order changed: left=%v right=%v", orders["left"], orders["right"])
	}
	metrics := coordinator.Metrics()
	if metrics.PeakActive != 2 || metrics.PeakPending > 4 {
		t.Fatalf("coordinator metrics = %+v, want active 2 and pending <= 4", metrics)
	}
}

func TestColumnarWriteCoordinatorPropagatesFirstErrorAndReleasesTasks(t *testing.T) {
	coordinator, err := newColumnarWriteCoordinator(1, 3)
	if err != nil {
		t.Fatal(err)
	}
	queue := coordinator.Register("bronze.events")
	wantErr := errors.New("encode failed")
	var ran atomic.Int64
	var released atomic.Int64
	start := make(chan struct{})
	for ordinal := range 3 {
		ordinal := ordinal
		if err := queue.Enqueue(columnarWriteTask{
			rows: 1,
			run: func() error {
				ran.Add(1)
				if ordinal == 0 {
					<-start
					return wantErr
				}
				return nil
			},
			release: func() { released.Add(1) },
		}); err != nil {
			t.Fatal(err)
		}
	}
	close(start)
	if err := coordinator.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("Close error = %v, want %v", err, wantErr)
	}
	if got := ran.Load(); got != 1 {
		t.Fatalf("tasks run after first error = %d, want 1", got)
	}
	if got := released.Load(); got != 3 {
		t.Fatalf("released tasks = %d, want 3", got)
	}
}

func TestColumnarWriteCoordinatorRejectsInvalidBounds(t *testing.T) {
	for _, bounds := range [][2]int{{0, 1}, {1, 0}, {2, 1}} {
		if _, err := newColumnarWriteCoordinator(bounds[0], bounds[1]); err == nil {
			t.Fatalf("newColumnarWriteCoordinator(%d, %d) succeeded", bounds[0], bounds[1])
		}
	}
}
