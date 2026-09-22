package snerd

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestShardedQueueBasic(t *testing.T) {
	dir := t.TempDir()

	q, err := NewShardedQueue("sharded-test", dir, 4)
	if err != nil {
		t.Fatalf("Failed to create ShardedQueue: %v", err)
	}

	time.Sleep(100 * time.Millisecond) // Let membership loop start

	var processed int32

	RegisterTaskHandler("my_task", func(ctx context.Context, data string) error {
		atomic.AddInt32(&processed, 1)
		return nil
	})

	t1 := &SnerdTask{
		TaskID:   "task-1",
		TaskType: "my_task",
		Parameters: "{}",
	}
	t2 := &SnerdTask{
		TaskID:   "task-2",
		TaskType: "my_task",
		Parameters: "{}",
	}

	if err := q.Enqueue(t1); err != nil {
		t.Fatalf("Failed to enqueue t1: %v", err)
	}
	if err := q.Enqueue(t2); err != nil {
		t.Fatalf("Failed to enqueue t2: %v", err)
	}

	time.Sleep(1500 * time.Millisecond)

	val := atomic.LoadInt32(&processed)
	if val != 2 {
		t.Errorf("Expected 2 processed, got %d", val)
	}
}
