package snerd

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWorkerPoolsIsolation(t *testing.T) {
	// 1. Setup isolated storage
	testStorePath := filepath.Join(os.TempDir(), "snerd_test_pools", "tasks.log")
	os.RemoveAll(filepath.Dir(testStorePath))
	defer os.RemoveAll(filepath.Dir(testStorePath))

	// 2. Configure Worker Pools: 1 default, 1 urgent
	pools := map[string]int{
		"default": 1,
		"urgent":  1,
	}

	queue := NewAnyQueueWithPoolsAndStorage("test-pool-queue", 100, 100*time.Millisecond, testStorePath, pools)
	defer queue.StopProcessor()

	// 3. Register a task handler that simulates long-running work
	defaultStarted := make(chan struct{})
	defaultDone := make(chan struct{})
	urgentDone := make(chan struct{})

	RegisterTaskHandler("LONG_TASK", func(ctx context.Context, params string) error {
		if params == `{"type":"default"}` {
			close(defaultStarted)
			<-defaultDone // block until test tells it to finish
		} else if params == `{"type":"urgent"}` {
			close(urgentDone)
		}
		return nil
	})

	// 4. Enqueue long-running default task
	defaultTask, _ := NewSnerdTaskAdvanced("t-def-1", "LONG_TASK", map[string]string{"type": "default"}, 1, 0, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	queue.EnqueueSnerdTask(defaultTask)

	// Wait for default task to start and block the default pool
	select {
	case <-defaultStarted:
		// Default task is running and holding the only permit in the "default" pool
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for default task to start")
	}

	// 5. Enqueue urgent task with pool="urgent"
	urgentPool := "urgent"
	urgentTask, _ := NewSnerdTaskAdvanced("t-urg-1", "LONG_TASK", map[string]string{"type": "urgent"}, 1, 0, nil, nil, nil, nil, nil, nil, nil, nil, &urgentPool)
	queue.EnqueueSnerdTask(urgentTask)

	// 6. Assert urgent task executes immediately (within reasonable time) despite default pool being blocked
	select {
	case <-urgentDone:
		// Urgent task executed! Success!
	case <-time.After(2 * time.Second):
		t.Fatal("Urgent task was blocked by default pool! Isolation failed.")
	}

	// 7. Cleanup
	close(defaultDone)
}
