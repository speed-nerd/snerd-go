package snerd

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLinearJobChaining(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "chaining_linear.log")
	q := NewAnyQueueWithStorage("chaining-linear-q", 100, 50*time.Millisecond, logFile)

	events := make(chan string, 3)

	RegisterTaskHandler("step_task", func(ctx context.Context, data string) error {
		events <- data
		return nil
	})

	taskA, _ := NewSnerdTaskAdvanced("taskA", "step_task", "A", 3, 1.0, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	taskB, _ := NewSnerdTaskAdvanced("taskB", "step_task", "B", 3, 1.0, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	taskB.TriggerAfterIds = []string{"taskA"}
	
	taskC, _ := NewSnerdTaskAdvanced("taskC", "step_task", "C", 3, 1.0, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	taskC.TriggerAfterIds = []string{"taskB"}

	// Enqueue in random order, dependencies should enforce A -> B -> C
	q.Enqueue(taskC)
	q.Enqueue(taskA)
	q.Enqueue(taskB)

	timeout := time.After(3 * time.Second)
	var order []string

collect:
	for i := 0; i < 3; i++ {
		select {
		case ev := <-events:
			order = append(order, ev)
		case <-timeout:
			t.Fatalf("Timeout waiting for tasks. Got %v", order)
			break collect
		}
	}

	if len(order) != 3 || order[0] != "\"A\"" || order[1] != "\"B\"" || order[2] != "\"C\"" {
		t.Fatalf("Expected order A, B, C but got %v", order)
	}

	q.StopProcessor()
	os.RemoveAll("./.snerdata")
}

func TestFanInJobChaining(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "chaining_fanin.log")
	q := NewAnyQueueWithStorage("chaining-fanin-q", 100, 50*time.Millisecond, logFile)

	events := make(chan string, 3)

	RegisterTaskHandler("step_task", func(ctx context.Context, data string) error {
		// simulate some work for A and B
		if data == "A" || data == "B" {
			time.Sleep(100 * time.Millisecond)
		}
		events <- data
		return nil
	})

	taskA, _ := NewSnerdTaskAdvanced("taskA", "step_task", "A", 3, 1.0, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	taskB, _ := NewSnerdTaskAdvanced("taskB", "step_task", "B", 3, 1.0, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	
	taskC, _ := NewSnerdTaskAdvanced("taskC", "step_task", "C", 3, 1.0, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	taskC.TriggerAfterIds = []string{"taskA", "taskB"}

	q.Enqueue(taskC)
	q.Enqueue(taskA)
	q.Enqueue(taskB)

	timeout := time.After(3 * time.Second)
	var order []string

collect:
	for i := 0; i < 3; i++ {
		select {
		case ev := <-events:
			order = append(order, ev)
		case <-timeout:
			t.Fatalf("Timeout waiting for tasks. Got %v", order)
			break collect
		}
	}

	if len(order) != 3 {
		t.Fatalf("Expected 3 tasks, got %d", len(order))
	}
	if order[2] != "\"C\"" {
		t.Fatalf("Expected C to run last, but got %v", order)
	}

	q.StopProcessor()
	os.RemoveAll("./.snerdata")
}

func TestOrphanedJobChaining(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "chaining_orphan.log")
	q := NewAnyQueueWithStorage("chaining-orphan-q", 100, 50*time.Millisecond, logFile)

	events := make(chan string, 1)

	RegisterTaskHandler("step_task", func(ctx context.Context, data string) error {
		events <- data
		return nil
	})

	taskA, _ := NewSnerdTaskAdvanced("taskA", "step_task", "A", 3, 1.0, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	taskA.TriggerAfterIds = []string{"nonexistent-parent"}

	q.Enqueue(taskA)

	// Wait to ensure it doesn't run
	select {
	case <-events:
		// Success: absent parent is treated as completed in v1
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("Orphaned task should have executed because absent parent is considered completed")
	}

	q.StopProcessor()
	os.RemoveAll("./.snerdata")
}
