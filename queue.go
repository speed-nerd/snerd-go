package snerd

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofrs/flock"
)

// AnyQueue is a thread-safe queue that manages SnerdTask execution, retry logic, and statistics

// PriorityQueue implements heap.Interface and holds RetryableTasks
type PriorityQueue []*RetryableTask

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	scoreI := 0.0
	if pq[i].UrgencyScore != nil {
		scoreI = *pq[i].UrgencyScore
	}
	scoreJ := 0.0
	if pq[j].UrgencyScore != nil {
		scoreJ = *pq[j].UrgencyScore
	}
	// Max-Heap: greater score should be closer to root (Less returns true if i should pop BEFORE j)
	return scoreI > scoreJ
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*RetryableTask)
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

// AnyQueue is a thread-safe queue that manages SnerdTask execution, retry logic, and statistics
type AnyQueue struct {
	name          string
	maxSize       int
	mu            sync.Mutex
	totalEnqueued int64
	totalDequeued int64
	fileStore     *FileStore
	// storageLock holds an exclusive OS-level lock on the task log for the
	// queue's lifetime, guaranteeing a single processor per storage file.
	storageLock     *flock.Flock
	rateLimiter     *RateLimiter
	processorActive bool
	processorCtx    context.Context
	processorCancel context.CancelFunc
	activeHashes    map[string]bool
	hashMu          sync.Mutex
	executingTasks  map[string]bool
	execMu          sync.Mutex
	// Tasks that have been dispatched from ProcessDueTasks but haven't been
	// added to executingTasks yet. Prevents re-adding the same task.
	queuedTasks map[string]bool
	queuedMu    sync.Mutex
	// Tasks that have completed execution, mapped to their completion time.
	// Final safety net to prevent duplicates; entries are evicted after
	// completedTTL to bound memory.
	completedTasks map[string]time.Time
	completedMu    sync.Mutex
	workerPools    map[string]chan struct{}
	progressSubs   []chan string
	progressMu     sync.Mutex
}

// TaskFactory creates a Task from its stored data.
// The factory function is responsible for reconstructing a Task instance, including unmarshaling any stored data.
type TaskFactory func(id string, data string) (Task, error)

const (
	// How long a completed task id stays in completedTasks before eviction.
	// It is only a safety net against duplicate execution within a short
	// window; the tombstone in the task log is the durable source of truth.
	completedTTL = 60 * time.Second
	// How often the sweeper evicts expired completed entries.
	completedSweepInterval = 30 * time.Second
)

// NewAnyQueue creates a new queue with the given parameters
func NewAnyQueue(args ...interface{}) *AnyQueue {
	var name string = "default-queue"
	var maxSize int = 100                                    // reasonable default
	var taskStorePath string = "./.snerdata/tasks/tasks.log" // Use the hidden .snerdata folder
	var processingInterval time.Duration = 10 * time.Second  // Default processing interval

	// Try to determine the arguments based on their types
	for _, arg := range args {
		switch v := arg.(type) {
		case string:
			name = v
		case int:
			maxSize = v
		case time.Duration:
			processingInterval = v
		}
	}

	return newAnyQueue(name, maxSize, taskStorePath, processingInterval)
}

// NewAnyQueueWithStorage creates a new queue that persists tasks to a custom
// file location instead of the default ./.snerdata/tasks/tasks.log. This is
// useful for isolating queues per concern, pointing at a shared network drive
// (e.g. EFS/NFS) for cross-process queue sharing, or test isolation.
func NewAnyQueueWithStorage(name string, maxSize int, processingInterval time.Duration, taskStorePath string) *AnyQueue {
	return newAnyQueue(name, maxSize, taskStorePath, processingInterval)
}

// newAnyQueue is the shared constructor used by NewAnyQueue and
// NewAnyQueueWithStorage.
func newAnyQueue(name string, maxSize int, taskStorePath string, processingInterval time.Duration) *AnyQueue {
	pools := map[string]int{"default": 100}
	return newAnyQueueAdvanced(name, maxSize, taskStorePath, processingInterval, pools)
}

// NewAnyQueueWithPools creates a new queue with explicit worker pool sizes
func NewAnyQueueWithPools(name string, maxSize int, pools map[string]int) *AnyQueue {
	taskStorePath := "./.snerdata/tasks/tasks.log"
	processingInterval := 10 * time.Second
	return newAnyQueueAdvanced(name, maxSize, taskStorePath, processingInterval, pools)
}

// NewAnyQueueWithPoolsAndStorage creates a new queue with pools and custom storage
func NewAnyQueueWithPoolsAndStorage(name string, maxSize int, processingInterval time.Duration, taskStorePath string, pools map[string]int) *AnyQueue {
	return newAnyQueueAdvanced(name, maxSize, taskStorePath, processingInterval, pools)
}

func newAnyQueueAdvanced(name string, maxSize int, taskStorePath string, processingInterval time.Duration, poolConfig map[string]int) *AnyQueue {
	// Acquire exclusive ownership of the task log before anything else. Two
	// processors on the same file would race and double-execute tasks, so a
	// second queue on the same storage fails fast instead. The OS releases the
	// lock automatically when the process exits.
	if mkErr := os.MkdirAll(filepath.Dir(taskStorePath), 0755); mkErr != nil {
		panic(fmt.Sprintf("[Snerd] ERROR: Could not create storage directory '%s': %v", filepath.Dir(taskStorePath), mkErr))
	}
	storageLock := flock.New(taskStorePath + ".lock")
	locked, lockErr := storageLock.TryLock()
	if lockErr != nil || !locked {
		panic(fmt.Sprintf("[Snerd] ERROR: Another queue instance is already running on storage '%s'. "+
			"Use a single queue instance per storage file (register all your task types on it), "+
			"or use NewAnyQueueWithStorage with a different path. (lock file: %s.lock)", taskStorePath, taskStorePath))
	}

	// Initialize the file store
	fileStore, err := NewFileStore(taskStorePath)
	if err != nil {
		fmt.Printf("Warning: Could not initialize file store: %v\n", err)
		// Create default empty file store path
		dirPath := filepath.Dir(taskStorePath)
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			fmt.Printf("Error creating directory: %v\n", err)
		}
		// On Unix-like systems, directories starting with a dot are already hidden
		// For Windows, hide the directory
		if runtime.GOOS == "windows" {
			// Get the parent directory to find the .snerdata folder
			snerDataDir := filepath.Join(filepath.Dir(dirPath), ".snerdata")
			// Use attrib command to set the hidden attribute
			cmd := exec.Command("attrib", "+h", snerDataDir)
			if err := cmd.Run(); err != nil {
				fmt.Printf("Warning: Could not hide directory on Windows: %v\n", err)
			}
		}
		// Try again with empty file
		fileStore, err = NewFileStore(taskStorePath)
		if err != nil {
			fmt.Printf("Error: Still could not initialize file store: %v\n", err)
		}
	}

	initialHashes := make(map[string]bool)
	if fileStore != nil {
		if tasks, rErr := fileStore.ReadTasks(); rErr == nil {
			for _, task := range tasks {
				if (task.DeletedAt == nil || task.DeletedAt.IsZero()) && task.PayloadHash != nil {
					initialHashes[*task.PayloadHash] = true
				}
			}
		}
	}

	// Initialize worker pools
	workerPools := make(map[string]chan struct{})
	hasDefault := false
	for poolName, capacity := range poolConfig {
		workerPools[poolName] = make(chan struct{}, capacity)
		if poolName == "default" {
			hasDefault = true
		}
	}
	if !hasDefault {
		workerPools["default"] = make(chan struct{}, 100)
	}

	// Create the queue with the specified parameters
	q := &AnyQueue{
		name:            name,
		maxSize:         maxSize,
		processorActive: false,
		executingTasks:  make(map[string]bool),
		queuedTasks:     make(map[string]bool),
		completedTasks:  make(map[string]time.Time),
		activeHashes:    initialHashes,
		fileStore:       fileStore,
		storageLock:     storageLock,
		rateLimiter:     NewRateLimiter(filepath.Dir(taskStorePath)),
		workerPools:     workerPools,
		progressSubs:    make([]chan string, 0),
	}

	// Start the task processor in the background
	q.startProcessor(processingInterval)

	return q
}

// startProcessor starts the background task processor
func (q *AnyQueue) startProcessor(interval time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.processorActive {
		return // Already running
	}

	q.processorCtx, q.processorCancel = context.WithCancel(context.Background())
	q.processorActive = true

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-q.processorCtx.Done():
				q.mu.Lock()
				q.processorActive = false
				q.mu.Unlock()
				return
			case <-ticker.C:
				q.ProcessDueTasks() // Process on each tick
			}
		}
	}()

	// Periodically evict completed-task entries older than completedTTL so
	// the dedup map does not grow unboundedly on long-running processes.
	go func() {
		ticker := time.NewTicker(completedSweepInterval)
		defer ticker.Stop()

		for {
			select {
			case <-q.processorCtx.Done():
				return
			case <-ticker.C:
				q.completedMu.Lock()
				evictExpiredCompleted(q.completedTasks, time.Now(), completedTTL)
				q.completedMu.Unlock()
			}
		}
	}()
}

// evictExpiredCompleted drops completed entries older than now-ttl.
func evictExpiredCompleted(completed map[string]time.Time, now time.Time, ttl time.Duration) {
	for id, completedAt := range completed {
		if now.Sub(completedAt) >= ttl {
			delete(completed, id)
		}
	}
}

// StopProcessor stops the background task processor
func (q *AnyQueue) StopProcessor() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.processorActive || q.processorCancel == nil {
		return // Not running
	}

	q.processorCancel()
	q.processorActive = false
}

// Enqueue adds a task to the queue
func (q *AnyQueue) Enqueue(task Task) error {
	// Handle SnerdTask directly
	if snerdTask, ok := task.(*SnerdTask); ok {
		return q.EnqueueSnerdTask(snerdTask)
	}

	// For any non-SnerdTask, return an error as we no longer support legacy task types
	return fmt.Errorf("legacy task types are no longer supported; use SnerdTask instead")
}

// EnqueueSnerdTask adds a parameter-based SnerdTask to the queue for execution
// This is the preferred method for adding new tasks as it uses the parameter-based
// approach that doesn't require client-side task registration
func (q *AnyQueue) EnqueueSnerdTask(task *SnerdTask) error {
	if task.PayloadHash != nil {
		q.hashMu.Lock()
		if q.activeHashes[*task.PayloadHash] {
			q.hashMu.Unlock()
			fmt.Printf("Duplicate task detected (hash: %s), silently dropping.\n", *task.PayloadHash)
			return nil
		}
		q.activeHashes[*task.PayloadHash] = true
		q.hashMu.Unlock()
	}

	// Convert the SnerdTask to a RetryableTask for storage
	zeroTime := time.Time{}
	task.DeletedAt = &zeroTime
	retryableTask := task.ToRetryableTask()

	// Store the task in the file store
	if q.fileStore != nil {
		if err := q.fileStore.CreateTask(retryableTask); err != nil {
			return fmt.Errorf("failed to store task: %w", err)
		}
	}

	// Update queue stats
	atomic.AddInt64(&q.totalEnqueued, 1)

	// NOTE: We intentionally do NOT execute tasks immediately here.
	// All execution goes through the periodic processor (ProcessDueTasks)
	// which uses a PriorityQueue to respect priority ordering.
	// The fast path would bypass priority and cause low-priority tasks
	// enqueued first to always execute before high-priority tasks enqueued later.

	return nil
}

func (q *AnyQueue) processTask(task Task) {
	// Create context (default empty context since this is an internal bypass)
	ctx := context.Background()

	// Just execute the task directly
	if err := task.Execute(ctx); err != nil {
		fmt.Printf("error executing task: %v\n", err)
	}
}

// ProcessDueTasks processes all tasks that are due for execution (retry time has passed)..
func (q *AnyQueue) ProcessDueTasks() {
	// Step 1: Load all tasks from the FileStore
	if q.fileStore == nil {
		fmt.Println("No file store available for processing tasks")
		return
	}

	fmt.Println("Processing due tasks...")
	tasks, err := q.fileStore.ReadDueTasks()
	if err != nil {
		fmt.Printf("Error reading tasks: %v\n", err)
		return
	}

	// No need to filter here, ReadDueTasks already returns only due tasks
	if len(tasks) == 0 {
		fmt.Println("No due tasks found")
		return
	}

	fmt.Printf("Found %d due tasks\n", len(tasks))

	// Step 2: Group tasks by pool
	tasksByPool := make(map[string][]*RetryableTask)
	for _, t := range tasks {
		poolName := "default"
		if t.Pool != nil && *t.Pool != "" {
			poolName = *t.Pool
		}
		tasksByPool[poolName] = append(tasksByPool[poolName], t)
	}

	// Step 3: Process each pool independently
	for poolName, poolTasks := range tasksByPool {
		poolChan, exists := q.workerPools[poolName]
		if !exists {
			poolChan, exists = q.workerPools["default"]
			if !exists {
				fmt.Printf("Warning: Default pool missing, dropping tasks for pool %s\n", poolName)
				continue
			}
		}

		pq := make(PriorityQueue, len(poolTasks))
		for i, t := range poolTasks {
			pq[i] = t
		}
		heap.Init(&pq)

		available := cap(poolChan) - len(poolChan)
		if available <= 0 {
			continue
		}

		for i := 0; i < available && pq.Len() > 0; i++ {
			t := heap.Pop(&pq).(*RetryableTask)

			// Check if task is already queued or executing (prevent duplicates)
			q.queuedMu.Lock()
			if q.queuedTasks[t.GetTaskID()] {
				q.queuedMu.Unlock()
				continue
			}
			q.queuedTasks[t.GetTaskID()] = true
			q.queuedMu.Unlock()

			// Wait for a worker slot
			poolChan <- struct{}{}

			// Convert RetryableTask to SnerdTask for execution
			snerdTask := FromRetryableTask(t)

			// Skip tasks with missing type or parameters
			if snerdTask.TaskType == "" {
				fmt.Printf("Skipping task %s: missing task type\n", snerdTask.GetTaskID())
				q.queuedMu.Lock()
				delete(q.queuedTasks, snerdTask.GetTaskID())
				q.queuedMu.Unlock()
				<-poolChan
				continue
			}

			// Log task execution for debugging
			fmt.Printf("Executing task %s (type=%s)\n", snerdTask.GetTaskID(), snerdTask.TaskType)

			// Get the task handler from the registry
			isWebhook := snerdTask.WebhookUrl != nil && *snerdTask.WebhookUrl != ""
			var handler func(context.Context, string) error
			var exists bool

			if !isWebhook {
				handlersMutex.RLock()
				handler, exists = taskHandlers[snerdTask.TaskType]
				handlersMutex.RUnlock()

				if !exists || handler == nil {
					fmt.Printf("No handler registered for task type: %s\n", snerdTask.TaskType)
					q.queuedMu.Lock()
					delete(q.queuedTasks, snerdTask.GetTaskID())
					q.queuedMu.Unlock()
					continue
				}
			}

			// Execute the handler with the task parameters
			fmt.Printf("Task parameters: %s\n", snerdTask.Parameters)

			// Check Rate Limits before executing
			if snerdTask.RateLimitGroup != nil && snerdTask.MaxPerMinute != nil {
				if !q.rateLimiter.CheckLimit(*snerdTask.RateLimitGroup, *snerdTask.MaxPerMinute) {
					snerdTask.RetryAfterTime = time.Now().Add(60 * time.Second)
					if q.fileStore != nil {
						q.fileStore.UpdateTaskRetryConfig(snerdTask.GetTaskID(), fmt.Errorf("rate_limit_exceeded"))
					}
					q.queuedMu.Lock()
					delete(q.queuedTasks, snerdTask.GetTaskID())
					q.queuedMu.Unlock()
					<-poolChan
					continue
				}
			}

			q.execMu.Lock()

			// Move from queued to executing
			q.queuedMu.Lock()
			delete(q.queuedTasks, snerdTask.GetTaskID())
			q.queuedMu.Unlock()

			// Double-check against the latest state in the file store to avoid TOCTOU race conditions
			if q.fileStore != nil {
				latestTask, err := q.fileStore.GetLatestTask(snerdTask.GetTaskID())
				if err == nil && latestTask != nil {
					now := time.Now().UTC()
					if (!latestTask.ExecuteAt.IsZero() && latestTask.ExecuteAt.After(now)) ||
						(!latestTask.RetryAfterTime.IsZero() && latestTask.RetryAfterTime.After(now)) {
						// Task is not due anymore
						q.execMu.Unlock()
						<-poolChan
						continue
					}
				} else {
					// Task was deleted
					q.execMu.Unlock()
					<-poolChan
					continue
				}
			}

			if q.executingTasks[snerdTask.GetTaskID()] {
				q.execMu.Unlock()
				<-poolChan
				continue
			}
			q.executingTasks[snerdTask.GetTaskID()] = true
			q.execMu.Unlock()

			go func(snerdTask *SnerdTask, handler func(context.Context, string) error, isWebhook bool, poolChan chan struct{}) {
				// Final safety check: skip if already completed (prevents duplicate execution)
				q.completedMu.Lock()
				_, alreadyCompleted := q.completedTasks[snerdTask.GetTaskID()]
				if alreadyCompleted {
					q.completedMu.Unlock()
					<-poolChan
					return
				}
				q.completedMu.Unlock()

				defer func() {
					q.execMu.Lock()
					delete(q.executingTasks, snerdTask.GetTaskID())
					q.execMu.Unlock()
					<-poolChan
				}()

				var ctx context.Context
				var cancel context.CancelFunc
				if snerdTask.MaxExecutionSeconds != nil {
					ctx, cancel = context.WithTimeout(context.Background(), time.Duration(*snerdTask.MaxExecutionSeconds)*time.Second)
				} else {
					ctx, cancel = context.WithCancel(context.Background())
				}
				defer cancel()

				var err error
				if isWebhook {
					err = snerdTask.Execute(ctx)
				} else {
					err = handler(ctx, snerdTask.Parameters)
				}
				if err != nil {
					fmt.Println("Error executing the TASK!!!!")
					// Task failed execution
					fmt.Printf("Error executing task %s: %v\n", snerdTask.GetTaskID(), err)

					// Handle retry logic if the task has failed
					// maxRetries means total attempts (not retries after first).
					// UpdateTaskRetryConfig will increment the retry count, so we check
					// if the current count is less than maxRetries - 1 to allow one more retry.
					if snerdTask.RetryCount < snerdTask.MaxRetries-1 {

						fmt.Println("RETRYING THE TASK!!!!")

						// Update task in file store with retry information
						if q.fileStore != nil {
							fmt.Println("CALLING QUEUE FILESTORE FOR RETRYING THE TASK!!!!")
							// Calculate next retry time for logging
							retryHours := snerdTask.RetryAfterHours
							if retryHours <= 0 {
								// Default to 30 minutes if not specified
								retryHours = 0.5
							}
							retryDuration := time.Duration(retryHours * float64(time.Hour))

							// Log the retry information (RetryCount+1 because UpdateTaskRetryConfig will increment)
							fmt.Printf("Scheduling task %s for retry %d/%d at %s\n",
								snerdTask.GetTaskID(),
								snerdTask.RetryCount+1,
								snerdTask.MaxRetries,
								time.Now().Add(retryDuration).Format(time.RFC3339))

							// Update the task for retry (this increments RetryCount in the file store)
							updateErr := q.fileStore.UpdateTaskRetryConfig(snerdTask.GetTaskID(), err)
							if updateErr != nil {
								fmt.Printf("Error updating task retry config: %v\n", updateErr)
							} else {
								fmt.Printf("Successfully updated task %s for retry\n", snerdTask.GetTaskID())
							}
						} else {
							fmt.Printf("Warning: Cannot update task %s - no file store available\n", snerdTask.GetTaskID())
						}
					} else {
						// Max retries reached - execute the task's OnMaxRetryReached method if implemented
						fmt.Printf("Task %s reached max retries (%d)\n", snerdTask.GetTaskID(), snerdTask.MaxRetries)
						// Create a context provider function that returns the error
						contextProvider := func() interface{} {
							return err
						}
						// Pass the context provider to OnMaxRetryReached
						if callbackErr := snerdTask.OnMaxRetryReached(ctx, contextProvider); callbackErr != nil {
							fmt.Printf("Error executing OnMaxRetryReached: %v\n", callbackErr)
						}

						// Delete the task after it has reached max retries
						if q.fileStore != nil {
							// Mark as completed to prevent duplicate execution
							q.completedMu.Lock()
							q.completedTasks[snerdTask.GetTaskID()] = time.Now()
							q.completedMu.Unlock()

							// First check if the task is already deleted
							latestTask, getErr := q.fileStore.GetLatestTask(snerdTask.GetTaskID())
							if getErr != nil {
								fmt.Printf("Error getting latest task: %v\n", getErr)
							} else if latestTask.DeletedAt == nil || latestTask.DeletedAt.IsZero() {
								// Only delete if not already deleted
								deleteErr := q.fileStore.DeleteTask(snerdTask.GetTaskID())
								if deleteErr != nil {
									fmt.Printf("Error deleting task: %v\n", deleteErr)
								} else {
									if snerdTask.PayloadHash != nil {
										q.hashMu.Lock()
										delete(q.activeHashes, *snerdTask.PayloadHash)
										q.hashMu.Unlock()
									}
									fmt.Printf("Successfully deleted task %s after max retries\n", snerdTask.GetTaskID())
								}
							} else {
								fmt.Printf("Task %s is already deleted, skipping deletion\n", snerdTask.GetTaskID())
							}
						}
					}
				} else {
					// Task executed successfully
					fmt.Printf("Task %s executed successfully\n", snerdTask.GetTaskID())

					if q.fileStore != nil {
						rescheduled := false
						if snerdTask.CronExpr != nil && *snerdTask.CronExpr != "" {
							parser := cronParser()
							if sched, err := parser.Parse(*snerdTask.CronExpr); err == nil {
								snerdTask.ExecuteAt = sched.Next(time.Now().UTC())
								snerdTask.RetryCount = 0
								snerdTask.LastErrorObj = nil
								snerdTask.LastJobError = nil
								if saveErr := q.fileStore.CreateTask(snerdTask.ToRetryableTask()); saveErr != nil {
									fmt.Printf("Error rescheduling cron task: %v\n", saveErr)
								} else {
									rescheduled = true
									fmt.Printf("Cron task %s rescheduled for %s\n", snerdTask.GetTaskID(), snerdTask.ExecuteAt.Format(time.RFC3339))
								}
							}
						}

						if !rescheduled {
							fmt.Println("CALLING QUEUE FILESTORE FOR DELETING THE TASK AFTER SUCCESSFUL TASK!!!!")
							// Mark as completed to prevent duplicate execution
							q.completedMu.Lock()
							q.completedTasks[snerdTask.GetTaskID()] = time.Now()
							q.completedMu.Unlock()

							latestTask, getErr := q.fileStore.GetLatestTask(snerdTask.GetTaskID())
							if getErr != nil {
								fmt.Printf("Error getting latest task: %v\n", getErr)
							} else if latestTask.DeletedAt == nil || latestTask.DeletedAt.IsZero() {
								deleteErr := q.fileStore.DeleteTask(snerdTask.GetTaskID())
								if deleteErr != nil {
									fmt.Printf("Error deleting task %s: %v\n", snerdTask.GetTaskID(), deleteErr)
								} else {
									if snerdTask.PayloadHash != nil {
										q.hashMu.Lock()
										delete(q.activeHashes, *snerdTask.PayloadHash)
										q.hashMu.Unlock()
									}
									fmt.Printf("Successfully deleted task %s after completion\n", snerdTask.GetTaskID())
								}
							}
						}

						// Record task completion statistics
						duration := time.Since(snerdTask.CreatedAt)
						fmt.Printf("Task %s completed in %v (type=%s)\n",
							snerdTask.GetTaskID(),
							duration.Round(time.Millisecond),
							snerdTask.TaskType)
					}
				}
				atomic.AddInt64(&q.totalDequeued, 1)
			}(snerdTask, handler, isWebhook, poolChan)
		}
	}
}
func (q *AnyQueue) Name() string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.name
}

// Size returns the number of active tasks currently in the queue.
func (q *AnyQueue) Size() int {
	// With the new implementation, we need to count tasks from the FileStore
	if q.fileStore == nil {
		return 0
	}

	tasks, err := q.fileStore.ReadDueTasks()
	if err != nil {
		fmt.Printf("Error reading tasks: %v\n", err)
		return 0
	}

	return len(tasks)
}

// RemainingCapacity returns the number of additional tasks that can be enqueued before reaching maxSize..
func (q *AnyQueue) RemainingCapacity() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.maxSize - q.Size()
}

// TotalProcessed returns the total number of tasks that have been processed and dequeued.
func (q *AnyQueue) TotalProcessed() int {
	return int(atomic.LoadInt64(&q.totalDequeued)) // atomic read
}

// TotalEnqueued returns the total number of tasks that have been enqueued.
func (q *AnyQueue) TotalEnqueued() int {
	return int(atomic.LoadInt64(&q.totalEnqueued)) // atomic read
}

// TaskWithData extends Task to support saving and retrieving task-specific data.
// Implement this interface if your task needs to persist additional fields.
type TaskWithData interface {
	Task
	// GetTaskType returns a unique identifier for this task type.
	// This is used for debugging and monitoring, not for type-based dispatch.
	GetTaskType() string
	// MarshalData serializes the task data to JSON.
	MarshalData() ([]byte, error)
	// UnmarshalData deserializes the task data from JSON.
	UnmarshalData([]byte) error
	// Clone creates a new instance of this task with the same type but no data.
	// This will be populated via UnmarshalData when reconstructing tasks.
	Clone() TaskWithData
}

// SubscribeProgress returns a channel that receives real-time task progress JSON chunks.
func (q *AnyQueue) SubscribeProgress() <-chan string {
	q.progressMu.Lock()
	defer q.progressMu.Unlock()
	ch := make(chan string, 100)
	q.progressSubs = append(q.progressSubs, ch)
	return ch
}

// YieldProgress broadcasts a progress update to all subscribers.
func (q *AnyQueue) YieldProgress(taskID string, data string) {
	// Marshal to ensure special characters in data are properly escaped
	payload, err := json.Marshal(map[string]string{
		"action":  "progress",
		"task_id": taskID,
		"data":    data,
	})
	if err != nil {
		return
	}
	jsonStr := string(payload)
	q.progressMu.Lock()
	defer q.progressMu.Unlock()
	for _, ch := range q.progressSubs {
		select {
		case ch <- jsonStr:
		default:
			// Skip blocking
		}
	}
}
