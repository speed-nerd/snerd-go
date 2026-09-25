package snerd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

var compacting atomic.Bool

// FileStore provides persistent storage for retryable tasks.
// It manages task log files, compaction, and metadata tracking for tasks.
type FileStore struct {
	mu           sync.Mutex
	filePath     string
	totalTasks   int
	deletedTasks int
	appendCount  int
	tasksCache   map[string]*RetryableTask
}

// NewFileStore creates a new FileStore for the given file path.
// It rebuilds metadata from the existing log file if present.
func NewFileStore(path string) (*FileStore, error) {
	fs := &FileStore{
		filePath:   path,
		tasksCache: make(map[string]*RetryableTask),
	}

	// rebuild counters from existing task.log file.
	if err := fs.RebuildMetaData(); err != nil {
		return nil, fmt.Errorf("rebuild metadata: %w", err)
	}

	return fs, nil
}

// RebuildMetaData scans the log file and rebuilds internal counters for tasks and deletions.
func (fs *FileStore) RebuildMetaData() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Clear cache
	fs.tasksCache = make(map[string]*RetryableTask)

	file, err := os.Open(fs.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// nothing to rebuild.
			return nil
		}
		return fmt.Errorf("open file: %w", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Printf("Error closing file: %s\n", err)
			return
		}
	}(file)

	fs.totalTasks = 0
	fs.deletedTasks = 0
	fs.appendCount = 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var t RetryableTask
		if err := json.Unmarshal(line, &t); err != nil {
			fmt.Printf("unmarshal task: %v\n", err)
			continue
		}

		fs.appendCount++
		if t.DeletedAt != nil && !t.DeletedAt.IsZero() {
			fs.deletedTasks++
			delete(fs.tasksCache, t.TaskID)
		} else {
			fs.totalTasks++
			taskCopy := t
			fs.tasksCache[t.TaskID] = &taskCopy
		}

	}
	return scanner.Err()
}

// CreateTask appends a new retryable task to the log file and updates internal counters.
func (fs *FileStore) CreateTask(task *RetryableTask) error {
	return fs.createTaskInner(task, true)
}

// createTaskInner is the internal save method with option to skip compaction check.
// DeleteTask passes checkCompact=false to prevent compaction cascades when
// many tasks complete concurrently.
func (fs *FileStore) createTaskInner(task *RetryableTask, checkCompact bool) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	dirPath := filepath.Dir(fs.filePath)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}

	f, err := os.OpenFile(fs.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal task: %w", err)
	}

	if _, err := f.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("write to file: %w", err)
	}

	// Update cache and stats
	fs.appendCount++
	if task.DeletedAt != nil && !task.DeletedAt.IsZero() {
		fs.deletedTasks++
		delete(fs.tasksCache, task.TaskID)
	} else {
		fs.totalTasks++
		taskCopy := *task
		fs.tasksCache[task.TaskID] = &taskCopy
	}

	if checkCompact && fs.shouldCompact() {
		go func() {
			if err := fs.Compact(); err != nil {
				fmt.Printf("Error compacting file: %v\n", err)
			}
		}()
	}

	return nil
}

func (fs *FileStore) ReadTasks() ([]*RetryableTask, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	tasks := make([]*RetryableTask, 0, len(fs.tasksCache))
	for _, t := range fs.tasksCache {
		taskCopy := *t
		tasks = append(tasks, &taskCopy)
	}
	return tasks, nil
}

func (fs *FileStore) ReadDueTasks() ([]*RetryableTask, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	var tasks []*RetryableTask
	now := time.Now().UTC()
	for _, t := range fs.tasksCache {
		// Defense-in-depth: skip soft-deleted tasks even though they should
		// already be evicted from the cache by createTaskInner.
		if t.DeletedAt != nil && !t.DeletedAt.IsZero() {
			continue
		}
		if (t.RetryAfterTime.Before(now) || t.RetryAfterTime.Equal(now)) && (t.ExecuteAt.Before(now) || t.ExecuteAt.Equal(now)) {
			taskCopy := *t
			tasks = append(tasks, &taskCopy)
		}
	}
	return tasks, nil
}

func (fs *FileStore) UpdateTaskRetryConfig(taskID string, taskErr error) error {
	task, err := fs.GetLatestTask(taskID)
	if err != nil {
		return err
	}

	task.RetryCount++
	retryHours := task.RetryAfterHours
	if retryHours <= 0 {
		retryHours = 0.5
	}
	retryDuration := time.Duration(retryHours * float64(time.Hour))
	task.RetryAfterTime = time.Now().Add(retryDuration)

	if taskErr != nil {
		task.LastErrorObj = taskErr
		task.LastJobError = &JobErrorReturn{
			ErrorObj:    taskErr,
			ErrorString: taskErr.Error(),
			RetryWorthy: true,
		}
	} else {
		task.LastErrorObj = nil
		task.LastJobError = nil
	}

	task.UpdatedAt = time.Now()
	return fs.CreateTask(task)
}

func (fs *FileStore) GetLatestTask(taskID string) (*RetryableTask, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if task, exists := fs.tasksCache[taskID]; exists {
		taskCopy := *task
		return &taskCopy, nil
	}
	return nil, fmt.Errorf("task with ID %s not found", taskID)
}

func (fs *FileStore) AreTasksCompleted(taskIDs []string) bool {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	for _, id := range taskIDs {
		if _, exists := fs.tasksCache[id]; exists {
			return false
		}
	}
	return true
}

func (fs *FileStore) DetectCycle(newTaskID string, triggerAfterIds []string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	visited := make(map[string]bool)
	stack := make([]string, len(triggerAfterIds))
	copy(stack, triggerAfterIds)

	for len(stack) > 0 {
		currentID := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if currentID == newTaskID {
			return fmt.Errorf("cycle detected involving task %s", newTaskID)
		}

		if visited[currentID] {
			continue
		}
		visited[currentID] = true

		if task, exists := fs.tasksCache[currentID]; exists {
			if len(task.TriggerAfterIds) > 0 {
				stack = append(stack, task.TriggerAfterIds...)
			}
		}
	}
	return nil
}

func (fs *FileStore) DeleteTask(taskID string) error {
	fs.mu.Lock()

	task, exists := fs.tasksCache[taskID]
	if !exists {
		fs.mu.Unlock()
		return nil
	}

	taskCopy := *task
	fs.mu.Unlock()

	now := time.Now()
	taskCopy.DeletedAt = &now
	taskCopy.UpdatedAt = now

	// Skip compaction check on delete to prevent cascading compaction
	// when many tasks complete concurrently (which would empty the log).
	return fs.createTaskInner(&taskCopy, false)
}

func (fs *FileStore) Compact() error {
	if !compacting.CompareAndSwap(false, true) {
		return nil
	}
	defer compacting.Store(false)

	fs.mu.Lock()
	defer fs.mu.Unlock()

	tempFilePath := fs.filePath + ".tmp"
	tempFile, err := os.Create(tempFilePath)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	encoder := json.NewEncoder(tempFile)
	for _, task := range fs.tasksCache {
		if err := encoder.Encode(task); err != nil {
			tempFile.Close()
			return fmt.Errorf("encode task: %w", err)
		}
	}

	if err := tempFile.Sync(); err != nil {
		tempFile.Close()
		return fmt.Errorf("sync temp file: %w", err)
	}

	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}

	if err := os.Rename(tempFilePath, fs.filePath); err != nil {
		return fmt.Errorf("rename temp file: %w", err)
	}

	fs.totalTasks = len(fs.tasksCache)
	fs.deletedTasks = 0
	fs.appendCount = 0

	return nil
}

func (fs *FileStore) shouldCompact() bool {
	info, err := os.Stat(fs.filePath)
	if err == nil && info.Size() > 20*1024*1024 {
		return true
	}
	if fs.totalTasks > 0 {
		ratio := float64(fs.deletedTasks) / float64(fs.totalTasks)
		if ratio > 0.5 {
			return true
		}
	}
	if fs.appendCount >= 10000 {
		fs.appendCount = 0
		return true
	}
	return false
}
