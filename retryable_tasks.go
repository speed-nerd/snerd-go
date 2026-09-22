package snerd

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// These declarations are already defined elsewhere in the codebase

// Task factory registration happens elsewhere in the codebase

// Path to the data folder - using a cross-platform approach to hidden folders
const filePath = "./.snerdata/tasks/tasks.log"

// Task with retries
type RetryableTask struct {
	TaskID          string    `json:"taskId"`
	RetryCount      int       `json:"retryCount"`
	MaxRetries      int       `json:"maxRetries"`
	RetryAfterHours float64   `json:"retryAfterHours"`
	RetryAfterTime  time.Time `json:"retryAfterTime"`
	TaskData        string    `json:"taskData"` // JSON string to store task-specific data
	TaskType        string    `json:"taskType"` // For diagnostic purposes only
	RateLimitGroup  *string   `json:"rate_limit_group,omitempty"`
	MaxPerMinute    *int      `json:"max_per_minute,omitempty"`
	AutoDedupe      *bool     `json:"autoDedupe,omitempty"`
	PayloadHash     *string   `json:"payloadHash,omitempty"`
	UrgencyScore    *float64  `json:"urgency_score,omitempty"`
	// Fields to store error information for OnMaxRetryReached
	LastErrorObj        error
	LastJobError        *JobErrorReturn
	ExecuteAt           time.Time  `json:"executeAt"`
	CronExpr            *string    `json:"cronExpression,omitempty"`
	WebhookUrl          *string    `json:"webhookUrl,omitempty"`
	MaxExecutionSeconds *int       `json:"maxExecutionSeconds,omitempty"`
	Pool                *string    `json:"pool,omitempty"`
	CreatedAt           time.Time  `json:"-"`
	UpdatedAt           time.Time  `json:"-"`
	DeletedAt           *time.Time `json:"deletedAt,omitempty"`
	// Embedded task object - this is the actual task that will be executed
	EmbeddedTask Task `json:"-"`
}

func (t *RetryableTask) Execute(ctx context.Context) error {
	// If we have an embedded task, delegate execution to it
	if t.EmbeddedTask != nil {
		return t.EmbeddedTask.Execute(ctx)
	}

	// No embedded task, try to execute using the registered task handler directly
	if t.TaskType != "" {
		// First try to get the task handler from the global registry
		handlersMutex.RLock()
		handler, exists := taskHandlers[t.TaskType]
		handlersMutex.RUnlock()

		if exists && handler != nil {
			// Convert RetryableTask to SnerdTask to extract parameters properly
			snerdTask := FromRetryableTask(t)

			// Execute the task with its parameters
			return handler(ctx, snerdTask.Parameters)
		}

		// Fallback to legacy factory approach
		factory, found := taskFactories[t.TaskType]
		if found && factory != nil {
			// Use the factory to create a concrete task instance
			concreteTask, err := factory(t.TaskID, t.TaskData)
			if err == nil && concreteTask != nil {
				// Successfully reconstructed the task
				t.EmbeddedTask = concreteTask
				// Execute the reconstructed task
				return t.EmbeddedTask.Execute(ctx)
			}
		}
	}

	// If we reach here, we couldn't execute the task using any method
	return fmt.Errorf("no handler or factory registered for task type: %s", t.TaskType)
}

func (t *RetryableTask) GetTaskID() string            { return t.TaskID }
func (t *RetryableTask) GetRetryCount() int           { return t.RetryCount }
func (t *RetryableTask) GetMaxRetries() int           { return t.MaxRetries }
func (t *RetryableTask) GetRetryAfterTime() time.Time { return t.RetryAfterTime }

func (t *RetryableTask) GetRetryAfterHours() float64 { return t.RetryAfterHours }

// MarshalJSON implements the json.Marshaler interface to ensure proper serialization of RetryableTask
func (t *RetryableTask) MarshalJSON() ([]byte, error) {
	// Create an Alias struct without the EmbeddedTask field to avoid infinite recursion
	type Alias struct {
		TaskID              string          `json:"taskId"`
		RetryCount          int             `json:"retryCount"`
		MaxRetries          int             `json:"maxRetries"`
		RetryAfterHours     float64         `json:"retryAfterHours"`
		RetryAfterTime      time.Time       `json:"retryAfterTime"`
		TaskData            string          `json:"taskData"`
		TaskType            string          `json:"taskType"`
		ExecuteAt           time.Time       `json:"executeAt"`
		CronExpr            *string         `json:"cronExpression,omitempty"`
		WebhookUrl          *string         `json:"webhookUrl,omitempty"`
		MaxExecutionSeconds *int            `json:"maxExecutionSeconds,omitempty"`
		Pool                *string         `json:"pool,omitempty"`
		LastErrorObj        string          `json:"lastErrorObj,omitempty"`
		LastJobError        *JobErrorReturn `json:"lastJobError,omitempty"`
		DeletedAt           *time.Time      `json:"deletedAt,omitempty"`
	}

	// Before serializing, ensure TaskData has the latest data from EmbeddedTask
	if t.EmbeddedTask != nil {
		// This part is critical: serialize the embedded task to JSON and store it in TaskData
		taskData, err := json.Marshal(t.EmbeddedTask)
		if err == nil {
			// Update TaskData with the serialized embedded task
			t.TaskData = string(taskData)
			fmt.Printf("Task %s: serialized embedded task data of type %s\n", t.TaskID, t.TaskType)
		} else {
			fmt.Printf("Warning: Could not serialize embedded task data for task %s: %v\n", t.TaskID, err)
		}
	}

	var errStr string
	if t.LastErrorObj != nil {
		errStr = t.LastErrorObj.Error()
	}

	alias := Alias{
		TaskID:              t.TaskID,
		RetryCount:          t.RetryCount,
		MaxRetries:          t.MaxRetries,
		RetryAfterHours:     t.RetryAfterHours,
		RetryAfterTime:      t.RetryAfterTime,
		TaskData:            t.TaskData,
		TaskType:            t.TaskType,
		ExecuteAt:           t.ExecuteAt,
		CronExpr:            t.CronExpr,
		WebhookUrl:          t.WebhookUrl,
		MaxExecutionSeconds: t.MaxExecutionSeconds,
		Pool:                t.Pool,
		LastErrorObj:        errStr,
		LastJobError:        t.LastJobError,
		DeletedAt:           t.DeletedAt,
	}

	// Standard json.Marshal produces compact JSON without indentation or newlines
	// which works perfectly with the line-by-line storage in the .snerdata task log
	return json.Marshal(alias)
}

// UnmarshalJSON implements the json.Unmarshaler interface to properly deserialize a RetryableTask
func (t *RetryableTask) UnmarshalJSON(data []byte) error {
	// Use a temporary struct without the EmbeddedTask field to avoid infinite recursion
	type Alias struct {
		TaskID              string          `json:"taskId"`
		RetryCount          int             `json:"retryCount"`
		MaxRetries          int             `json:"maxRetries"`
		RetryAfterHours     float64         `json:"retryAfterHours"`
		RetryAfterTime      time.Time       `json:"retryAfterTime"`
		TaskData            string          `json:"taskData"`
		TaskType            string          `json:"taskType"`
		LastErrorObj        string          `json:"lastErrorObj,omitempty"`
		LastJobError        *JobErrorReturn `json:"lastJobError,omitempty"`
		ExecuteAt           time.Time       `json:"executeAt"`
		CronExpr            *string         `json:"cronExpression,omitempty"`
		WebhookUrl          *string         `json:"webhookUrl,omitempty"`
		MaxExecutionSeconds *int            `json:"maxExecutionSeconds,omitempty"`
		Pool                *string         `json:"pool,omitempty"`
		DeletedAt           *time.Time      `json:"deletedAt,omitempty"`
	}

	// Unmarshal into our temporary struct
	var alias Alias
	if err := json.Unmarshal(data, &alias); err != nil {
		return err
	}

	// Copy all fields except EmbeddedTask
	t.TaskID = alias.TaskID
	t.RetryCount = alias.RetryCount
	t.MaxRetries = alias.MaxRetries
	t.RetryAfterHours = alias.RetryAfterHours
	t.RetryAfterTime = alias.RetryAfterTime
	t.TaskData = alias.TaskData
	t.TaskType = alias.TaskType
	if alias.LastErrorObj != "" {
		t.LastErrorObj = errors.New(alias.LastErrorObj)
	}
	t.LastJobError = alias.LastJobError
	t.ExecuteAt = alias.ExecuteAt
	t.CronExpr = alias.CronExpr
	t.WebhookUrl = alias.WebhookUrl
	t.MaxExecutionSeconds = alias.MaxExecutionSeconds
	t.Pool = alias.Pool
	t.DeletedAt = alias.DeletedAt

	// We'll reconstruct the EmbeddedTask when Execute is called, not here
	t.EmbeddedTask = nil

	return nil
}
func (t *RetryableTask) GenerateRandomString(length int) (string, error) {
	randomBytes := make([]byte, (length*3+3)/4) // Adjust the byte slice length to ensure enough bytes for base64 encoding
	_, err := rand.Read(randomBytes)
	if err != nil {
		return "", err
	}

	randStr := base64.URLEncoding.EncodeToString(randomBytes)[:length]

	// Remove any unwanted characters (e.g., hyphens and underscores)
	randStr = strings.Map(func(r rune) rune {
		if r == '-' || r == '_' {
			return -1
		}
		return r
	}, randStr)

	// If the result is shorter than requested length, regenerate
	for len(randStr) < length {
		extraBytes := make([]byte, (length-len(randStr))*3/4+1)
		_, err := rand.Read(extraBytes)
		if err != nil {
			return "", err
		}
		extraStr := base64.URLEncoding.EncodeToString(extraBytes)
		randStr += strings.Map(func(r rune) rune {
			if r == '-' || r == '_' {
				return -1
			}
			return r
		}, extraStr)
	}

	return randStr[:length], nil
}

// Helper method to encode task data into JSON and save it
func EncodeTaskData(data interface{}) (string, error) {
	if data == nil {
		return "", nil
	}

	encoded, err := json.Marshal(data)
	if err != nil {
		return "", err
	}

	return string(encoded), nil
}

// Helper method to decode task data from JSON
func DecodeTaskData(encoded string, target interface{}) error {
	if encoded == "" {
		return nil // Nothing to decode
	}

	return json.Unmarshal([]byte(encoded), target)
}

// CreateTaskFactoryWithDecoder creates a factory function that can reconstruct tasks from stored data
// This helps client code avoid having to deal with marshaling/unmarshaling
func CreateTaskFactoryWithDecoder[P any](creator func(taskID string, payload P) Task) TaskFactory {
	return func(taskID string, data string) (Task, error) {
		var payload P
		if data != "" {
			if err := DecodeTaskData(data, &payload); err != nil {
				return nil, fmt.Errorf("failed to decode task data: %w", err)
			}
		}
		return creator(taskID, payload), nil
	}
}

// Save a task to the database
func (t *RetryableTask) Save() error {
	fmt.Println("Saving task:", t)
	var err error
	t.TaskID, err = t.GenerateRandomString(12)
	if err != nil {
		return fmt.Errorf("error generating task ID: %w", err)
	}

	fmt.Println("Generated task ID:", t.TaskID)
	t.CreatedAt = time.Now()
	t.UpdatedAt = t.CreatedAt
	t.DeletedAt = nil
	fileStore, err := NewFileStore(filePath)
	if err != nil {
		return err
	}
	fmt.Println("Saving task to file store:", t)
	if err := fileStore.CreateTask(t); err != nil {
		return err
	}
	fmt.Println("Saved task to file store:", t)
	return nil
}

// FetchDueTasks gets all tasks that are due for execution based on RetryAfter time
func FetchDueTasks() ([]RetryableTask, error) {
	var tasks []RetryableTask
	currentTime := time.Now()

	// Get all non-deleted tasks
	fileStore, err := NewFileStore(filePath)
	if err != nil {
		return nil, err
	}
	allTasks, err := fileStore.ReadTasks()
	if err != nil {
		return nil, err
	}

	// Only include tasks that are due (retry_after <= current time)
	for _, t := range allTasks {
		diff := currentTime.Sub(t.RetryAfterTime)
		if currentTime.After(t.RetryAfterTime) || diff >= 0 {

			tasks = append(tasks, *t)
		}
	}

	// Only log when tasks are found to reduce noise
	if len(tasks) > 0 {
		fmt.Printf("Found %d tasks to process\n", len(tasks))
	}

	return tasks, nil
}

// DeleteTask removes a task from the database by its TaskID
func DeleteTask(taskId string) error {
	fileStore, err := NewFileStore(filePath)
	if err != nil {
		return err
	}
	if err := fileStore.DeleteTask(taskId); err != nil {
		return err
	}
	return nil
}

// GetRegisteredTaskFactory returns a task factory for a specific task type
func GetRegisteredTaskFactory(retryableTask RetryableTask) (func(id string, data string) (Task, error), bool) {
	factory, exists := taskFactories["any-task"]
	if !exists {
		fmt.Println("YES No factory found for task type: any-task")
		return nil, false
	}
	fmt.Println("YES Found factory for task type: any-task")
	// Create a new factory
	return func(id string, data string) (Task, error) {
		// Handle both empty and non-empty task data
		task, err := factory(id, data)
		if err != nil {
			// For debugging purposes
			fmt.Printf("Error in factory for task %s: %v, data: '%s'\n", id, err, data)
			return nil, err
		}

		// Set the RetryConfig fields if the task implements a setter for it
		if retryCountSetter, ok := task.(interface{ SetRetryConfigFields(int, int, float64) }); ok {
			retryCountSetter.SetRetryConfigFields(retryableTask.RetryCount, retryableTask.MaxRetries, retryableTask.RetryAfterHours)
			fmt.Printf("Set retry count for task %s to %d\n", id, retryableTask.RetryCount)
		}

		return task, nil
	}, true
}

// UpdateTaskRetryConfig updates a task's retry configuration in the database
func (t *RetryableTask) UpdateTaskRetryConfig(taskId string) error {
	// This is the legacy method without error tracking
	return t.UpdateTaskRetryConfigWithError(taskId, nil)
}

// UpdateTaskRetryConfigWithError updates a task's retry configuration and stores error information
func (t *RetryableTask) UpdateTaskRetryConfigWithError(taskId string, errorObj error) error {
	// Create a file store using the hidden .snerdata folder
	fileStore, err := NewFileStore(filePath)
	if err != nil {
		return err
	}

	// Call the updated method with error information
	if err := fileStore.UpdateTaskRetryConfig(taskId, errorObj); err != nil {
		return err
	}
	return nil
}

// Map of task type IDs to factory functions
var taskFactories = make(map[string]func(id string, data string) (Task, error))

// RegisterTaskType registers a task type with a factory function
// This allows tasks to be recreated from their saved data
func RegisterTaskType[P any](
	taskType string,
	creator func(id string, payload P) Task,
) {
	taskFactories[taskType] = func(id string, data string) (Task, error) {
		var payload P
		// Only try to unmarshal if we actually have data
		if data != "" {
			if err := json.Unmarshal([]byte(data), &payload); err != nil {
				return nil, fmt.Errorf("error unmarshaling task data: %w", err)
			}
		} else {
			fmt.Println("Empty task data, skipping unmarshal for task type:", taskType)
		}
		return creator(id, payload), nil
	}
}

// SerializeTaskPayload serializes a task payload for storage
// This allows client code to create task data without knowing serialization details
func SerializeTaskPayload[P any](payload P) (string, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to serialize task payload: %w", err)
	}
	return string(data), nil
}

// CreateTaskWithPayload creates a new task with the provided payload
func CreateTaskWithPayload[P any](
	taskID string,
	taskType string,
	payload P,
	maxRetries int,
	retryAfterHours int,
) (*RetryableTask, error) {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("error marshaling payload: %w", err)
	}

	data := string(payloadJSON)

	// Ensure RetryAfter is in UTC for consistency
	retryAfterTime := time.Now().Add(time.Duration(retryAfterHours) * time.
		Hour)
	task := &RetryableTask{
		TaskID:          taskID,
		RetryCount:      0,
		MaxRetries:      maxRetries,
		RetryAfterHours: float64(retryAfterHours),
		RetryAfterTime:  retryAfterTime,
		TaskData:        data,
		TaskType:        taskType,
		ExecuteAt:       time.Now().UTC(),
	}
	if err := task.Save(); err != nil {
		return nil, err
	}
	return task, nil
}

// InitFunction is a function that registers task types
type InitFunction func()

var (
	registeredInitFunctions []InitFunction
	initMutex               sync.Mutex
)

// RegisterInitFunction registers a function to be called when ensuring task types are registered
// This allows packages to register their task types without circular dependencies
func RegisterInitFunction(fn InitFunction) {
	initMutex.Lock()
	defer initMutex.Unlock()
	registeredInitFunctions = append(registeredInitFunctions, fn)
}

// EnsureTaskTypesRegistered ensures that all known task types are registered
// This is called before processing tasks to prevent the "no factory registered" error
func EnsureTaskTypesRegistered() {
	initMutex.Lock()
	defer initMutex.Unlock()

	// Call all registered init functions
	for _, fn := range registeredInitFunctions {
		fn()
	}
}
