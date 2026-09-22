package snerd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

// dashboardProgressEvent is a single entry in the dashboard's Progress Stream.
type dashboardProgressEvent struct {
	Ts     float64 `json:"ts"`
	TaskID string  `json:"task_id"`
	Data   string  `json:"data"`
}

// progressRing is a bounded, thread-safe buffer of recent progress events.
type progressRing struct {
	mu     sync.Mutex
	events []dashboardProgressEvent
	cap    int
}

func (r *progressRing) append(ev dashboardProgressEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
	if len(r.events) > r.cap {
		r.events = r.events[len(r.events)-r.cap:]
	}
}

func (r *progressRing) latest(n int) []dashboardProgressEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n > len(r.events) {
		n = len(r.events)
	}
	out := make([]dashboardProgressEvent, n)
	copy(out, r.events[len(r.events)-n:])
	return out
}

// tasksLogPath returns the path of the append-only task log this queue persists to.
func (q *AnyQueue) tasksLogPath() string {
	if q.fileStore != nil {
		return q.fileStore.filePath
	}
	return "./.snerdata/tasks/tasks.log"
}

// readDedupedTasks reads the append-only task log and returns the latest
// line per taskId (cron refires and retries append new lines over time).
func readDedupedTasksMultiple(tasksPaths []string) map[string]map[string]interface{} {
	tasksMap := make(map[string]map[string]interface{})
	for _, tasksPath := range tasksPaths {
		file, err := os.Open(tasksPath)
		if err != nil {
			continue
		}
		
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				continue
			}
			var t map[string]interface{}
			if json.Unmarshal([]byte(line), &t) == nil {
				if tid, ok := t["taskId"].(string); ok {
					tasksMap[tid] = t
				}
			}
		}
		file.Close()
	}
	return tasksMap
}

func readDedupedTasks(tasksPath string) map[string]map[string]interface{} {
	return readDedupedTasksMultiple([]string{tasksPath})
}



// zeroTime is how Go serializes a zero-value time.Time; treat it as absent.
const zeroTime = "0001-01-01T00:00:00Z"

func isSetTime(v interface{}) bool {
	s, ok := v.(string)
	return ok && s != "" && s != zeroTime
}

// hasJobError reports whether a LastJobError is actually set. snerd-go writes
// "LastJobError": null on error-free records, so key presence alone is not enough.
func hasJobError(t map[string]interface{}) bool {
	if v, ok := t["LastJobError"]; ok && v != nil {
		return true
	}
	// Tolerate the lowercase variant in case logs were written by older builds
	if v, ok := t["lastJobError"]; ok && v != nil {
		return true
	}
	return false
}

// dashboardStatus derives the UI status for a deduped task record.
func dashboardStatus(t map[string]interface{}) string {
	hasErr := hasJobError(t)
	if isSetTime(t["deletedAt"]) {
		rtCount, _ := t["retryCount"].(float64)
		maxRt, _ := t["maxRetries"].(float64)
		if hasErr && rtCount >= maxRt {
			return "dead_letter"
		} else if hasErr {
			return "failed"
		}
		return "completed"
	}
	if hasErr {
		return "failed"
	}
	execAt, _ := t["executeAt"].(string)
	if execAt != "" && execAt != zeroTime {
		if et, err := time.Parse(time.RFC3339Nano, execAt); err == nil && !et.After(time.Now()) {
			return "active"
		}
	}
	return "queued"
}

// StartDashboard starts the built-in dashboard UI on the given port.
//
// The dashboard is a single-page React app (served from ./static/index.html
// relative to the process working directory) that shows live queue stats,
// a Recent Jobs table, and a real-time Progress Stream fed by YieldProgress.
// Updates are delivered via HTTP polling of the JSON API (/api/stats,
// /api/tasks, /api/progress).
//
// The dashboard only serves the UI — jobs keep running whether or not it is open.
func (q *AnyQueue) StartDashboard(port int) {
	ring := &progressRing{cap: 500}

	// Feed the progress stream from the queue's internal broadcast channel
	sub := q.SubscribeProgress()
	go func() {
		for chunk := range sub {
			var ev struct {
				TaskID string `json:"task_id"`
				Data   string `json:"data"`
			}
			if json.Unmarshal([]byte(chunk), &ev) != nil {
				continue
			}
			ring.append(dashboardProgressEvent{
				Ts:     float64(time.Now().UnixNano()) / float64(time.Second),
				TaskID: ev.TaskID,
				Data:   ev.Data,
			})
		}
	}()

	mux := http.NewServeMux()
	tasksPath := q.tasksLogPath()

	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")

		tasksMap := readDedupedTasks(tasksPath)
		enqueued, processed, failed := 0, 0, 0
		for _, t := range tasksMap {
			enqueued++
			if isSetTime(t["deletedAt"]) {
				if hasJobError(t) {
					failed++
				} else {
					processed++
				}
			}
		}
		fmt.Fprintf(w, `{"enqueued":%d,"processed":%d,"failed":%d}`, enqueued, processed, failed)
	})

	mux.HandleFunc("/api/tasks", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")

		tasksMap := readDedupedTasks(tasksPath)
		res := make([]map[string]interface{}, 0, len(tasksMap))
		for _, t := range tasksMap {
			rtCount, _ := t["retryCount"].(float64)
			maxRt, _ := t["maxRetries"].(float64)
			rtAfter, _ := t["retryAfterTime"].(string)

			res = append(res, map[string]interface{}{
				"id":                  t["taskId"],
				"type":                t["taskType"],
				"status":              dashboardStatus(t),
				"progress":            0,
				"retryCount":          rtCount,
				"maxRetries":          maxRt,
				"retryAfterTime":      rtAfter,
				"cronExpression":      t["cronExpression"],
				"webhookUrl":          t["webhookUrl"],
				"maxExecutionSeconds": t["maxExecutionSeconds"],
			})
		}
		json.NewEncoder(w).Encode(res)
	})

	mux.HandleFunc("/api/progress", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ring.latest(100))
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		data, err := os.ReadFile("static/index.html")
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, "Dashboard UI not found: place the dashboard bundle at ./static/index.html")
			return
		}
		w.Header().Set("Content-Type", "text/html")
		w.Write(data)
	})

	fmt.Printf("[Snerd] Dashboard running on http://localhost:%d\n", port)
	go func() {
		if err := http.ListenAndServe(fmt.Sprintf(":%d", port), mux); err != nil {
			fmt.Printf("[Snerd] Dashboard server error: %v\n", err)
		}
	}()
}

func (sq *ShardedQueue) StartDashboard(port int) {
	ring := &progressRing{cap: 500}

	sub := sq.SubscribeProgress()
	go func() {
		for chunk := range sub {
			var ev struct {
				TaskID string `json:"task_id"`
				Data   string `json:"data"`
			}
			if json.Unmarshal([]byte(chunk), &ev) != nil {
				continue
			}
			ring.append(dashboardProgressEvent{
				Ts:     float64(time.Now().UnixNano()) / float64(time.Second),
				TaskID: ev.TaskID,
				Data:   ev.Data,
			})
		}
	}()

	mux := http.NewServeMux()

	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")

		var paths []string
		for _, q := range sq.GetShards() {
			paths = append(paths, q.tasksLogPath())
		}
		tasksMap := readDedupedTasksMultiple(paths)
		enqueued, processed, failed := 0, 0, 0
		for _, t := range tasksMap {
			enqueued++
			if isSetTime(t["deletedAt"]) {
				if hasJobError(t) {
					failed++
				} else {
					processed++
				}
			}
		}
		fmt.Fprintf(w, `{"enqueued":%d,"processed":%d,"failed":%d}`, enqueued, processed, failed)
	})

	mux.HandleFunc("/api/tasks", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")

		var paths []string
		for _, q := range sq.GetShards() {
			paths = append(paths, q.tasksLogPath())
		}
		tasksMap := readDedupedTasksMultiple(paths)
		res := make([]map[string]interface{}, 0, len(tasksMap))
		for _, t := range tasksMap {
			rtCount, _ := t["retryCount"].(float64)
			maxRt, _ := t["maxRetries"].(float64)
			rtAfter, _ := t["retryAfterTime"].(string)

			res = append(res, map[string]interface{}{
				"id":                  t["taskId"],
				"type":                t["taskType"],
				"status":              dashboardStatus(t),
				"progress":            0,
				"retryCount":          rtCount,
				"maxRetries":          maxRt,
				"retryAfterTime":      rtAfter,
				"cronExpression":      t["cronExpression"],
				"webhookUrl":          t["webhookUrl"],
				"maxExecutionSeconds": t["maxExecutionSeconds"],
			})
		}
		json.NewEncoder(w).Encode(res)
	})

	mux.HandleFunc("/api/progress", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ring.latest(100))
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		data, err := os.ReadFile("static/index.html")
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, "Dashboard UI not found: place the dashboard bundle at ./static/index.html")
			return
		}
		w.Header().Set("Content-Type", "text/html")
		w.Write(data)
	})

	fmt.Printf("[Snerd] Dashboard running on http://localhost:%d\n", port)
	go func() {
		if err := http.ListenAndServe(fmt.Sprintf(":%d", port), mux); err != nil {
			fmt.Printf("[Snerd] Dashboard server error: %v\n", err)
		}
	}()
}
