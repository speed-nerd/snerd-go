<div align="center">
  <img src="./assets/Designer-9.png" height="120" alt="Snerd-Go Logo" />
  <h1>⚙️ snerd-go v0.3.0</h1>
  <p>A blazingly fast, brutally simple, zero-infrastructure embedded background job engine for Go.</p>

  [![Go Reference](https://pkg.go.dev/badge/github.com/speed-nerd/snerd-go.svg)](https://pkg.go.dev/github.com/speed-nerd/snerd-go)
  [![Docs](https://img.shields.io/badge/docs-speed--nerd.github.io-blue)](https://speed-nerd.github.io/docs/)
</div>

> 📝 **What's New in v0.3.0?** Check out the [Changelog & Releases](https://speed-nerd.github.io/docs/changelog) for the latest features including Sharded Queues, Worker Pools, and Job Chaining!

If you are tired of wrestling with heavy, bloated background job frameworks like Redis, Postgres tables, or RabbitMQ just to send a few emails in the background... well, you are in the right place.

`snerd-go` is an embedded, high-performance background task queue that lives entirely in a single, perfectly OS-locked, append-only `.log` file on your file system. It was designed to bring aggressive concurrency and a lightweight footprint to your Go microservices.

No databases. No external daemons. No nonsense.

---

## 🔥 Features

* **Zero External Infrastructure**: You don't need a Redis cluster. Your tasks are persisted directly to `.snerdata/tasks/tasks.log` using standard filesystem I/O.
* **Built-in Web Dashboard**: A one-line `StartDashboard(port)` serves a live React UI with queue stats, job table, and a real-time progress stream.
* **Bulletproof File Locks**: Safely scales across multiple processes! We utilize OS-level file-locking boundaries to guarantee that your tasks are never corrupted.
* **Smart API Rate-Limiting**: Natively tracks `rateLimitGroup` execution velocity to prevent 429 "Too Many Requests" API errors.
* **Payload-Hashing Deduplication**: Automatically computes cryptographic hashes to drop duplicate tasks instantly.
* **Dynamic Float Prioritization**: A native Binary Max-Heap bypasses standard FIFO rules for high urgency tasks.
* **Cron, Webhooks & Hard Timeouts**: Recurring schedules, serverless HTTP execution, and per-task execution timeouts.
* **Progress Streaming**: Handlers can emit live progress events that stream straight into the dashboard.
* **Dead-Letter Queue (DLQ)**: Built-in `maxRetries` limits and hooks to elegantly catch and bury poison-pill tasks.
* **Sharded Queues**: Distribute load across multiple queue nodes safely using file-backed lock sharding (`snerd.NewShardedQueue`).
* **Worker Pools**: Prevent slow tasks from starving fast tasks by dedicating workers to specific pools (`snerd.NewAnyQueueWithPools`).
* **Job Chaining**: Sequence tasks as DAGs using `triggerAfterIds`.

---

## 📦 Installation

```bash
go get github.com/speed-nerd/snerd-go
```

---

## 🚀 Quickstart (Basic)

It takes roughly 3 lines of code to spin up a queue and start firing background jobs.

```go
package main

import (
	"context"
	"fmt"
	"time"

	snerd "github.com/speed-nerd/snerd-go"
)

func main() {
	// 1. Create the Queue (name, max size, processor poll interval)
	// For local development this persists to ./.snerdata/tasks/tasks.log
	queue := snerd.NewAnyQueue("my-fast-queue", 10, 2*time.Second)

	// Need a custom location instead? (durable network-drive storage, per-server
	// isolation, or keeping tests out of .snerdata)
	// queue := snerd.NewAnyQueueWithStorage("my-fast-queue", 10, 2*time.Second, "/var/data/snerd/tasks.log")

	// 2. Register your Task Handler (the closure that does the actual work)
	snerd.RegisterTaskHandler("generate_ai_image", func(ctx context.Context, parameters string) error {
		fmt.Printf("Generating image with payload: %s\n", parameters)

		// do your heavy lifting here!
		// return fmt.Errorf("...") to trigger a retry!
		return nil
	})

	// 3. (Optional) Register a Dead-Letter Handler for when retries run out
	snerd.RegisterMaxRetryHandler("generate_ai_image", func(ctx context.Context, parameters string) error {
		fmt.Printf("Task permanently failed! Payload: %s\n", parameters)
		return nil
	})

	// 4. Enqueue a task! (maxRetries=3, retryAfterHours=1.0)
	task, _ := snerd.NewSnerdTask(
		"unique-task-id-123",                    // Unique task ID
		"generate_ai_image",                     // Task type (matches handler)
		map[string]string{"prompt": "A crab in space"}, // JSON payload
		3,    // Max retries
		1.0,  // Delay in hours before a failed task is retried
	)
	queue.EnqueueSnerdTask(task)

	// Keep your app alive — jobs run in background goroutines
	select {}
}
```

---

## ⚙️ Advanced Task Configuration

To power complex workflows, tasks can be configured with advanced orchestration parameters via `NewSnerdTaskAdvanced` (pass `nil` for anything you don't need):

```go
rateLimitGroup := "openai_api"
maxPerMinute := 50
autoDedupe := true
urgencyScore := 0.95
cronStr := "1h"
pool := "urgent"
triggerAfterIds := []string{"parent-job-123"}

task, _ := snerd.NewSnerdTaskAdvanced(
	"unique-task-id-123",  // Unique task ID
	"generate_ai_image",   // Task type (matches handler)
	map[string]string{"prompt": "A crab in space"}, // JSON payload
	3,                     // Max retries
	1.0,                   // Delay in hours before a failed task is retried
	&rateLimitGroup,       // Rate limit group
	&maxPerMinute,         // Max executions per minute for that group
	&autoDedupe,           // Auto-dedupe identical payloads
	&urgencyScore,         // Urgency score (higher floats to the front)
	nil,                   // executeAt — RFC3339 timestamp for delayed execution
	&cronStr,              // Cron — recurring job, runs every 1 hour
	nil,                   // webhookUrl — HTTP execution instead of a local handler
	nil,                   // maxExecutionSeconds — hard timeout
	&pool,                 // pool — Dedicate this task to a specific worker pool
	triggerAfterIds,       // triggerAfterIds — Wait for other tasks to successfully complete
)
queue.EnqueueSnerdTask(task)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `maxRetries` | `int` | — | How many times a failed task is retried before hitting the Dead Letter Queue. |
| `retryAfterHours` | `float64` | — | Backoff in **hours** before a failed task is retried (e.g. `0.001` ≈ seconds). |
| `autoDedupe` | `*bool` | `nil` | If `true`, a cryptographic hash of `taskType` + `parameters` is computed. If an identical payload is already pending, the new task is silently dropped. |
| `urgencyScore` | `*float64` | `nil` | A value (e.g. `0.99`) used to bypass the standard FIFO queue. A Binary Max-Heap floats high urgency tasks to the front. |
| `rateLimitGroup` | `*string` | `nil` | A custom string (e.g. `"openai_api"`) that groups tasks together for backpressure control. |
| `maxPerMinute` | `*int` | `nil` | Used with `rateLimitGroup`. If the group exceeds this limit in a 60-second rolling window, further tasks in the group pause for a minute — natively preventing 429 errors. |
| `executeAt` | `*string` | `nil` | An RFC3339 timestamp of when the job should first run (delayed execution). |
| `cron` | `*string` | `nil` | A cron expression for recurring jobs: standard 5-field (`"0 * * * *"`), 6-field with seconds (`"*/10 * * * * *"`), or shorthands `"30s"`, `"10m"`, `"2h"`, `"1d"`. |
| `webhookUrl` | `*string` | `nil` | Optional webhook URL — the payload is dispatched via HTTP POST instead of a local handler. |
| `maxExecutionSeconds` | `*int` | `nil` | Optional hard timeout in seconds (see below). |
| `pool` | `*string` | `nil` | Dedicate this task to a specific worker pool (e.g. `"urgent"`). Use `NewAnyQueueWithPools` to allocate workers per pool. |
| `triggerAfterIds` | `[]string` | `nil` | Wait for other tasks (by `task_id`) to successfully complete before executing this task. |

### ⏱️ Note on Hard Timeouts (`maxExecutionSeconds`)
When `maxExecutionSeconds` is provided, the engine executes your handler with a `context.WithTimeout`. If the task takes longer than the timeout, the context is cancelled. **If your handler respects context cancellation** (select on `ctx.Done()`), it will terminate early and the execution is marked as failed and retried:

```go
snerd.RegisterTaskHandler("slow-job", func(ctx context.Context, parameters string) error {
	select {
	case <-time.After(10 * time.Minute): // the actual work
		return nil
	case <-ctx.Done():
		return ctx.Err() // gives up promptly when the timeout trips
	}
})
```

### 🌐 HTTP Webhooks (Serverless Execution)
You can configure a task to execute externally via an HTTP POST request. By setting a `webhookUrl`, the background processor skips any registered handlers and directly invokes the HTTP endpoint with the payload and the header `X-SnerdMQ-Event: Execute`.

If the endpoint returns a non-2xx status code, it triggers a retry. If it permanently fails (reaches `maxRetries`), the Dead Letter Queue event is automatically fired via a final HTTP POST to the same `webhookUrl` with the header `X-SnerdMQ-Event: MaxRetriesReached`.

### 🕒 Cron Jobs vs. Retryable Jobs
When using the scheduling features, it is important to understand the difference between Cron and Retry behaviors:
> - **A Cron Job** is a *Repeatable Job* that executes again **only after a success**, on a fixed schedule.
> - **A Retryable Job** is a *Recovery Job* that executes again **only after a failure**, attempting to recover using the `retryAfterHours` backoff.
> - **Combined:** If a Cron Job fails, it temporarily uses `retryAfterHours` to retry until it recovers. Once it succeeds, it goes back to ticking on its standard cron schedule!

### ☠️ Dead Letter Queue (Handling Permanent Failures)
The DLQ captures tasks that have exhausted all `maxRetries`. Define a custom handler with `snerd.RegisterMaxRetryHandler(taskType, handler)` — critical for alerting or manual intervention when a background process consistently fails.

### 📁 Custom Storage Location
By default, tasks persist to `.snerdata/tasks/tasks.log`. To use a different file — isolating queues per concern, pointing at a network drive (EFS/NFS) for durable storage, or keeping tests clean — use `NewAnyQueueWithStorage`:

```go
// Same semantics as NewAnyQueue, plus an explicit task log path
queue := snerd.NewAnyQueueWithStorage("image-processing", 10, 2*time.Second, "/mnt/efs/image-jobs/tasks.log")
```

The rate limiter state file (`rate_limits.json`) is stored alongside the task log, so two queues on different paths are fully independent — including their dashboards.

**One queue instance per storage file.** Each queue takes an exclusive OS-level lock on its task log (e.g. `tasks.log.lock`) at creation. A second queue on the same file fails fast with a panic instead of racing it and double-executing tasks — so register all your task types on a single queue, or give each queue its own path.

---

## 📊 Live Dashboard

`snerd-go` ships with a built-in **React UI dashboard** served directly by the library — no extra services or dependencies required. It gives you a real-time window into your queue:

- **Live stats**: total enqueued, processed, and failed jobs
- **Recent Jobs table**: per-task status (`queued`, `active`, `completed`, `failed`, `dead_letter`), retry counts, and badges showing which features a task uses (cron / webhook / timeout)
- **Real-time Progress Stream**: live output from `YieldProgress` calls in your handlers

```go
queue := snerd.NewAnyQueue("my-queue", 10, 2*time.Second)

// Start the built-in dashboard on http://localhost:9090
queue.StartDashboard(9090)
```

Then open **http://localhost:9090** in your browser. The page polls a small JSON API exposed by the library — also handy if you want to build your own tooling on top:

| Endpoint | Returns |
|---|---|
| `/api/stats` | `{"enqueued": N, "processed": N, "failed": N}` |
| `/api/tasks` | All jobs with status, retries, cron, webhook, timeout info |
| `/api/progress` | The last 100 progress events (`{ts, task_id, data}`) |

**Serving the UI:** the dashboard page is the single file `static/index.html`, resolved relative to your process's working directory. The bundle ships with this repo under `static/` — run your binary from the directory that contains the `static/` folder (or copy the folder next to your binary).

> **Note:** `StartDashboard` only serves the UI — your jobs keep running whether or not the dashboard is open.

---

## 📡 Progress Reporting

Long-running handlers can stream live updates to the Dashboard's Progress Stream (ideal for streaming LLM tokens or multi-step ETL work):

```go
snerd.RegisterTaskHandler("generate_report", func(ctx context.Context, parameters string) error {
	for step := 1; step <= 10; step++ {
		doWork(step)
		queue.YieldProgress("report-task-1", fmt.Sprintf("Step %d/10 complete", step))
	}
	return nil
})
```

You can also subscribe to the raw progress feed from your own code (each message is a JSON string with `task_id` and `data`):

```go
for msg := range queue.SubscribeProgress() {
	fmt.Println("progress:", msg)
}
```

---

## 🧩 Queue Topology: One Queue or Many?

### ✅ Recommended: one queue, all job types (singleton)

The recommended pattern is **one queue instance per application**: register every job type on it and serve a single shared dashboard:

```go
package main

import (
	"context"
	"fmt"
	"time"

	snerd "github.com/speed-nerd/snerd-go"
)

func main() {
	// ONE queue for the whole app (persists to ./.snerdata/tasks/tasks.log)
	queue := snerd.NewAnyQueue("main", 10, 2*time.Second)

	// Job type #1: image processing
	snerd.RegisterTaskHandler("process_image", func(ctx context.Context, data string) error {
		fmt.Printf("Processing image: %s\n", data)
		return nil
	})

	// Job type #2: OTP emails — same queue
	snerd.RegisterTaskHandler("send_otp_email", func(ctx context.Context, data string) error {
		fmt.Printf("Sending OTP: %s\n", data)
		return nil
	})

	// Both job types flow through the exact same queue
	imgTask, _ := snerd.NewSnerdTask("img-1", "process_image", map[string]string{"image_id": "abc123"}, 3, 0.5)
	queue.EnqueueSnerdTask(imgTask)

	otpTask, _ := snerd.NewSnerdTask("otp-1", "send_otp_email", map[string]string{"to": "john@wick.com"}, 3, 0.5)
	queue.EnqueueSnerdTask(otpTask)

	// ONE dashboard shows every job type
	queue.StartDashboard(9090)

	select {} // keep the process alive — jobs run in background goroutines
}
```

All job types share everything: the same persistent job log, retry/DLQ pipeline, rate-limit state, stats — and one dashboard at `http://localhost:9090` showing all of them.

### 🚫 Same storage twice = fails fast

Each queue takes an **exclusive OS-level lock** on its task log at creation. A second queue on the same storage fails instead of silently double-executing your tasks:

```go
first := snerd.NewAnyQueue("main", 10, 2*time.Second)   // ✅ owns .snerdata/tasks/tasks.log
second := snerd.NewAnyQueue("other", 10, 2*time.Second) // ❌ panics:
// "[Snerd] ERROR: Another queue instance is already running on storage ..."
```

This applies across processes too — a second process pointed at the same log file also fails to start its queue.

### 🔀 Need multiple queues? Give each one its own storage

```go
images := snerd.NewAnyQueueWithStorage("images", 10, 2*time.Second, "./.snerdata-images/tasks.log")
emails := snerd.NewAnyQueueWithStorage("emails", 10, 500*time.Millisecond, "./.snerdata-emails/tasks.log")

images.StartDashboard(9090) // separate dashboards, so separate ports
emails.StartDashboard(9091)
```

Now you have two fully independent engines: separate job logs, separate rate-limit state, separate dashboards. Only split when you actually need isolation (different cadence, different retention, independent monitoring) — otherwise the singleton is simpler and recommended.

---

## 🌍 Advanced: Distributed Scaling

A queue instance exclusively owns its storage file: it takes an OS-level lock (`<tasks.log>.lock`) at creation and holds it for its lifetime. A second instance pointed at the same file — in the same process or on another server — fails fast instead of racing it and double-executing tasks.

Scaling out therefore means **one queue per server**, each with its own storage. Your load balancer routes requests across servers, and every server processes the tasks it enqueued:

```go
// Each server runs its own queue on its own log file (local disk works fine)
queue := snerd.NewAnyQueueWithStorage("worker-server-1", 10, 2*time.Second, "/var/data/snerd/tasks.log")
```

A shared network drive (AWS EFS or NFS) is still a good home for that log when a single instance needs durable storage — e.g. a container that restarts but must keep its queue state. OS-level file locking keeps writes safe — no Redis required.

### 🍕 Embedded Sharding (Multi-Process Scaling)

If you have multiple instances of your Go app running on the **same machine** (e.g., behind a local load balancer) and you want them to share the processing load, you can use the built-in **Sharded Queue**. This transparently partitions tasks across multiple `.log` files and coordinates lease-based ownership via a background heartbeat.

```go
package main

import (
    "context"
    "fmt"
    snerd "github.com/speed-nerd/snerd-go"
)

func main() {
    // 1. Create a Sharded Queue requesting 4 partitions
    sharded, err := snerd.NewShardedQueue("main", "./.snerdata", 4)
    if err != nil {
        panic(err)
    }
    
    // 2. Register handlers globally (they replicate to all shards)
    snerd.RegisterTaskHandler("process_image", func(ctx context.Context, data string) error {
        fmt.Println("Processing:", data)
        return nil
    })
    
    // 3. Enqueue tasks (they are hashed to a specific shard automatically)
    task := snerd.NewSnerdTask("id-1", "process_image", "{}", 3, 1.0, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
    sharded.Enqueue(task)
    
    // 4. Start the dashboard (it aggregates all owned shards!)
    sharded.StartDashboard(9090)

    // 5. Gracefully shutdown and release the OS file locks on exit
    defer sharded.Shutdown()
    
    select {} // keep alive
}
```

> **Note:** We strongly recommend keeping sharded processes on a single physical machine with a local disk. Using embedded sharding across NFS/EFS is not officially supported due to clock drift and unreliable `flock` implementations over network mounts.

> **Concurrency Limits:** `ShardedQueue` automatically uses a **Shared Worker Pool**. If you request 10 shards, the engine will still strictly enforce your maximum concurrency limits (default: 100 concurrent workers) process-wide, dynamically shifting workers to whichever shard has due tasks.

---

## 🔧 Queue API Reference

| API | Description |
|---|---|
| `snerd.NewAnyQueue(args ...interface{})` | Create a queue. Variadic options: `string` = name (default `"default-queue"`), `int` = max size (default `100`), `time.Duration` = processor poll interval (default `10s`). Persists to `.snerdata/tasks/tasks.log`. Panics if another queue instance already owns that file. |
| `snerd.NewAnyQueueWithStorage(name, maxSize, interval, storePath)` | Create a queue with an explicit task log file location instead of the default `.snerdata/tasks/tasks.log`. Panics if another queue instance already owns that file. |
| `snerd.NewAnyQueueWithPools(...)` | Create a queue that natively isolates concurrent executions by providing a map of pool names to max worker counts. |
| `snerd.NewShardedQueue(...)` | Create a clustered, multi-node queue that automatically shards the `.snerdata` file locks so you can run multiple concurrent processes safely on the same storage directory. |
| `queue.EnqueueSnerdTask(task)` / `queue.Enqueue(task)` | Enqueue a task. Due tasks execute immediately in background goroutines; the rest are picked up by the processor loop. |
| `snerd.RegisterTaskHandler(type, handler)` | Register `func(ctx context.Context, parameters string) error` for a task type. |
| `snerd.RegisterMaxRetryHandler(type, handler)` | Register the Dead-Letter handler for a task type. |
| `queue.StartDashboard(port int)` | Serve the built-in dashboard UI on the given port. |
| `queue.YieldProgress(taskID, data)` | Emit a progress event (dashboard Progress Stream / `SubscribeProgress`). |
| `queue.SubscribeProgress()` | Receive progress events as JSON strings on a channel. |
| `queue.StopProcessor()` | Stop the background polling loop. |
| `queue.ProcessDueTasks()` | Manually trigger one processing sweep. |
| `queue.Name()`, `queue.Size()`, `queue.RemainingCapacity()`, `queue.TotalEnqueued()`, `queue.TotalProcessed()` | Queue inspection helpers. |

---

## 🧠 Architecture Details

`snerd-go` utilizes an **Append-Only Log Model** to achieve massive write speeds.
Instead of updating rows in a database, every time a task is enqueued, updated, or deleted, a brand new JSON line is instantly appended to the end of the log file.

When the queue wakes up on its polling interval, it scans the log, maps out the absolute latest state of every task, and spawns parallel goroutines for anything that is currently due (`executeAt <= now` and `retryAfterTime <= now`).

If your file ever grows too large, `snerd-go` atomically clones, shrinks, and replaces the file in the background (Log Compaction) to keep disk space minimal.

---

## 🤝 License

MIT License. Do whatever you want with it, just don't let your tasks die unhandled.


## Advanced Orchestration (v0.3.0 Features)

SnerdMQ v0.3.0 introduced powerful new primitives for managing complex background jobs natively in the core engine. Below are examples of how to utilize these features when embedding SnerdMQ in Go:

### 🍕 Sharded Queues (Scaling Out)

SnerdMQ natively supports distributed execution across multiple servers while acting as a single logical queue. Just mount a shared storage drive (like AWS EFS) and boot multiple daemons. They will automatically lock and negotiate ownership of shards. Just tell the queue how many shards to claim on boot.

```go
// Boot a multi-tenant daemon that owns up to 4 shards locally
queue, err := snerd.NewShardedQueue("my-queue", "/var/data/snerd", 4)
```

### 🏊 Worker Pools

```go
// Route an AI task to a dedicated pool
aiPool := "ai-pool"
aiTask := &snerd.SnerdTask{
    TaskID: "ai-1",
    TaskType: "ai_generation",
    Parameters: `{"prompt":"horse"}`,
    Pool: &aiPool,
}
queue.EnqueueSnerdTask(aiTask)

// Route an email task to a fast, urgent pool
urgentPool := "urgent"
emailTask := &snerd.SnerdTask{
    TaskID: "email-1",
    TaskType: "send_email",
    Parameters: `{"to":"user@a.com"}`,
    Pool: &urgentPool,
}
queue.EnqueueSnerdTask(emailTask)
```

### 🔗 Job Chaining (DAGs)

```go
// Step 1: Transcode
queue.EnqueueSnerdTask(&snerd.SnerdTask{
    TaskID: "transcode-1",
    TaskType: "transcode_video",
    Parameters: `{"file":"raw.mp4"}`,
})

// Step 2: Upload (Waits for Step 1)
nextTask := &snerd.SnerdTask{
    TaskID: "upload-1",
    TaskType: "upload_s3",
    Parameters: `{"file":"out.mp4"}`,
    TriggerAfterIds: []string{"transcode-1"},
}
queue.EnqueueSnerdTask(nextTask)
```

### 🕒 Cron & Scheduled Jobs

```go
// Run every day at 08:00
cronExpr := "0 8 * * *"
cronTask := &snerd.SnerdTask{
    TaskID: "digest",
    TaskType: "email",
    Parameters: `{}`,
    CronExpr: &cronExpr,
}
queue.EnqueueSnerdTask(cronTask)
```

### 🛑 Hard Timeouts

```go
// Forcefully kill if running > 5 mins
timeout := 300
riskyTask := &snerd.SnerdTask{
    TaskID: "risky",
    TaskType: "fetch",
    Parameters: `{}`,
    MaxExecutionSeconds: &timeout,
}
queue.EnqueueSnerdTask(riskyTask)
```

### 🌐 Webhook Callbacks

```go
// Execute via HTTP instead of local handlers
webhook := "https://api.example.com/webhook"
hookTask := &snerd.SnerdTask{
    TaskID: "serverless",
    TaskType: "resize",
    Parameters: `{}`,
    WebhookUrl: &webhook,
}
queue.EnqueueSnerdTask(hookTask)
```

*Built with ❤️ for John Wick tier engineering.*

