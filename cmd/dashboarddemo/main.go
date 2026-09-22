package main

import (
	"context"
	"fmt"
	"time"

	snerd "github.com/speed-nerd/snerd-go"
)

func main() {
	q := snerd.NewAnyQueue("dashboard-demo", 100, 1*time.Second)

	// A handler that succeeds and streams progress
	snerd.RegisterTaskHandler("progress-task", func(ctx context.Context, parameters string) error {
		for i := 1; i <= 5; i++ {
			q.YieldProgress("progress-task-1", fmt.Sprintf("step %d/5 of %s", i, parameters))
			time.Sleep(300 * time.Millisecond)
		}
		return nil
	})

	// A handler that always fails (exercises retry/dead-letter path)
	snerd.RegisterTaskHandler("fail-task", func(ctx context.Context, parameters string) error {
		return fmt.Errorf("boom: intentional failure")
	})

	// Cron ping handler — emits a progress event on every cron fire
	snerd.RegisterTaskHandler("cron-ping", func(ctx context.Context, parameters string) error {
		q.YieldProgress("cron-ping-1", "cron ping at "+time.Now().Format(time.RFC3339))
		return nil
	})

	// Slow handler — sleeps 6s so the 2s hard timeout trips (respects ctx cancellation)
	snerd.RegisterTaskHandler("slow-task", func(ctx context.Context, parameters string) error {
		select {
		case <-time.After(6 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	q.StartDashboard(9022)

	// 1) Success task with live progress
	t1, _ := snerd.NewSnerdTask("progress-task-1", "progress-task", map[string]string{"job": "demo"}, 2, 0.0)
	q.EnqueueSnerdTask(t1)

	// 2) Failing task with tiny max retries → dead letter
	t2, _ := snerd.NewSnerdTask("fail-task-1", "fail-task", map[string]string{}, 1, 0.0001)
	q.EnqueueSnerdTask(t2)

	// 3) Future-scheduled task → stays "queued"
	future := time.Now().Add(1 * time.Hour).UTC().Format(time.RFC3339)
	t3, _ := snerd.NewSnerdTaskAdvanced("future-task-1", "progress-task", map[string]string{}, 1, 0.0, nil, nil, nil, nil, &future, nil, nil, nil, nil)
	q.EnqueueSnerdTask(t3)

	// 4) Cron job — refires every 10 seconds
	cronExpr := "*/10 * * * * *"
	t4, _ := snerd.NewSnerdTaskAdvanced("cron-ping-1", "cron-ping", map[string]string{}, 2, 0.0, nil, nil, nil, nil, nil, &cronExpr, nil, nil, nil)
	q.EnqueueSnerdTask(t4)

	// 5) Webhook job — executed via HTTP POST to the mock webhook server
	webhookUrl := "http://localhost:9010/webhook-ok"
	t5, _ := snerd.NewSnerdTaskAdvanced("webhook-task-1", "webhook-task", map[string]string{"via": "webhook"}, 2, 0.0, nil, nil, nil, nil, nil, nil, &webhookUrl, nil, nil)
	q.EnqueueSnerdTask(t5)

	// 6) Hard-timeout job — handler sleeps 6s but the timeout is 2s
	timeoutSecs := 2
	t6, _ := snerd.NewSnerdTaskAdvanced("timeout-task-1", "slow-task", map[string]string{}, 1, 0.0005, nil, nil, nil, nil, nil, nil, nil, &timeoutSecs, nil)
	q.EnqueueSnerdTask(t6)

	fmt.Println("Demo running — dashboard on http://localhost:9022 (Ctrl+C to stop)")

	// Emit a heartbeat progress event every 3 seconds so the Progress Stream stays live
	tick := 0
	for range time.Tick(3 * time.Second) {
		tick++
		q.YieldProgress("heartbeat", fmt.Sprintf("tick #%d — queue healthy at %s", tick, time.Now().Format(time.RFC3339)))
	}
}
