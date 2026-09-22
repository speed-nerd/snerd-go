package snerd

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gofrs/flock"
	"github.com/google/uuid"
)

type ShardedQueue struct {
	Name         string
	Dir          string
	shards       map[string]*AnyQueue
	shardsMu     sync.RWMutex
	progressSubs []chan string
	progressMu   sync.Mutex
}

func NewShardedQueue(name, dirPath string, requestedShards int) (*ShardedQueue, error) {
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, err
	}

	totalShards, err := resolveLayout(dirPath, name, requestedShards)
	if err != nil {
		return nil, fmt.Errorf("Failed to resolve sharding layout: %w", err)
	}

	sq := &ShardedQueue{
		Name:             name,
		Dir:              dirPath,
		shards:           make(map[string]*AnyQueue),
		progressSubs:     make([]chan string, 0),
	}

	go sq.startMembershipHeartbeat(totalShards)
	return sq, nil
}

func (sq *ShardedQueue) SubscribeProgress() chan string {
	ch := make(chan string, 1024)
	sq.progressMu.Lock()
	defer sq.progressMu.Unlock()
	sq.progressSubs = append(sq.progressSubs, ch)
	return ch
}

func (sq *ShardedQueue) Enqueue(task *SnerdTask) error {
	sq.shardsMu.RLock()
	var ownedShards []string
	for k := range sq.shards {
		ownedShards = append(ownedShards, k)
	}
	sq.shardsMu.RUnlock()

	if len(ownedShards) == 0 {
		return fmt.Errorf("Engine is in standby mode: zero shards owned. Cannot enqueue.")
	}

	if task.TaskID == "" {
		task.TaskID = uuid.New().String()
	}

	targetShard := routeShard(task.TaskID, ownedShards)

	sq.shardsMu.RLock()
	q, exists := sq.shards[targetShard]
	sq.shardsMu.RUnlock()

	if exists {
		return q.Enqueue(task)
	}
	return fmt.Errorf("Target shard not found")
}

func (sq *ShardedQueue) GetShards() map[string]*AnyQueue {
	sq.shardsMu.RLock()
	defer sq.shardsMu.RUnlock()
	res := make(map[string]*AnyQueue)
	for k, v := range sq.shards {
		res[k] = v
	}
	return res
}

func (sq *ShardedQueue) startMembershipHeartbeat(totalShards int) {
	store := NewMembershipStore(sq.Dir)
	owner := ownerID()

	for {
		now := time.Now()
		skew := skewMargin()
		var currentOwned []string

		if m, err := store.Load(); err == nil {
			claimable := claimableShards(m, now, skew, owner)
			for _, shard := range claimable {
				if outcome, err := store.Claim(shard, owner, now, skew); err == nil {
					if l, err := tryLockShard(sq.Dir, shard); err == nil && l != nil {
						currentOwned = append(currentOwned, shard)
						if outcome == ClaimOutcomeClaimed || outcome == ClaimOutcomeTakenOver {
							sq.instantiateShard(shard, l)
						}
					} else {
						store.RevertClaim(shard, owner)
					}
				}
			}
		}

		sq.reconcileShards(currentOwned)
		time.Sleep(RenewIntervalSecs * time.Second)
	}
}

func (sq *ShardedQueue) instantiateShard(shard string, lock *flock.Flock) {
	sq.shardsMu.Lock()
	if _, exists := sq.shards[shard]; exists {
		sq.shardsMu.Unlock()
		return
	}
	sq.shardsMu.Unlock()

	shardDir := shardDir(sq.Dir, shard)
	logPath := filepath.Join(shardDir, LegacyTasksDir, LegacyTasksLog)
	os.MkdirAll(filepath.Dir(logPath), 0755)

	lock.Unlock() // Let AnyQueue take the lock.
	q := NewAnyQueueWithStorage(sq.Name, 100, 1*time.Second, logPath)

	ch := q.SubscribeProgress()
	go func() {
		for msg := range ch {
			sq.progressMu.Lock()
			for _, sub := range sq.progressSubs {
				select {
				case sub <- msg:
				default:
				}
			}
			sq.progressMu.Unlock()
		}
	}()

	sq.shardsMu.Lock()
	sq.shards[shard] = q
	sq.shardsMu.Unlock()
	fmt.Printf("[Snerd] Acquired and started %s\n", shard)
}

func (sq *ShardedQueue) reconcileShards(currentOwned []string) {
	sq.shardsMu.Lock()
	defer sq.shardsMu.Unlock()

	ownedMap := make(map[string]bool)
	for _, s := range currentOwned {
		ownedMap[s] = true
	}

	for shard, q := range sq.shards {
		if !ownedMap[shard] {
			q.StopProcessor()
			delete(sq.shards, shard)
			fmt.Printf("[Snerd] Lost lease for %s, stopped engine.\n", shard)
		}
	}
}
