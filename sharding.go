package snerd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/gofrs/flock"
)

const (
	LegacyTasksDir = "tasks"
	LegacyTasksLog = "tasks.log"
	LegacyLock     = ".lock"
	ShardLock      = ".lock"
)

type StorageLayout int

const (
	StorageLayoutSharded StorageLayout = iota
	StorageLayoutLegacy
	StorageLayoutFresh
)

func detectLayout(dir string) StorageLayout {
	if _, err := os.Stat(filepath.Join(dir, MembershipFile)); err == nil {
		return StorageLayoutSharded
	}
	if _, err := os.Stat(filepath.Join(dir, LegacyTasksDir, LegacyTasksLog)); err == nil {
		return StorageLayoutLegacy
	}
	return StorageLayoutFresh
}

func Fnv1a64(data string) uint64 {
	var hash uint64 = 0xcbf29ce484222325
	for i := 0; i < len(data); i++ {
		hash ^= uint64(data[i])
		hash *= 0x100000001b3
	}
	return hash
}

func routeShard(taskID string, owned []string) string {
	if len(owned) == 0 {
		panic("cannot route with zero owned shards")
	}
	idx := Fnv1a64(taskID) % uint64(len(owned))
	return owned[idx]
}

func shardDir(queueDir, shard string) string {
	return filepath.Join(queueDir, shard)
}

func migrateLegacyUnlocked(dir string) error {
	legacyLockPath := filepath.Join(dir, LegacyLock)
	if _, err := os.Stat(legacyLockPath); err == nil {
		l := flock.New(legacyLockPath)
		locked, err := l.TryLock()
		if err != nil || !locked {
			return fmt.Errorf("Another daemon is already running on storage '%s'", dir)
		}
		l.Unlock()
	}

	shard0 := shardDir(dir, shardKey(0))
	if err := os.MkdirAll(filepath.Join(shard0, LegacyTasksDir), 0755); err != nil {
		return err
	}

	legacyLog := filepath.Join(dir, LegacyTasksDir, LegacyTasksLog)
	if err := os.Rename(legacyLog, filepath.Join(shard0, LegacyTasksDir, LegacyTasksLog)); err != nil {
		return err
	}

	if _, err := os.Stat(legacyLockPath); err == nil {
		if err := os.Rename(legacyLockPath, filepath.Join(shard0, ShardLock)); err != nil {
			return err
		}
	}

	os.Remove(filepath.Join(dir, LegacyTasksDir))
	return nil
}

func resolveLayout(dir string, queueName string, requestedShards int) (int, error) {
	store := NewMembershipStore(dir)
	layout := detectLayout(dir)

	if layout == StorageLayoutSharded {
		m, err := store.Load()
		if err != nil {
			return 0, err
		}
		if m.Shards != requestedShards {
			fmt.Printf("[Snerd] membership.json declares %d shards; requested %d — membership is authoritative, ignoring request.\n", m.Shards, requestedShards)
		}
		return m.Shards, nil
	}

	var res int
	err := store.WithLockHeld(func() error {
		if layout == StorageLayoutLegacy {
			if store.Exists() {
				m, err := store.LoadUnlocked()
				if err != nil {
					return err
				}
				res = m.Shards
				return nil
			}
			if err := migrateLegacyUnlocked(dir); err != nil {
				return err
			}
			if err := store.InitUnlocked(queueName, 1); err != nil {
				return err
			}
			fmt.Printf("[Snerd] Auto-adopted legacy storage into shard-0 (queue '%s').\n", queueName)
			res = 1
			return nil
		}
		
		if !store.Exists() {
			if err := store.InitUnlocked(queueName, requestedShards); err != nil {
				return err
			}
		}
		m, err := store.LoadUnlocked()
		if err != nil {
			return err
		}
		res = m.Shards
		return nil
	})
	return res, err
}

func tryLockShard(dir, shard string) (*flock.Flock, error) {
	sDir := shardDir(dir, shard)
	if err := os.MkdirAll(filepath.Join(sDir, LegacyTasksDir), 0755); err != nil {
		return nil, err
	}
	lockPath := filepath.Join(sDir, ShardLock)
	l := flock.New(lockPath)
	locked, err := l.TryLock()
	if err != nil {
		return nil, err
	}
	if !locked {
		return nil, nil
	}
	
	f, err := os.OpenFile(lockPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err == nil {
		f.WriteString(fmt.Sprintf("%d", os.Getpid()))
		f.Close()
	}

	return l, nil
}
