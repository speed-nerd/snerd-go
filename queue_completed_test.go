package snerd

import (
	"testing"
	"time"
)

func TestEvictExpiredCompletedRemovesStaleEntries(t *testing.T) {
	now := time.Now()
	completed := map[string]time.Time{
		"stale": now.Add(-90 * time.Second),
		"fresh": now.Add(-10 * time.Second),
	}

	evictExpiredCompleted(completed, now, completedTTL)

	if _, ok := completed["stale"]; ok {
		t.Error("expected stale entry to be evicted")
	}
	if _, ok := completed["fresh"]; !ok {
		t.Error("expected fresh entry to be kept")
	}
	if len(completed) != 1 {
		t.Errorf("expected 1 entry left, got %d", len(completed))
	}
}

func TestEvictExpiredCompletedKeepsEntriesWithinTTL(t *testing.T) {
	now := time.Now()
	completed := map[string]time.Time{
		"edge": now.Add(-(completedTTL - time.Second)),
	}

	evictExpiredCompleted(completed, now, completedTTL)

	if _, ok := completed["edge"]; !ok {
		t.Error("expected entry within TTL to be kept")
	}
}

func TestEvictExpiredCompletedEmptyMap(t *testing.T) {
	completed := map[string]time.Time{}
	evictExpiredCompleted(completed, time.Now(), completedTTL)
	if len(completed) != 0 {
		t.Error("expected empty map to stay empty")
	}
}
