package snerd

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/gofrs/flock"
)

const (
	LeaseDurationSecs    = 30
	RenewIntervalSecs    = 10
	DefaultSkewMarginSecs = 5
	MembershipFile       = "membership.json"
	MembershipLock       = ".membership.lock"
	MembershipTmp        = "membership.json.tmp"
)

var (
	ErrNotInitialized   = errors.New("not initialized")
	ErrAlreadyInit      = errors.New("already initialized")
	ErrLeaseLost        = errors.New("lease lost")
	ErrOwnedByOther     = errors.New("owned by other")
	ErrCorrupt          = errors.New("corrupt membership")
)

type Claim struct {
	Owner       string    `json:"owner"`
	LeaseExpiry time.Time `json:"lease_expiry"`
}

type Membership struct {
	Queue   string           `json:"queue"`
	Shards  int              `json:"shards"`
	Version uint64           `json:"version"`
	Claims  map[string]Claim `json:"claims"`
}

type ClaimOutcome int

const (
	ClaimOutcomeClaimed ClaimOutcome = iota
	ClaimOutcomeRenewed
	ClaimOutcomeTakenOver
)

func shardKey(index int) string {
	return fmt.Sprintf("shard-%d", index)
}

func ownerID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown-host"
	}
	return fmt.Sprintf("%s@pid-%d", host, os.Getpid())
}

func isLapsed(claim Claim, now time.Time, skew time.Duration) bool {
	return now.After(claim.LeaseExpiry.Add(skew))
}

func skewMargin() time.Duration {
	if val := os.Getenv("SNERD_CLOCK_SKEW_MARGIN"); val != "" {
		if secs, err := strconv.Atoi(val); err == nil && secs >= 0 {
			return time.Duration(secs) * time.Second
		}
	}
	return DefaultSkewMarginSecs * time.Second
}

func claimableShards(m *Membership, now time.Time, skew time.Duration, owner string) []string {
	var res []string
	for i := 0; i < m.Shards; i++ {
		key := shardKey(i)
		claim, exists := m.Claims[key]
		if !exists || claim.Owner == owner || isLapsed(claim, now, skew) {
			res = append(res, key)
		}
	}
	return res
}

type MembershipStore struct {
	dir string
}

func NewMembershipStore(dir string) *MembershipStore {
	return &MembershipStore{dir: dir}
}

func (s *MembershipStore) membershipPath() string {
	return filepath.Join(s.dir, MembershipFile)
}

func (s *MembershipStore) lockPath() string {
	return filepath.Join(s.dir, MembershipLock)
}

func (s *MembershipStore) Exists() bool {
	_, err := os.Stat(s.membershipPath())
	return err == nil
}

func (s *MembershipStore) Init(queue string, shards int) error {
	return s.withLock(func() error {
		return s.InitUnlocked(queue, shards)
	})
}

func (s *MembershipStore) InitUnlocked(queue string, shards int) error {
	if s.Exists() {
		return ErrAlreadyInit
	}
	m := &Membership{
		Queue:   queue,
		Shards:  shards,
		Version: 0,
		Claims:  make(map[string]Claim),
	}
	return s.writeUnlocked(m)
}

func (s *MembershipStore) Load() (*Membership, error) {
	var m *Membership
	err := s.withLock(func() error {
		var innerErr error
		m, innerErr = s.LoadUnlocked()
		return innerErr
	})
	return m, err
}

func (s *MembershipStore) LoadUnlocked() (*Membership, error) {
	data, err := os.ReadFile(s.membershipPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotInitialized
		}
		return nil, err
	}
	var m Membership
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, ErrCorrupt
	}
	if m.Claims == nil {
		m.Claims = make(map[string]Claim)
	}
	return &m, nil
}

func (s *MembershipStore) WithLockHeld(f func() error) error {
	lock := flock.New(s.lockPath())
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()
	return f()
}

func (s *MembershipStore) withLock(f func() error) error {
	return s.WithLockHeld(f)
}

func (s *MembershipStore) writeUnlocked(m *Membership) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmpPath := filepath.Join(s.dir, MembershipTmp)
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	// fsync not strictly needed in this port if we trust atomic rename, but typically done
	f, err := os.OpenFile(tmpPath, os.O_RDWR, 0644)
	if err == nil {
		f.Sync()
		f.Close()
	}
	return os.Rename(tmpPath, s.membershipPath())
}

func (s *MembershipStore) mutate(f func(*Membership) (interface{}, error)) (interface{}, error) {
	var res interface{}
	err := s.withLock(func() error {
		m, err := s.LoadUnlocked()
		if err != nil {
			return err
		}
		out, err := f(m)
		if err != nil {
			return err
		}
		m.Version++
		res = out
		return s.writeUnlocked(m)
	})
	return res, err
}

func (s *MembershipStore) Claim(shard, owner string, now time.Time, skew time.Duration) (ClaimOutcome, error) {
	expiry := now.Add(LeaseDurationSecs * time.Second)
	out, err := s.mutate(func(m *Membership) (interface{}, error) {
		existing, ok := m.Claims[shard]
		if !ok {
			m.Claims[shard] = Claim{Owner: owner, LeaseExpiry: expiry}
			return ClaimOutcomeClaimed, nil
		}
		if existing.Owner == owner {
			m.Claims[shard] = Claim{Owner: owner, LeaseExpiry: expiry}
			return ClaimOutcomeRenewed, nil
		}
		if isLapsed(existing, now, skew) {
			m.Claims[shard] = Claim{Owner: owner, LeaseExpiry: expiry}
			return ClaimOutcomeTakenOver, nil
		}
		return nil, ErrOwnedByOther
	})
	if err != nil {
		return 0, err
	}
	return out.(ClaimOutcome), nil
}

func (s *MembershipStore) RevertClaim(shard, owner string) error {
	_, err := s.mutate(func(m *Membership) (interface{}, error) {
		if claim, ok := m.Claims[shard]; ok && claim.Owner == owner {
			delete(m.Claims, shard)
		}
		return nil, nil
	})
	return err
}

func (s *MembershipStore) Renew(shard, owner string, now time.Time) error {
	expiry := now.Add(LeaseDurationSecs * time.Second)
	_, err := s.mutate(func(m *Membership) (interface{}, error) {
		claim, ok := m.Claims[shard]
		if ok && claim.Owner == owner {
			m.Claims[shard] = Claim{Owner: owner, LeaseExpiry: expiry}
			return nil, nil
		}
		return nil, ErrLeaseLost
	})
	return err
}

func (s *MembershipStore) Release(shard, owner string) error {
	_, err := s.mutate(func(m *Membership) (interface{}, error) {
		claim, ok := m.Claims[shard]
		if ok && claim.Owner == owner {
			delete(m.Claims, shard)
			return nil, nil
		}
		return nil, ErrLeaseLost
	})
	return err
}
