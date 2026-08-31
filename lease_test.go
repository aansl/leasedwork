package leasedwork

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeStore struct {
	mu sync.Mutex

	saved    []string
	saveRows int64
	saveErr  error

	renews   int
	renewRow int64
	renewErr error

	owned    bool
	ownedErr error
	ownCalls int
}

func newFakeStore() *fakeStore {
	return &fakeStore{saveRows: 1, renewRow: 1, owned: true}
}

func (f *fakeStore) SaveProgress(_ context.Context, p string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saved = append(f.saved, p)
	return f.saveRows, f.saveErr
}

func (f *fakeStore) RenewLease(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renews++
	return f.renewRow, f.renewErr
}

func (f *fakeStore) OwnsLease(context.Context) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ownCalls++
	return f.owned, f.ownedErr
}

func (f *fakeStore) writes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.saved...)
}

func (f *fakeStore) ownershipChecks() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ownCalls
}

func TestNilLeaseIsSafe(t *testing.T) {
	var l *Lease[string]
	l.Save("x", true)
	if l.Lost() {
		t.Fatal("nil lease reported lost")
	}
	l.StartRenewal(context.Background())()
}

func TestSaveThrottlesUnlessForced(t *testing.T) {
	s := newFakeStore()
	l := NewLease(Config[string]{Store: s, MinInterval: time.Hour})

	l.Save("first", false)
	l.Save("throttled", false)
	l.Save("forced", true)

	got := s.writes()
	want := []string{"first", "forced"}
	if len(got) != len(want) {
		t.Fatalf("writes = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("writes = %v, want %v", got, want)
		}
	}
}

// A zero-row UPDATE is ambiguous on MySQL/MariaDB: the driver reports rows
// changed, not rows matched. A renewal landing in the same one-second TIMESTAMP
// tick as a progress write changes nothing and reports zero on a completely
// healthy run. Treating that as lost ownership used to discard finished work.
func TestZeroRowsWithOwnershipIsNotLost(t *testing.T) {
	s := newFakeStore()
	s.saveRows = 0
	s.owned = true

	l := NewLease(Config[string]{Store: s})
	l.Save("payload", true)

	if l.Lost() {
		t.Fatal("healthy run marked lost after a no-op UPDATE")
	}
	if s.ownershipChecks() != 1 {
		t.Fatalf("ownership checks = %d, want 1", s.ownershipChecks())
	}
}

func TestZeroRowsWithoutOwnershipMarksLost(t *testing.T) {
	s := newFakeStore()
	s.saveRows = 0
	s.owned = false

	l := NewLease(Config[string]{Store: s})
	l.Save("payload", true)

	if !l.Lost() {
		t.Fatal("reclaimed run not marked lost")
	}

	// A lost lease must stop writing entirely.
	before := len(s.writes())
	l.Save("after", true)
	if len(s.writes()) != before {
		t.Fatal("wrote after losing the lease")
	}
}

// A transient read failure must never be able to abandon a live run; the
// reconciler is the backstop if the process really is dead.
func TestOwnershipCheckErrorAssumesStillOwned(t *testing.T) {
	s := newFakeStore()
	s.saveRows = 0
	s.ownedErr = errors.New("connection reset")

	l := NewLease(Config[string]{Store: s})
	l.Save("payload", true)

	if l.Lost() {
		t.Fatal("marked lost because the ownership check failed")
	}
}

func TestNonZeroRowsSkipsOwnershipCheck(t *testing.T) {
	s := newFakeStore()
	l := NewLease(Config[string]{Store: s})
	l.Save("payload", true)

	if s.ownershipChecks() != 0 {
		t.Fatalf("healthy path cost %d extra queries, want 0", s.ownershipChecks())
	}
}

// Payloads reference keys in satellite state, so that state must be persisted
// first. If it cannot be, the payload must not be written either.
func TestBeforeSaveErrorAbandonsWrite(t *testing.T) {
	s := newFakeStore()
	l := NewLease(Config[string]{
		Store:      s,
		BeforeSave: func(context.Context) error { return errors.New("state write failed") },
	})

	l.Save("payload", true)

	if len(s.writes()) != 0 {
		t.Fatalf("payload written despite failed state write: %v", s.writes())
	}
}

func TestSaveOrderIsStateThenPayload(t *testing.T) {
	s := newFakeStore()
	var order []string
	var mu sync.Mutex

	l := NewLease(Config[string]{
		Store: s,
		BeforeSave: func(context.Context) error {
			mu.Lock()
			order = append(order, "state")
			mu.Unlock()
			return nil
		},
		AfterSave: func(context.Context) {
			mu.Lock()
			order = append(order, "after")
			mu.Unlock()
		},
	})
	l.Save("payload", true)

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "state" || order[1] != "after" {
		t.Fatalf("hook order = %v, want [state after]", order)
	}
}

func TestRenewalRunsOnTimerAndStopsIdempotently(t *testing.T) {
	s := newFakeStore()
	l := NewLease(Config[string]{Store: s, RenewInterval: 5 * time.Millisecond})

	stop := l.StartRenewal(context.Background())
	deadline := time.Now().Add(2 * time.Second)
	for {
		s.mu.Lock()
		n := s.renews
		s.mu.Unlock()
		if n >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("renewals = %d, want >= 2", n)
		}
		time.Sleep(time.Millisecond)
	}
	stop()
	stop() // must not panic
}

func TestRenewalDetectsLostLease(t *testing.T) {
	s := newFakeStore()
	s.renewRow = 0
	s.owned = false

	l := NewLease(Config[string]{Store: s, RenewInterval: 5 * time.Millisecond})
	stop := l.StartRenewal(context.Background())
	defer stop()

	deadline := time.Now().Add(2 * time.Second)
	for !l.Lost() {
		if time.Now().After(deadline) {
			t.Fatal("renewal never noticed the lease was lost")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestConcurrentSaveAndRenewal(t *testing.T) {
	s := newFakeStore()
	l := NewLease(Config[string]{Store: s, MinInterval: 0, RenewInterval: time.Millisecond})

	stop := l.StartRenewal(context.Background())
	defer stop()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				l.Save("payload", true)
				_ = l.Lost()
			}
		}()
	}
	wg.Wait()
}
