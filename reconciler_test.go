package leasedwork

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// memGuard mimics the Redis guard's semantics in memory: a window lock that one
// caller wins, and an attempt counter incremented only by that winner.
type memGuard struct {
	mu       sync.Mutex
	locked   map[string]bool
	attempts map[string]int
	err      error
}

func newMemGuard() *memGuard {
	return &memGuard{locked: map[string]bool{}, attempts: map[string]int{}}
}

func (g *memGuard) BeginWindow(_ context.Context, key string, _ time.Duration) (int, bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.err != nil {
		return 0, false, g.err
	}
	if g.locked[key] {
		return 0, false, nil
	}
	g.locked[key] = true
	return g.attempts[key], true, nil
}

func (g *memGuard) RecordAttempt(_ context.Context, key string, _ time.Duration) (int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.attempts[key]++
	return g.attempts[key], nil
}

func (g *memGuard) Clear(_ context.Context, key string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.attempts, key)
	delete(g.locked, key)
	return nil
}

// expireWindow simulates the lock TTL elapsing, so the next sweep may act.
func (g *memGuard) expireWindow() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.locked = map[string]bool{}
}

type recorder struct {
	mu        sync.Mutex
	recovered []int
	abandoned int
}

func (r *recorder) recover(_ context.Context, _ Job, attempt int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recovered = append(r.recovered, attempt)
	return nil
}

func (r *recorder) abandon(context.Context, Job) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.abandoned++
	return nil
}

func (r *recorder) counts() (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.recovered), r.abandoned
}

func staleOne(key string) func(context.Context, time.Time) ([]Job, error) {
	return func(context.Context, time.Time) ([]Job, error) {
		return []Job{{Key: key}}, nil
	}
}

func newTestReconciler(g Guard, rec *recorder, max int) *Reconciler {
	return NewReconciler(ReconcilerConfig{
		Name:        "test",
		StaleAfter:  90 * time.Second,
		MaxAttempts: max,
		Guard:       g,
		Stale:       staleOne("job-1"),
		Recover:     rec.recover,
		Abandon:     rec.abandon,
	})
}

// The bug this guard exists for: with the attempt counter incremented directly
// by each replica, N replicas sweeping the same window spent an N-attempt
// budget in a single sweep — and with N > MaxAttempts one replica marked the
// job permanently failed in the very sweep the others re-enqueued it.
func TestReplicasDoNotMultiplyAttempts(t *testing.T) {
	g := newMemGuard()
	rec := &recorder{}

	// Five replicas, all sweeping the same stale job at the same instant.
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		r := newTestReconciler(g, rec, 3)
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.Sweep(context.Background())
		}()
	}
	wg.Wait()

	recovered, abandoned := rec.counts()
	if recovered != 1 {
		t.Fatalf("recover calls = %d, want 1 — the window was not deduplicated", recovered)
	}
	if abandoned != 0 {
		t.Fatalf("abandon calls = %d, want 0 — budget was spent in one sweep", abandoned)
	}
	if g.attempts["job-1"] != 0 {
		t.Fatalf("attempts = %d, want 0 — the sweep charged an attempt nobody ran", g.attempts["job-1"])
	}
}

// The budget rations runs that actually happened, so a worker must claim the
// job for an attempt to be charged.
func TestAttemptsAreChargedByWorkersNotSweeps(t *testing.T) {
	g := newMemGuard()
	rec := &recorder{}
	r := newTestReconciler(g, rec, 3)

	for i := 0; i < 3; i++ {
		r.Sweep(context.Background())
		// A worker dequeues the retry and claims the job, which is what counts.
		if _, err := r.RecordAttempt(context.Background(), "job-1"); err != nil {
			t.Fatal(err)
		}
		g.expireWindow()
	}

	recovered, abandoned := rec.counts()
	if recovered != 3 {
		t.Fatalf("recover calls = %d, want 3", recovered)
	}
	if abandoned != 0 {
		t.Fatalf("abandon calls = %d, want 0", abandoned)
	}

	// Fourth window: three real runs have been spent, so the budget is gone.
	r.Sweep(context.Background())
	recovered, abandoned = rec.counts()
	if recovered != 3 {
		t.Fatalf("recover calls = %d after exhausting budget, want 3", recovered)
	}
	if abandoned != 1 {
		t.Fatalf("abandon calls = %d, want 1", abandoned)
	}
}

// Counting at sweep time counted *noticing* rather than *running*: a queue deep
// enough that a re-enqueued job waited out its window burned the whole budget
// without the job ever starting, failing user work that nothing was wrong with.
func TestBacklogDoesNotBurnBudget(t *testing.T) {
	g := newMemGuard()
	rec := &recorder{}
	r := newTestReconciler(g, rec, 3)

	// Ten windows pass with the task sitting in a backed-up queue: it is
	// re-enqueued but never claimed, so no attempt is ever recorded.
	for i := 0; i < 10; i++ {
		r.Sweep(context.Background())
		g.expireWindow()
	}

	recovered, abandoned := rec.counts()
	if abandoned != 0 {
		t.Fatalf("abandon calls = %d, want 0 — a backlog failed a healthy job", abandoned)
	}
	if recovered != 10 {
		t.Fatalf("recover calls = %d, want 10 (one per window)", recovered)
	}
}

// The attempt passed to Recover is the number this run will become.
func TestRecoverSeesNextAttemptNumber(t *testing.T) {
	g := newMemGuard()
	rec := &recorder{}
	r := newTestReconciler(g, rec, 3)

	r.Sweep(context.Background())
	if _, err := r.RecordAttempt(context.Background(), "job-1"); err != nil {
		t.Fatal(err)
	}
	g.expireWindow()
	r.Sweep(context.Background())

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.recovered) != 2 || rec.recovered[0] != 1 || rec.recovered[1] != 2 {
		t.Fatalf("attempt numbers = %v, want [1 2]", rec.recovered)
	}
}

func TestAbandonClearsHistory(t *testing.T) {
	g := newMemGuard()
	rec := &recorder{}
	r := newTestReconciler(g, rec, 1)

	r.Sweep(context.Background()) // re-enqueued
	if _, err := r.RecordAttempt(context.Background(), "job-1"); err != nil {
		t.Fatal(err)
	}
	g.expireWindow()
	r.Sweep(context.Background()) // budget of 1 is spent, abandoned

	g.mu.Lock()
	_, stillCounted := g.attempts["job-1"]
	g.mu.Unlock()
	if stillCounted {
		t.Fatal("attempt history survived abandonment; a manual retry would start over budget")
	}
}

// Without the guard there is no way to tell attempt 1 from attempt 50, so a
// guard failure must skip the job rather than risk an unbounded, expensive
// resurrection loop.
func TestGuardErrorSkipsJob(t *testing.T) {
	g := newMemGuard()
	g.err = errors.New("redis down")
	rec := &recorder{}
	r := newTestReconciler(g, rec, 3)

	r.Sweep(context.Background())

	recovered, abandoned := rec.counts()
	if recovered != 0 || abandoned != 0 {
		t.Fatalf("recover=%d abandon=%d, want 0/0 when the guard is unavailable", recovered, abandoned)
	}
}

func TestStaleErrorIsNotFatal(t *testing.T) {
	rec := &recorder{}
	r := NewReconciler(ReconcilerConfig{
		Guard:      newMemGuard(),
		StaleAfter: time.Minute,
		Stale: func(context.Context, time.Time) ([]Job, error) {
			return nil, errors.New("db down")
		},
		Recover: rec.recover,
		Abandon: rec.abandon,
	})
	r.Sweep(context.Background())

	if recovered, _ := rec.counts(); recovered != 0 {
		t.Fatalf("recover calls = %d, want 0", recovered)
	}
}

func TestStartStopIsIdempotent(t *testing.T) {
	rec := &recorder{}
	r := NewReconciler(ReconcilerConfig{
		Guard:      newMemGuard(),
		StaleAfter: time.Minute,
		Interval:   time.Millisecond,
		Jitter:     time.Millisecond,
		Stale:      staleOne("job-1"),
		Recover:    rec.recover,
		Abandon:    rec.abandon,
	})

	stop := r.Start(context.Background())
	time.Sleep(20 * time.Millisecond)
	stop()
	stop()
}

func TestStaleBeforeIsDerivedFromStaleAfter(t *testing.T) {
	var got time.Time
	r := NewReconciler(ReconcilerConfig{
		Guard:      newMemGuard(),
		StaleAfter: 90 * time.Second,
		Stale: func(_ context.Context, staleBefore time.Time) ([]Job, error) {
			got = staleBefore
			return nil, nil
		},
		Recover: func(context.Context, Job, int) error { return nil },
	})
	r.Sweep(context.Background())

	want := time.Now().Add(-90 * time.Second)
	if d := got.Sub(want); d > time.Second || d < -time.Second {
		t.Fatalf("staleBefore off by %v", d)
	}
}
