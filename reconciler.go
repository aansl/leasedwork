package leasedwork

import (
	"context"
	"math/rand"
	"time"
)

// Defaults applied by [NewReconciler] when a ReconcilerConfig field is zero.
const (
	DefaultSweepInterval = 30 * time.Second
	DefaultSweepJitter   = 10 * time.Second
	DefaultSweepTimeout  = 15 * time.Second
	DefaultMaxAttempts   = 3

	// DefaultStaleAfter is how long a job may go without renewing its lease
	// before it is treated as abandoned.
	//
	// It must be several multiples of the lease's renew interval, so that a slow
	// database or one dropped renewal never reclaims a healthy run: the renewer
	// is on a fixed timer, so a live worker misses several in a row before
	// crossing this line. At the default renew interval that is four.
	//
	// Export this same value to whatever claims a job. Two independently tuned
	// thresholds can disagree, producing a job the reconciler re-enqueues for
	// ever and the claim refuses every time.
	DefaultStaleAfter = 90 * time.Second
)

// Job identifies one recoverable unit of work. Key must be stable across sweeps
// and unique per job — it is the guard key and appears in logs. Value carries
// whatever the caller's Recover and Abandon need (the row, a decoded payload)
// and is opaque here.
type Job struct {
	Key   string
	Value any
}

// Guard makes a sweep that runs on every replica behave as if it ran on one,
// and holds the count of how many times a job has actually been retried.
//
// The split between BeginWindow and RecordAttempt is deliberate and is the fix
// for two distinct bugs.
//
// The first: a reconciler that counts an attempt itself, once per replica per
// sweep, does not enforce the budget it appears to. With three replicas a
// three-attempt budget is spent in a single sweep, and with four, one replica
// marks the job permanently failed in the very sweep the other three
// re-enqueued it — racing a worker about to pick it up and succeed.
// BeginWindow collapses that to one action per staleness window.
//
// The second: counting at sweep time counts *noticing*, not *running*. A queue
// deep enough that a re-enqueued job waits longer than one window burns the
// whole budget without the job ever starting, so a backlog fails user work that
// nothing is actually wrong with. RecordAttempt moves the count to the moment a
// worker genuinely takes the job, which is the thing worth rationing.
type Guard interface {
	// BeginWindow claims the exclusive right to act on key for the next window,
	// and reports how many attempts have been recorded for it so far.
	//
	// ok is false when another replica (or an earlier sweep still inside the
	// window) already holds the claim; the caller must then do nothing at all
	// for this job — not re-enqueue it, not fail it.
	//
	// The claim must expire after window on its own.
	BeginWindow(ctx context.Context, key string, window time.Duration) (attempts int, ok bool, err error)

	// RecordAttempt counts one real attempt and returns the new total. It is
	// called by the worker at the moment it claims a job that a previous run
	// abandoned — never by the sweep.
	//
	// ttl bounds the counter: comfortably longer than any single run, short
	// enough that a job which recovered weeks ago carries no history into a new
	// incident.
	RecordAttempt(ctx context.Context, key string, ttl time.Duration) (attempts int, err error)

	// Clear forgets a key's attempt history. Call it when a job reaches a
	// terminal state: a later manual retry is a new incident deserving its own
	// budget.
	Clear(ctx context.Context, key string) error
}

// ReconcilerConfig configures a [Reconciler]. Stale, Recover and Guard are
// required; Abandon is required whenever MaxAttempts can be reached.
type ReconcilerConfig struct {
	// Name labels log lines.
	Name string

	// StaleAfter is how long a job may go without renewing its lease before it
	// is treated as abandoned.
	//
	// Share this exact value with whatever claims a job. Two independently
	// tuned thresholds can disagree, and then the reconciler re-enqueues a job
	// the claim refuses, forever.
	StaleAfter time.Duration

	// Interval is the sweep period. Keep it well below StaleAfter so a dead job
	// is picked up promptly instead of waiting out most of a second threshold
	// stacked on the first. Default DefaultSweepInterval.
	Interval time.Duration

	// Jitter spreads sweeps across replicas, including the very first one.
	// Without it, replicas started by one rollout tick in lockstep forever.
	// Default DefaultSweepJitter.
	Jitter time.Duration

	// SweepTimeout bounds one sweep. Default DefaultSweepTimeout.
	SweepTimeout time.Duration

	// MaxAttempts caps how many times a job is resurrected before Abandon.
	//
	// Without a ceiling, a job that kills its worker every time it is picked up
	// — a malformed checkpoint, a step that reliably exhausts memory on one
	// input — is re-enqueued every StaleAfter forever, and each attempt can cost
	// real money before dying in the same place. A small number rides out the
	// infrastructure flapping this whole mechanism exists for, while turning a
	// genuinely poisonous job into a visible failure someone can act on.
	// Default DefaultMaxAttempts.
	MaxAttempts int

	// AttemptTTL bounds the guard's attempt counters.
	// Default 24h.
	AttemptTTL time.Duration

	// Guard deduplicates the sweep across replicas and counts attempts.
	Guard Guard

	// Stale returns jobs whose lease is older than staleBefore. Cap the number
	// of rows returned and order oldest-first, so the longest-stuck job is
	// recovered first when the batch is capped.
	Stale func(ctx context.Context, staleBefore time.Time) ([]Job, error)

	// Recover hands a job back to whatever will run it — normally re-enqueueing
	// it. attempt is 1 on the first recovery.
	//
	// Do not mark the job as claimed here. Whoever dequeues it runs the same
	// atomic claim every other path runs, so a duplicate costs one no-op
	// dequeue. Reserving it at enqueue time instead means a task lost in
	// transit leaves the job leased to a worker that never picks it up.
	Recover func(ctx context.Context, job Job, attempt int) error

	// Abandon drives a job that exhausted MaxAttempts to a terminal state and
	// tells anyone watching. Leave any partial progress in place: the user is
	// better served seeing how far the job got than an empty result.
	Abandon func(ctx context.Context, job Job) error

	// Logf receives diagnostics. Default is no logging.
	Logf func(format string, args ...any)
}

// Reconciler sweeps for jobs whose owning process stopped renewing their lease
// and hands them back to be run again.
//
// This is the half that makes a process death self-healing. A [Lease] gives a
// resumed run something to pick up from; a queue's own redelivery is not enough
// on its own, because it only fires when the task's own lease expires and says
// nothing about a run that died in a way the queue considers a success. The
// sweep is the proactive half: it reads the durable record of the job and
// notices that it stopped advancing.
//
// Run it on every replica. There is no leader election on purpose — a leader is
// one more thing that can be the pod that died, and the Guard already makes
// concurrent sweeps behave as one.
type Reconciler struct {
	cfg ReconcilerConfig
}

// NewReconciler returns a Reconciler using cfg, with zero fields defaulted.
func NewReconciler(cfg ReconcilerConfig) *Reconciler {
	if cfg.StaleAfter <= 0 {
		cfg.StaleAfter = DefaultStaleAfter
	}
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultSweepInterval
	}
	if cfg.Jitter <= 0 {
		cfg.Jitter = DefaultSweepJitter
	}
	if cfg.SweepTimeout <= 0 {
		cfg.SweepTimeout = DefaultSweepTimeout
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = DefaultMaxAttempts
	}
	if cfg.AttemptTTL <= 0 {
		cfg.AttemptTTL = 24 * time.Hour
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	if cfg.Name == "" {
		cfg.Name = "leasedwork"
	}
	return &Reconciler{cfg: cfg}
}

// Start runs the sweep until ctx is cancelled or the returned stop is called.
// stop is idempotent and returns immediately; it does not wait for an in-flight
// sweep, which is bounded by SweepTimeout anyway.
func (r *Reconciler) Start(ctx context.Context) (stop func()) {
	done := make(chan struct{})
	var closed bool

	go func() {
		// Stagger the first sweep too, not only the interval — otherwise every
		// replica's very first tick still lands together.
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-time.After(time.Duration(rand.Int63n(int64(r.cfg.Jitter)))):
		}

		ticker := time.NewTicker(r.cfg.Interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				r.Sweep(ctx)
			}
		}
	}()

	return func() {
		if closed {
			return
		}
		closed = true
		close(done)
	}
}

// Sweep runs one pass. Exported so it can be driven directly in tests and from
// an operator-triggered endpoint.
func (r *Reconciler) Sweep(ctx context.Context) {
	sweepCtx, cancel := context.WithTimeout(ctx, r.cfg.SweepTimeout)
	defer cancel()

	stale, err := r.cfg.Stale(sweepCtx, time.Now().Add(-r.cfg.StaleAfter))
	if err != nil {
		r.cfg.Logf("[%s] query stale jobs: %v", r.cfg.Name, err)
		return
	}
	if len(stale) == 0 {
		return
	}
	r.cfg.Logf("[%s] found %d stale job(s)", r.cfg.Name, len(stale))

	for _, job := range stale {
		spent, ok, err := r.cfg.Guard.BeginWindow(sweepCtx, job.Key, r.cfg.StaleAfter)
		if err != nil {
			// The guard is the only thing enforcing the ceiling. Without it we
			// cannot tell attempt 1 from attempt 50, so skip rather than risk an
			// unbounded and expensive resurrection loop. The next sweep retries.
			r.cfg.Logf("[%s] guard failed for %s, skipping: %v", r.cfg.Name, job.Key, err)
			continue
		}
		if !ok {
			// Another replica owns this window. Not an error.
			continue
		}

		if spent >= r.cfg.MaxAttempts {
			r.cfg.Logf("[%s] %s exceeded %d recovery attempts — abandoning it",
				r.cfg.Name, job.Key, r.cfg.MaxAttempts)
			if r.cfg.Abandon == nil {
				continue
			}
			if err := r.cfg.Abandon(sweepCtx, job); err != nil {
				r.cfg.Logf("[%s] abandon %s: %v", r.cfg.Name, job.Key, err)
				continue
			}
			// Terminal now: a later manual retry deserves a fresh budget.
			if err := r.cfg.Guard.Clear(sweepCtx, job.Key); err != nil {
				r.cfg.Logf("[%s] clear attempts for %s: %v", r.cfg.Name, job.Key, err)
			}
			continue
		}

		// spent+1 is what this recovery will become once a worker claims it and
		// calls RecordAttempt. It is advisory: if the task is lost in transit or
		// waits out the window in a backlog, no attempt is recorded and the next
		// window simply re-enqueues. That is the intended trade — a job is only
		// charged for runs that really happened, so a stalled queue no longer
		// fails work that nothing is wrong with. It also means a fleet that
		// consumes nothing at all is retried indefinitely rather than failing
		// every job in flight, which is the right way round: the jobs are not
		// the broken thing.
		if err := r.cfg.Recover(sweepCtx, job, spent+1); err != nil {
			r.cfg.Logf("[%s] recover %s: %v", r.cfg.Name, job.Key, err)
			continue
		}
		r.cfg.Logf("[%s] re-enqueued %s attempt=%d", r.cfg.Name, job.Key, spent+1)
	}
}

// Clear forgets a job's recovery history. Call it when a job completes
// successfully, so a job that recovered once does not carry that attempt into
// an unrelated incident later.
func (r *Reconciler) Clear(ctx context.Context, key string) error {
	return r.cfg.Guard.Clear(ctx, key)
}

// RecordAttempt charges the job one recovery attempt. Call it from the worker
// at the moment it claims a job a previous run abandoned — that is what the
// budget is meant to ration. A first run of a fresh job is not an attempt.
func (r *Reconciler) RecordAttempt(ctx context.Context, key string) (int, error) {
	return r.cfg.Guard.RecordAttempt(ctx, key, r.cfg.AttemptTTL)
}
