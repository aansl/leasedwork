package leasedwork

import (
	"context"
	"sync"
	"time"
)

// Defaults applied by [NewLease] when a Config field is left zero.
const (
	DefaultMinInterval   = 3 * time.Second
	DefaultWriteTimeout  = 5 * time.Second
	DefaultRenewInterval = 20 * time.Second
)

// Store is the persistence a lease needs. Implementations are built per run and
// close over the job's identity and this run's ownership token, which is why no
// method takes an id: the package never has to know whether your primary key is
// a UUID, a string or an int, and cannot accidentally write to the wrong row.
type Store[P any] interface {
	// SaveProgress durably records payload. It must be scoped to this run's
	// ownership token so a run that has already been reclaimed cannot overwrite
	// whoever owns the job now.
	//
	// The returned count is the driver's affected-row count, passed through
	// verbatim — do not try to normalise it. Zero is ambiguous by design (see
	// [Store.OwnsLease]); the package resolves the ambiguity itself.
	SaveProgress(ctx context.Context, payload P) (rowsChanged int64, err error)

	// RenewLease refreshes the liveness timestamp and nothing else, scoped to
	// this run's ownership token. Same affected-row caveat as SaveProgress.
	RenewLease(ctx context.Context) (rowsChanged int64, err error)

	// OwnsLease reads the row back and answers, authoritatively, whether this
	// run still owns the job. Called only when an UPDATE reported zero rows, so
	// the healthy path stays a single statement.
	OwnsLease(ctx context.Context) (bool, error)
}

// Config configures a [Lease]. Only Store is required.
type Config[P any] struct {
	Store Store[P]

	// MinInterval throttles SaveProgress. Callers are expected to call Save
	// liberally — after every step — and let this decide what actually reaches
	// the database. The point of a checkpoint is surviving a crash, not
	// sub-second fidelity. Keep it well under the reconciler's StaleAfter so
	// progress writes alone keep the lease fresh. Default DefaultMinInterval.
	MinInterval time.Duration

	// WriteTimeout bounds a single persistence call. A checkpoint must never be
	// the thing that stalls the job it is recording. Default DefaultWriteTimeout.
	WriteTimeout time.Duration

	// RenewInterval is how often liveness is proven. It must be several
	// multiples smaller than the reconciler's StaleAfter, so that one slow
	// database call or one dropped renewal cannot make a healthy run look dead:
	// the renewer is on a fixed timer, so a live worker has to miss several in a
	// row before crossing the line. Default DefaultRenewInterval.
	RenewInterval time.Duration

	// BeforeSave runs immediately before each progress write that survives the
	// throttle, and is the hook for satellite state the payload references —
	// docu-worker writes the agents' shared working memory here.
	//
	// Ordering is the whole point and it is deliberately not transactional. If
	// your payload references keys in some other blob, that blob must be written
	// FIRST. A crash between the two then leaves satellite state that is a
	// superset of what the payload mentions, which is harmless. The reverse
	// order leaves a payload pointing at state that was never persisted, which
	// is exactly the broken resume this package exists to prevent.
	//
	// An error here abandons the progress write, so a payload is never persisted
	// ahead of the state it depends on.
	BeforeSave func(ctx context.Context) error

	// AfterSave runs after a successful progress write. Use it for best-effort
	// side work such as refreshing the TTL on a stream a client is tailing.
	AfterSave func(ctx context.Context)

	// Logf receives diagnostics. Errors are logged and swallowed throughout: a
	// failed checkpoint costs recovery granularity and must never fail the job
	// the user is waiting on. Default is no logging.
	Logf func(format string, args ...any)
}

// Lease is one process's claim on one job. It is safe for concurrent use, and
// every method is safe to call on a nil *Lease so callers that run without
// checkpointing (tests, a synchronous path) need no branches.
type Lease[P any] struct {
	store         Store[P]
	minInterval   time.Duration
	writeTimeout  time.Duration
	renewInterval time.Duration
	beforeSave    func(context.Context) error
	afterSave     func(context.Context)
	logf          func(string, ...any)

	// mu guards only the small state below. It is deliberately NOT held across
	// the database calls: a stalled write would otherwise block Lost(), which
	// the job loop checks every iteration, and block the renewal goroutine.
	mu     sync.Mutex
	lastAt time.Time
	lost   bool
}

// NewLease returns a Lease using cfg, with zero fields replaced by defaults.
func NewLease[P any](cfg Config[P]) *Lease[P] {
	if cfg.MinInterval <= 0 {
		cfg.MinInterval = DefaultMinInterval
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = DefaultWriteTimeout
	}
	if cfg.RenewInterval <= 0 {
		cfg.RenewInterval = DefaultRenewInterval
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	return &Lease[P]{
		store:         cfg.Store,
		minInterval:   cfg.MinInterval,
		writeTimeout:  cfg.WriteTimeout,
		renewInterval: cfg.RenewInterval,
		beforeSave:    cfg.BeforeSave,
		afterSave:     cfg.AfterSave,
		logf:          cfg.Logf,
	}
}

// Lost reports whether this run has been found to no longer own the job —
// reclaimed by a reconciler, or cancelled. Once true it stays true.
//
// Job loops should check it each iteration and stop: nothing produced after
// this point will ever be persisted, so continuing spends real resources on
// output that is already discarded.
func (l *Lease[P]) Lost() bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lost
}

// Save persists payload. Call it liberally; it is throttled to MinInterval
// unless force is set. Use force at genuine milestones and for the final write
// of a run.
//
// Errors are logged and swallowed.
func (l *Lease[P]) Save(payload P, force bool) {
	if l == nil || l.store == nil {
		return
	}

	l.mu.Lock()
	if l.lost {
		l.mu.Unlock()
		return
	}
	if !force && time.Since(l.lastAt) < l.minInterval {
		l.mu.Unlock()
		return
	}
	l.lastAt = time.Now()
	l.mu.Unlock()

	// Deliberately not the job's context. That context is cancelled on
	// user-stop and on shutdown, which are exactly the moments the last
	// checkpoint is most worth writing.
	ctx, cancel := context.WithTimeout(context.Background(), l.writeTimeout)
	defer cancel()

	if l.beforeSave != nil {
		if err := l.beforeSave(ctx); err != nil {
			// Abandon the write rather than persist a payload ahead of the
			// state it references. See Config.BeforeSave.
			l.logf("before-save hook failed, skipping progress write: %v", err)
			return
		}
	}

	rows, err := l.store.SaveProgress(ctx, payload)
	if err != nil {
		l.logf("save progress: %v", err)
		return
	}
	if rows == 0 && !l.stillOwned(ctx) {
		l.markLost("progress write")
		return
	}

	if l.afterSave != nil {
		l.afterSave(ctx)
	}
}

// StartRenewal proves liveness on a fixed timer until ctx is cancelled or the
// returned stop is called.
//
// Pass a context that outlives cancellation of the job's own context where the
// runtime cancels early — asynq, for instance, cancels a task's context some
// seconds into a graceful shutdown and requeues the task, but does not wait for
// the handler goroutine, which keeps running. Renewal tied to that context
// stops proving liveness while the run is demonstrably still alive, and a
// reconciler then reclaims it. context.WithoutCancel plus the returned stop is
// the right shape.
//
// stop is idempotent.
func (l *Lease[P]) StartRenewal(ctx context.Context) (stop func()) {
	if l == nil || l.store == nil {
		return func() {}
	}

	done := make(chan struct{})
	var once sync.Once

	go func() {
		ticker := time.NewTicker(l.renewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				l.renewOnce()
			}
		}
	}()

	return func() { once.Do(func() { close(done) }) }
}

func (l *Lease[P]) renewOnce() {
	if l.Lost() {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), l.writeTimeout)
	defer cancel()

	rows, err := l.store.RenewLease(ctx)
	if err != nil {
		l.logf("renew lease: %v", err)
		return
	}
	if rows == 0 && !l.stillOwned(ctx) {
		l.markLost("lease renewal")
	}
}

// stillOwned resolves the ambiguity of a zero-row UPDATE by reading the row.
//
// On error it answers "still owned": a transient read failure must never be
// able to abandon a live run. The reconciler is the backstop if this process
// really is dead.
func (l *Lease[P]) stillOwned(ctx context.Context) bool {
	owned, err := l.store.OwnsLease(ctx)
	if err != nil {
		l.logf("ownership check failed, assuming still owned: %v", err)
		return true
	}
	return owned
}

func (l *Lease[P]) markLost(source string) {
	l.mu.Lock()
	already := l.lost
	l.lost = true
	l.mu.Unlock()
	if !already {
		l.logf("%s: job is owned by another run — abandoning this one", source)
	}
}
