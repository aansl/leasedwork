# leasedwork

Makes a long-running background job survive the death of the process running
it.

```
go get github.com/aansl/leasedwork
```

`GOPRIVATE=github.com/aansl/*` must be set so the module resolves over SSH
rather than the public proxy.

## What problem this solves

A worker picks up a job that takes minutes. The pod is rolled, OOM-killed, or
loses its node. Nothing is watching, so the row sits at `status = processing`
for ever: the queue considers the task finished, no error is raised anywhere,
and the user sees a spinner that never stops.

Fixing that needs two halves, and this package is both:

- **`Lease`** — held by the process doing the work. It renews a liveness
  timestamp on a fixed timer, persists progress as the job advances, and tells
  the worker the moment it has lost ownership, so it stops spending money on
  output nobody will keep.
- **`Reconciler`** — runs on every replica, sweeps for jobs whose lease went
  stale, and hands them back to the queue. It gives up after a bounded number
  of attempts, so a job that reliably kills its worker becomes a visible failure
  rather than an infinite, expensive loop.

It owns the concurrency and liveness reasoning and nothing else. Your schema,
your queue and your transport stay yours: you supply a `Store` that writes your
table and a `Recover` that re-enqueues your job.

## Usage

### Worker side

```go
lease := leasedwork.NewLease(leasedwork.Config[[]Block]{
    Store: &myStore{db: db, id: msgID, owner: leaseOwner}, // closes over identity
    // Blocks reference keys in shared state, so shared state is written first.
    BeforeSave: func(ctx context.Context) error { return saveSharedState(ctx) },
    AfterSave:  func(ctx context.Context) { rdb.Expire(ctx, streamKey, time.Hour) },
    Logf:       log.Printf,
})

// Not the task context: asynq cancels that during shutdown but does not wait
// for the handler goroutine, and renewal must outlive it.
stop := lease.StartRenewal(context.WithoutCancel(ctx))
defer stop()

// This run is taking over a job a previous run abandoned, so charge it one
// attempt. A first run of a fresh job charges nothing.
if wasAbandoned {
    reconciler.RecordAttempt(ctx, jobKey)
}

for _, step := range steps {
    if lease.Lost() {
        return nil // someone else owns this now; anything we produce is discarded
    }
    blocks = append(blocks, run(step))
    lease.Save(blocks, false) // throttled; force=true at real milestones
}
```

### Reconciler side

```go
rec := leasedwork.NewReconciler(leasedwork.ReconcilerConfig{
    Name:        "ai-chat",
    StaleAfter:  leasedwork.DefaultStaleAfter, // share this with your claim query
    Guard:       redisguard.New(rdb, "ai-chat-recover:"),
    Stale:       queryStaleRows,
    Recover:     func(ctx context.Context, j leasedwork.Job, n int) error { return enqueue(j) },
    Abandon:     markFailed,
    Logf:        log.Printf,
})
stop := rec.Start(ctx)
defer stop()
```

Run it on **every** replica. There is no leader election on purpose — a leader
is one more thing that can be the pod that died — and the `Guard` makes
concurrent sweeps behave as one.

## The rules this encodes, and why

Every one of these is a scar. Reimplementing the pattern by hand means
rediscovering them the expensive way.

**Liveness is renewed on a timer, never on progress.** A step that legitimately
runs longer than the stale threshold — extracting a large PDF, polling a remote
check — is not a dead worker. Tie renewal to progress and healthy long jobs get
reclaimed and restarted for ever.

**Ownership is an explicit token, never an affected-row count.** MySQL and
MariaDB report rows *changed*, not rows *matched*, unless `CLIENT_FOUND_ROWS` is
set. An `UPDATE` writing a value the column already holds reports zero. Since a
renewal writes only `lease_at`, and `TIMESTAMP` has one-second resolution, a
renewal landing in the same second as a progress write reports zero rows on a
perfectly healthy run. Every zero here is resolved by reading the row back —
and only on the zero path, so the healthy case stays one statement.

**Progress writes use a background context, never the job's.** The job's context
is cancelled on shutdown and on user-stop, which are exactly the moments the
final checkpoint is most worth writing.

**Satellite state is written before the payload that references it.** Not
transactional — the ordering *is* the guarantee. A crash between the two leaves
state that is a superset of what the payload mentions, which is harmless. The
reverse order leaves a payload pointing at state that was never persisted, which
is the broken resume this package exists to prevent.

**The sweep is deduplicated across replicas.** Every replica sweeping is fine and
wanted. But a job must be acted on once per *staleness window*, not once per
replica per window. Without that, a three-replica deployment spends a
three-attempt budget in a single sweep, and a four-replica one marks the job
permanently failed in the very sweep the other three re-enqueued it — racing a
worker about to pick it up and succeed. `Guard.BeginWindow` collapses that back
to one action per window; `TestReplicasDoNotMultiplyAttempts` is the regression
test.

**The budget counts runs, not sightings.** `Guard.RecordAttempt` is called by the
*worker*, at the moment it claims a job a previous run abandoned — never by the
sweep. Counting at sweep time counts noticing: a queue deep enough that a
re-enqueued job waits out its window burns the whole budget without the job ever
starting, so a backlog fails user work that nothing is wrong with
(`TestBacklogDoesNotBurnBudget`). The trade is that a fleet consuming nothing at
all retries indefinitely instead of failing everything in flight — which is the
right way round, because the jobs are not the broken thing. Enqueues stay bounded
at one per window, and the duplicate is a no-op against the claim.

**One stale threshold, shared.** Whoever claims a job and whoever sweeps for
stale ones must use the same constant. Two independently tuned thresholds can
disagree, and then the reconciler re-enqueues a job the claim refuses, for ever.

## Guard backends

`redisguard` implements `Guard` on Redis. If you already run Redis for your
queue it is the obvious backing, and it means nobody writes the SETNX-window
logic a fourth time. Any store with atomic set-if-absent and increment works.

## Releasing

CI runs build, vet, `go test -race` and a tidy check on every push and PR to
`main`. Publishing is the **release** workflow, run manually with a semver tag:
it re-verifies, then pushes the tag and cuts a release. Consumers upgrade with
`go get github.com/aansl/leasedwork@v0.2.0`.

Tagging is manual on purpose — a module version is immutable once anyone fetches
it, and auto-tagging every merge burns a version per commit while telling
consumers nothing about what changed.
