// Package leasedwork makes a long-running background job survive the death of
// the process running it.
//
// It generalises the mechanism docu-worker built for AI chat responses, where a
// pod dying mid-response used to leave a message stuck at status=processing
// forever. Two halves:
//
//   - [Lease] is held by the process doing the work. It renews a liveness
//     timestamp on a fixed timer, persists progress as the job advances, and
//     tells the worker when it has lost ownership so it can stop burning money
//     on output nobody will keep.
//
//   - [Reconciler] runs on every replica, sweeps for jobs whose lease went
//     stale, and hands them back to the queue — giving up after a bounded
//     number of attempts so a job that reliably kills its worker becomes a
//     visible failure instead of an infinite, expensive loop.
//
// # What this package does not do
//
// It does not own your schema, your queue, or your transport. You supply a
// [Store] that knows how to write your table and a Recover function that knows
// how to re-enqueue your job. The package owns exactly the concurrency and
// liveness reasoning, which is the part that is subtle and the part worth
// having in one place.
//
// # The rules encoded here, and why
//
// Each of these is a scar from a real outage. Reimplementing this pattern by
// hand means rediscovering them.
//
//   - Liveness is renewed on a timer, never on progress. A job step that
//     legitimately runs longer than the stale threshold (extracting a large
//     PDF, polling a remote check) is not a dead worker. Tying renewal to
//     progress means healthy long jobs get reclaimed and restarted forever.
//
//   - Ownership is an explicit token, never an affected-row count. MySQL and
//     MariaDB report rows *changed*, not rows *matched*, unless
//     CLIENT_FOUND_ROWS is set. An UPDATE that writes a value a column already
//     holds reports zero. A renewal that lands in the same one-second TIMESTAMP
//     tick as a progress write therefore looks exactly like a lost lease. Every
//     zero-row result is confirmed by reading the row back.
//
//   - Progress writes use a background context, never the job's. The job's
//     context is cancelled on shutdown and on user-stop, which are precisely
//     the moments the final checkpoint is most worth writing.
//
//   - The sweep is deduplicated across replicas. Every replica sweeping the
//     same table is fine and desirable — no leader election, no "the leader is
//     the pod that died". But the attempt counter must be incremented once per
//     staleness window, not once per replica per window, or a three-replica
//     deployment burns a three-attempt budget in a single sweep.
//
//   - The stale threshold is one constant, shared between whoever claims a job
//     and whoever sweeps for stale ones. Two independent thresholds can
//     disagree, producing a job the reconciler re-enqueues forever and the
//     claim refuses every time.
package leasedwork
