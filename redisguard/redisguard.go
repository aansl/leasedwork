// Package redisguard implements leasedwork.Guard on Redis.
//
// It is a separate package so that leasedwork itself imposes no storage
// choice; if you already run Redis for your queue, this is the obvious backing
// and you should not be writing the SETNX-window logic a fourth time.
package redisguard

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Guard deduplicates a reconciler sweep across replicas and counts recovery
// attempts, using two keys per job: a window lock and an attempt counter.
//
// Attempt history is deliberately Redis state rather than a column on the job.
// It is incident state, not part of the job: it should evaporate on its own,
// and it is written from a sweep that runs on every replica.
type Guard struct {
	rdb    redis.Cmdable
	prefix string
}

// New returns a Guard storing keys under prefix. Give each reconciler its own
// prefix — for example "ai-chat-recover:" — so unrelated job types cannot
// collide on a shared key namespace.
func New(rdb redis.Cmdable, prefix string) *Guard {
	return &Guard{rdb: rdb, prefix: prefix}
}

func (g *Guard) lockKey(key string) string    { return g.prefix + "lock:" + key }
func (g *Guard) attemptKey(key string) string { return g.prefix + "attempts:" + key }

// Begin implements leasedwork.Guard.
//
// The SETNX is what makes N replicas behave as one: exactly one sweep wins the
// window, and the losers do nothing at all rather than each adding to the
// attempt count.
//
// The lock and the counter are two round trips, not one script, so a process
// dying between them holds the lock without having acted. The cost is bounded
// and self-correcting: one recovery window is skipped and the next sweep after
// the lock expires proceeds normally. An error on the counter releases the lock
// explicitly so that case does not even wait out the window.
func (g *Guard) Begin(ctx context.Context, key string, window, counterTTL time.Duration) (int, bool, error) {
	lock := g.lockKey(key)

	ok, err := g.rdb.SetNX(ctx, lock, 1, window).Result()
	if err != nil {
		return 0, false, err
	}
	if !ok {
		return 0, false, nil
	}

	attempts := g.attemptKey(key)
	n, err := g.rdb.Incr(ctx, attempts).Result()
	if err != nil {
		g.rdb.Del(ctx, lock)
		return 0, false, err
	}
	if n == 1 {
		g.rdb.Expire(ctx, attempts, counterTTL)
	}
	return int(n), true, nil
}

// Clear implements leasedwork.Guard. It drops the attempt counter and the
// window lock, so a job that just reached a terminal state can be retried by
// hand immediately and with a full budget.
func (g *Guard) Clear(ctx context.Context, key string) error {
	return g.rdb.Del(ctx, g.attemptKey(key), g.lockKey(key)).Err()
}
