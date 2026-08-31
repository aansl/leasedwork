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

// BeginWindow implements leasedwork.Guard.
//
// The SETNX is what makes N replicas behave as one: exactly one sweep wins the
// window, and the losers do nothing rather than each charging the job an
// attempt. It is a plain read of the counter — the count is advanced by
// RecordAttempt, on the worker, when a job is really taken.
func (g *Guard) BeginWindow(ctx context.Context, key string, window time.Duration) (int, bool, error) {
	ok, err := g.rdb.SetNX(ctx, g.lockKey(key), 1, window).Result()
	if err != nil {
		return 0, false, err
	}
	if !ok {
		return 0, false, nil
	}

	n, err := g.rdb.Get(ctx, g.attemptKey(key)).Int()
	if err == redis.Nil {
		return 0, true, nil // never retried
	}
	if err != nil {
		// The window is claimed but the count is unknown. Release the lock so
		// the next sweep can retry promptly rather than waiting it out, and
		// report the error: acting on an unknown count risks an unbounded loop.
		g.rdb.Del(ctx, g.lockKey(key))
		return 0, false, err
	}
	return n, true, nil
}

// RecordAttempt implements leasedwork.Guard.
func (g *Guard) RecordAttempt(ctx context.Context, key string, ttl time.Duration) (int, error) {
	attempts := g.attemptKey(key)
	n, err := g.rdb.Incr(ctx, attempts).Result()
	if err != nil {
		return 0, err
	}
	if n == 1 {
		g.rdb.Expire(ctx, attempts, ttl)
	}
	return int(n), nil
}

// Clear implements leasedwork.Guard. It drops the attempt counter and the
// window lock, so a job that just reached a terminal state can be retried by
// hand immediately and with a full budget.
func (g *Guard) Clear(ctx context.Context, key string) error {
	return g.rdb.Del(ctx, g.attemptKey(key), g.lockKey(key)).Err()
}
