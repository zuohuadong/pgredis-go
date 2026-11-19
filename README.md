# pgredis-go

PostgreSQL-backed replacements for the Redis primitives a Go service usually
depends on: KV with TTL, integer counters, sorted sets (including an
expiring-member slot pool), fixed/sliding/token-bucket rate limits, a durable
queue with `SKIP LOCKED`, and session or renewal lease locks.

It is a Go port of the design in
[`vibeunion/postgresx`](https://github.com/vibeunion/postgresx)
(`packages/pgredis`, published as `@postgresx/noredis`). It is **not** a Redis
client and does not speak the Redis protocol: every method is a prepared SQL
statement issued on a `*sql.DB` you already have, so the database that holds
your facts also holds the state Redis would have kept.

```go
store := pgredis.New(db, pgredis.Options{Namespace: "app", TablePrefix: "pgredis"})
if err := store.EnsureSchema(ctx); err != nil { // once per process
    return err
}

// cache with a TTL
if found, _ := store.Get(ctx, "user:42", &user); !found {
    _ = store.Set(ctx, "user:42", user, pgredis.TTL(5*time.Minute))
}

// counter in a sliding window
n, _ := store.CounterAdd(ctx, "req:"+key, 1, pgredis.TTL(time.Minute))
if n > 100 { /* rate limited */ }
```

## Design rules

1. **One statement per operation.** Reads and writes of an operation happen in
   a single `INSERT ... ON CONFLICT ... RETURNING`, `DELETE ... RETURNING` or
   `UPDATE ... FROM (CTE)`, so no operation needs a multi-statement
   transaction to be safe against another process.
2. **Time comes from the database** (`now()`), never from the client clock, so
   nodes never disagree about expiry.
3. **Cache, counter and pool tables are `UNLOGGED`** by default: no WAL, no
   fsync on commit. They survive a clean restart and are truncated by crash
   recovery — the same durability the Redis data they replace had. Use
   `Options.Logged` to opt into WAL.
4. **Expiry is lazy on read and swept in batches on write.** An expired row is
   invisible to every reader; no background job is required for correctness,
   only for reclaiming space.
5. **Absent state is a normal answer, not an error.** Callers decide whether an
   unavailable store degrades to node-local behaviour.

## Parity with the TypeScript toolkit

| Primitive | `@postgresx/noredis` | this port |
|---|---|---|
| KV with TTL, NX/XX, compare-and-swap | ✅ | ✅ `Get/Put/Set/CompareAndSwapEqual/Expire/Persist/SweepKV` |
| integer counters | ✅ | ✅ `CounterAdd/CounterGet/CounterSet/CounterDelete/SweepCounters` |
| sorted sets | ✅ | ✅ `ZAdd/ZScore/ZCard/ZRem/ZRemRangeByScore/ZRange` |
| expiring-member slot pool | ✅ | ✅ `TryReserveSlot/RenewSlot/ReleaseSlot/Holders` |
| rate limits (fixed, sliding, token bucket) | ✅ | ✅ `RateLimitHit/HitN/Peek/Reward/SweepRateLimits` |
| durable queue with `SKIP LOCKED` | ✅ | ✅ `Enqueue/Dequeue/Ack/Retry/QueueDepth/ReclaimExpired/WaitForJob` |
| advisory locks | ✅ | ✅ `WithAdvisoryLock` (session) + `TryLeaseLock/RenewLease/Unlock` (renewable) |
| hash | ✅ | ❌ |
| list | ✅ | ❌ |
| set | ✅ | ❌ |
| pub/sub (`LISTEN/NOTIFY` helpers) | ✅ | ❌ (use `pgx`'s `WaitForNotification` directly) |
| stream / durable outbox | ✅ | ❌ |
| pipeline | ✅ | ❌ |
| metrics, ioredis-style migration facades, web adapters | ✅ | ❌ |

Missing rows are unimplemented, not planned-but-broken: open an issue if your
port needs one and the SQL design is usually a single statement.

## Testing

Every guarantee under test (single-statement atomicity, `SKIP LOCKED`,
advisory locks, `UNLOGGED` table semantics) is a property of the server, so the
suite needs a real PostgreSQL:

```bash
PGREDIS_TEST_DSN='postgres://user:pass@127.0.0.1:5432/pgredis_test?sslmode=disable' \
  go test ./...
```

The suite creates and drops its own tables per test and skips itself when no
DSN is set.

## Performance

The trade is measured, not assumed. Upstream's benchmark puts a PostgreSQL
counter at roughly 6k–11k ops/s against Redis's 41k, with p50 latency of
1.1–2.1 ms against 0.37 ms. Reads served from an in-process L1 are faster than
Redis, plain reads are 2–5x slower. Anything on the per-request path should
batch or pre-deduct locally; `RateLimitHitN` and `TryReserveSlot` exist for
that.

## License

MIT. The design is derived from `vibeunion/postgresx`, also MIT; see
`LICENSE` for the attribution notice.
