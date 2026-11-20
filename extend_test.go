package pgredis_test

import (
	"context"
	"testing"
	"time"

	"github.com/zuohuadong/pgredis-go"
)

func TestDeleteIfEqual(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	key := "bind:1"

	if err := f.store.Set(ctx, key, map[string]any{"a": "acct-a", "n": 1}, pgredis.TTL(time.Minute)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// A stale expectation must not delete a value another writer replaced.
	if removed, err := f.store.DeleteIfEqual(ctx, key, map[string]any{"a": "acct-b", "n": 1}); err != nil || removed {
		t.Fatalf("stale delete: removed=%v err=%v", removed, err)
	}
	if removed, err := f.store.DeleteIfEqual(ctx, key, map[string]any{"a": "acct-a", "n": 1}); err != nil || !removed {
		t.Fatalf("matching delete: removed=%v err=%v", removed, err)
	}
	if _, exists, err := f.store.TTLOf(ctx, key); err != nil || exists {
		t.Fatalf("key must be gone: exists=%v err=%v", exists, err)
	}
}

func TestCounterAddWithTTLWindows(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	key := "window:" + time.Now().Format("150405.000000")

	value, remaining, err := f.store.CounterAddWithTTL(ctx, key, 1, pgredis.TTL(600*time.Millisecond))
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if value != 1 || remaining <= 0 || remaining > 600*time.Millisecond {
		t.Fatalf("first window: value=%d remaining=%v", value, remaining)
	}
	if value, remaining, err = f.store.CounterAddWithTTL(ctx, key, 2, pgredis.TTL(600*time.Millisecond)); err != nil {
		t.Fatalf("second: %v", err)
	} else if value != 3 {
		t.Fatalf("second add must accumulate, got %d", value)
	} else if remaining <= 0 {
		t.Fatalf("second add must report the window, got %v", remaining)
	}

	time.Sleep(700 * time.Millisecond)
	value, remaining, err = f.store.CounterAddWithTTL(ctx, key, 1, pgredis.TTL(600*time.Millisecond))
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if value != 1 {
		t.Fatalf("an expired window must restart at the delta, got %d", value)
	}
	if remaining <= 0 || remaining > 600*time.Millisecond {
		t.Fatalf("restarted window remaining = %v", remaining)
	}
}
