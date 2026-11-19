package pgredis_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/zuohuadong/pgredis-go"
)

func TestGetOrSetFirstWriterWins(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	key := "mint:ns:key"

	value, minted, remaining, err := f.store.GetOrSet(ctx, key, "candidate-a", pgredis.TTL(time.Minute), false)
	if err != nil {
		t.Fatalf("GetOrSet first: %v", err)
	}
	if !minted || string(value) != `"candidate-a"` {
		t.Fatalf("first writer must win: minted=%v value=%s", minted, value)
	}
	if remaining <= 0 || remaining > time.Minute {
		t.Fatalf("remaining = %v, want (0, 1m]", remaining)
	}

	// A later caller reads the winner instead of overwriting it.
	value, minted, _, err = f.store.GetOrSet(ctx, key, "candidate-b", pgredis.TTL(time.Minute), false)
	if err != nil {
		t.Fatalf("GetOrSet second: %v", err)
	}
	if minted || string(value) != `"candidate-a"` {
		t.Fatalf("second writer must lose: minted=%v value=%s", minted, value)
	}
}

func TestGetOrSetSlidingExtendsTTL(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	key := "mint:slide"

	if _, _, _, err := f.store.GetOrSet(ctx, key, "v", pgredis.TTL(2*time.Second), false); err != nil {
		t.Fatalf("seed: %v", err)
	}
	time.Sleep(1500 * time.Millisecond)
	before, exists, err := f.store.TTLOf(ctx, key)
	if err != nil || !exists {
		t.Fatalf("TTLOf: exists=%v err=%v", exists, err)
	}
	if before > 1500*time.Millisecond {
		t.Fatalf("TTL should have decayed, got %v", before)
	}
	if value, minted, after, err := f.store.GetOrSet(ctx, key, "other", pgredis.TTL(2*time.Second), true); err != nil || minted {
		t.Fatalf("sliding read: minted=%v err=%v", minted, err)
	} else if string(value) != `"v"` {
		t.Fatalf("sliding read must keep the stored value, got %s", value)
	} else if after <= before {
		t.Fatalf("sliding read must extend the TTL: before=%v after=%v", before, after)
	}
}

func TestGetOrSetReplacesExpiredValue(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	key := "mint:expired"

	if _, _, _, err := f.store.GetOrSet(ctx, key, "old", pgredis.TTL(300*time.Millisecond), false); err != nil {
		t.Fatalf("seed: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	if _, exists, err := f.store.TTLOf(ctx, key); err != nil || exists {
		t.Fatalf("expired key must be invisible: exists=%v err=%v", exists, err)
	}
	value, minted, _, err := f.store.GetOrSet(ctx, key, "new", pgredis.TTL(time.Minute), false)
	if err != nil {
		t.Fatalf("replace: %v", err)
	}
	if !minted {
		t.Fatalf("an expired value must be replaced, got %s", value)
	}
}

func TestGetOrSetKeepsJSONShape(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	type binding struct {
		Account string `json:"a"`
		Served  int    `json:"n"`
	}
	value, minted, _, err := f.store.GetOrSet(ctx, "affinity:1", binding{Account: "acct", Served: 1}, pgredis.TTL(time.Minute), false)
	if err != nil || !minted {
		t.Fatalf("GetOrSet struct: minted=%v err=%v", minted, err)
	}
	var decoded binding
	if err := json.Unmarshal(value, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Account != "acct" || decoded.Served != 1 {
		t.Fatalf("round trip = %+v", decoded)
	}
}
