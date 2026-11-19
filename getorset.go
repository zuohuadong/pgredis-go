package pgredis

import (
	"context"
	"encoding/json"
	"reflect"
	"time"
)

// GetOrSet stores candidate under key when no live value exists, and returns
// whichever value now stands. It is the primitive behind minting a value once
// per cluster (an upstream session id, a synthetic user id): the first writer
// wins, every later caller reads the winner.
//
// minted reports whether candidate is the value that stood at the end of the
// call: it compares the returned JSON with candidate, so a caller that passes
// an already-stored candidate cannot tell "was already there" from "wrote it
// now" - the value is identical either way. Mint a random candidate when the
// distinction matters. sliding extends the TTL of an existing value on read,
// for caches whose entries live as long as they keep being used.
//
// The whole decision is one statement: the insert wins when the key is absent
// or expired, and the conflict branch either keeps the stored value or replaces
// it when it has expired.
func (s *Store) GetOrSet(ctx context.Context, key string, candidate any, ttl TTL, sliding bool) (value json.RawMessage, minted bool, remaining time.Duration, err error) {
	payload, err := jsonMarshal(candidate)
	if err != nil {
		return nil, false, 0, err
	}
	row := s.q.QueryRowContext(ctx,
		`INSERT INTO `+s.tables.kv+` (namespace, key, value, expires_at, updated_at)
		 VALUES ($1, $2, $3::jsonb, `+expiryExpr("$4")+`, now())
		 ON CONFLICT (namespace, key) DO UPDATE
		    SET value = CASE
		          WHEN `+s.tables.kv+`.expires_at IS NOT NULL AND `+s.tables.kv+`.expires_at <= now()
		            THEN EXCLUDED.value
		          ELSE `+s.tables.kv+`.value
		        END,
		        expires_at = CASE
		          WHEN `+s.tables.kv+`.expires_at IS NOT NULL AND `+s.tables.kv+`.expires_at <= now()
		            THEN EXCLUDED.expires_at
		          WHEN $5::boolean
		            THEN EXCLUDED.expires_at
		          ELSE `+s.tables.kv+`.expires_at
		        END,
		        updated_at = now()
		 RETURNING value,
		           CASE WHEN expires_at IS NULL THEN 0
		                ELSE (EXTRACT(EPOCH FROM (expires_at - now())) * 1000)::bigint
		           END`,
		s.ns(), key, payload, ttl.millis(), sliding)
	var (
		stored []byte
		millis int64
	)
	if err := row.Scan(&stored, &millis); err != nil {
		return nil, false, 0, err
	}
	return stored, sameJSON(stored, payload), time.Duration(maxInt64(millis, 0)) * time.Millisecond, nil
}

// TTLOf reports how long key remains valid. exists is false when the key is
// absent or expired; remaining is zero for a key without expiry.
//
// The subtraction happens in the database, so a node with a skewed clock reads
// the same remaining time as every other node.
func (s *Store) TTLOf(ctx context.Context, key string) (remaining time.Duration, exists bool, err error) {
	row := s.q.QueryRowContext(ctx,
		`SELECT CASE WHEN expires_at IS NULL THEN 0
		             ELSE (EXTRACT(EPOCH FROM (expires_at - now())) * 1000)::bigint
		        END
		   FROM `+s.tables.kv+`
		  WHERE namespace = $1 AND key = $2
		    AND (expires_at IS NULL OR expires_at > now())`,
		s.ns(), key)
	var millis int64
	if err := row.Scan(&millis); err != nil {
		if isNoRows(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return time.Duration(maxInt64(millis, 0)) * time.Millisecond, true, nil
}

// sameJSON reports whether two JSON documents carry the same value, ignoring
// the whitespace and key-order differences jsonb introduces.
func sameJSON(a, b []byte) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	var left, right any
	if err := jsonUnmarshal(a, &left); err != nil {
		return false
	}
	if err := jsonUnmarshal(b, &right); err != nil {
		return false
	}
	return reflect.DeepEqual(left, right)
}
