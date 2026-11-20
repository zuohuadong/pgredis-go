package pgredis

import (
	"context"
	"database/sql"
	"time"
)

// DeleteIfEqual removes key only when its stored JSON still equals expected,
// and reports whether it removed a row. It is the compare-and-delete behind
// releasing a binding: a node that read an old binding cannot delete the one
// another node has since replaced.
func (s *Store) DeleteIfEqual(ctx context.Context, key string, expected any) (bool, error) {
	payload, err := jsonMarshal(expected)
	if err != nil {
		return false, err
	}
	tag, err := s.q.ExecContext(ctx,
		`DELETE FROM `+s.tables.kv+`
		  WHERE namespace = $1 AND key = $2 AND value = $3::jsonb`,
		s.ns(), key, payload)
	if err != nil {
		return false, err
	}
	return rowsAffected(tag) > 0, nil
}

// CounterAddWithTTL adds delta and reports the new value with the time the
// window still has to run. A window that has expired restarts at delta with a
// fresh ttl, which is what makes a fixed-window limiter need no cleanup job.
//
// The remaining time comes from the database clock, so every node reads the
// same window boundary.
func (s *Store) CounterAddWithTTL(ctx context.Context, key string, delta int64, ttl TTL) (value int64, remaining time.Duration, err error) {
	var millis sql.NullInt64
	err = s.q.QueryRowContext(ctx,
		`INSERT INTO `+s.tables.counter+` (namespace, key, value, expires_at, updated_at)
		 VALUES ($1, $2, $3, `+expiryExpr("$4")+`, now())
		 ON CONFLICT (namespace, key) DO UPDATE
		    SET value = CASE
		                  WHEN `+s.tables.counter+`.expires_at IS NOT NULL
		                   AND `+s.tables.counter+`.expires_at <= now()
		                  THEN EXCLUDED.value
		                  ELSE `+s.tables.counter+`.value + EXCLUDED.value
		                END,
		        expires_at = CASE
		                       WHEN `+s.tables.counter+`.expires_at IS NOT NULL
		                        AND `+s.tables.counter+`.expires_at <= now()
		                       THEN EXCLUDED.expires_at
		                       ELSE `+s.tables.counter+`.expires_at
		                     END,
		        updated_at = now()
		 RETURNING value,
		           CASE WHEN expires_at IS NULL THEN NULL
		                ELSE (EXTRACT(EPOCH FROM (expires_at - now())) * 1000)::bigint
		           END`,
		s.ns(), key, delta, ttl.millis()).Scan(&value, &millis)
	if err != nil {
		return 0, 0, err
	}
	if !millis.Valid {
		return value, 0, nil
	}
	return value, time.Duration(maxInt64(millis.Int64, 0)) * time.Millisecond, nil
}
