// Package postgres_shadow writes telemetry snapshots as one JSONB row per device and read (table layout in
// sql/postgres_shadow.sql, the same "shadow" layout other collectors use).
package postgres_shadow

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/atrabilis/relay-events/storage/pgconn"
)

// Writer is one postgres_shadow output.
type Writer struct {
	name string
	fqn  string
	pool *pgxpool.Pool
}

// New connects using the environment variables named in cfg. It does not create tables.
func New(ctx context.Context, name string, cfg pgconn.Config) (*Writer, error) {
	pool, fqn, err := pgconn.Connect(ctx, name, cfg)
	if err != nil {
		return nil, err
	}
	return &Writer{name: name, fqn: fqn, pool: pool}, nil
}

// Write upserts one row; a second write with the same key overwrites the payload.
func (w *Writer) Write(ctx context.Context, plant string, ts time.Time, device, group string, payload map[string]any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("output %q: encoding payload: %w", w.name, err)
	}
	query := fmt.Sprintf(`INSERT INTO %s (plant, ts, device_name, slave_name, series_key, payload)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (plant, device_name, slave_name, ts, series_key)
DO UPDATE SET payload = EXCLUDED.payload, ingested_at = now()`, w.fqn)
	// slave_name carries the group; series_key repeats it, so the table shares its layout and primary key with the
	// shadow tables written by other collectors.
	if _, err := w.pool.Exec(ctx, query, plant, ts.UTC(), device, group, group, json.RawMessage(data)); err != nil {
		return fmt.Errorf("output %q: %w", w.name, err)
	}
	return nil
}

// Close releases the pool.
func (w *Writer) Close() { w.pool.Close() }
