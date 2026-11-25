package postgres_shadow

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/atrabilis/relay-events/storage/pgconn"
)

// TestWriteE2E needs a PostgreSQL server: DNP3_AGENT_E2E_PG=postgres://user:pass@host:port/db (a scratch database;
// the test creates and drops its own table).
func TestWriteE2E(t *testing.T) {
	url := os.Getenv("DNP3_AGENT_E2E_PG")
	if url == "" {
		t.Skip("DNP3_AGENT_E2E_PG not set")
	}
	ctx := context.Background()
	cfgPG, err := pgx.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.ConnectConfig(ctx, cfgPG)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	ddl, err := os.ReadFile("../../sql/postgres_shadow.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, string(ddl)); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = conn.Exec(ctx, "DROP TABLE edge.example_dnp3_shadow") }()

	t.Setenv("T_HOST", cfgPG.Host)
	t.Setenv("T_PORT", strconv.Itoa(int(cfgPG.Port)))
	t.Setenv("T_USER", cfgPG.User)
	t.Setenv("T_PASS", cfgPG.Password)
	t.Setenv("T_DB", cfgPG.Database)
	w, err := New(ctx, "shadow", pgconn.Config{HostEnv: "T_HOST", PortEnv: "T_PORT", UserEnv: "T_USER", PasswordEnv: "T_PASS",
		DatabaseEnv: "T_DB", Schema: "edge", Table: "example_dnp3_shadow"})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	at := time.Date(2026, 11, 25, 18, 0, 0, 123e6, time.UTC)
	payload := map[string]any{"AI": map[string]any{"Ia": 12.5}, "BI": map[string]any{"Open(Any)": false}}
	if err := w.Write(ctx, "p", at, "rc", "telemetry", payload); err != nil {
		t.Fatal(err)
	}
	payload["AI"] = map[string]any{"Ia": 13.0}
	if err := w.Write(ctx, "p", at, "rc", "telemetry", payload); err != nil { // same key: upsert
		t.Fatal(err)
	}
	var n int
	var ia float64
	var open bool
	err = conn.QueryRow(ctx, `SELECT count(*) OVER (), (payload #>> '{AI,Ia}')::float8, (payload #>> '{BI,Open(Any)}')::bool
		FROM edge.example_dnp3_shadow WHERE plant = 'p' AND device_name = 'rc' AND series_key = 'telemetry' AND ts = $1`, at).Scan(&n, &ia, &open)
	if err != nil || n != 1 || ia != 13 || open {
		t.Fatalf("row: n=%d ia=%v open=%v err=%v", n, ia, open, err)
	}
}
