# dnp3-agent

Records binary and double-bit input events (protection pickups, trips, breaker position, reclose...) from DNP3
outstations into PostgreSQL, without losing or duplicating events across outages, and optionally periodic telemetry
snapshots (binary inputs, analog inputs, counters) for dashboards. Pure Go DNP3 master (TCP), built on
[relay-events](https://github.com/Atrabilis/relay-events) for supervision, storage and metrics.

## How it works

- A session reads class 1, 2 and 3 events (whatever the outstation buffered while nobody polled), then class 0 (the
  current state of every point), then polls events every `poll_interval` and class 0 every `integrity_interval`.
- Every response fragment becomes one report. **The outstation is sent the application confirmation for a fragment
  only after the report is stored**; until then the events stay in the outstation's buffer, so a database outage or a
  crash does not lose them. Events the outstation sends again are not stored twice (relay-events dedups by point,
  device time stamp and value).
- Each event keeps the outstation's own time stamp (objects with absolute time, or relative time with a common time of
  occurrence). While the outstation asks for time (IIN1.4) or sends an unsynchronized CTO, the stamps are marked as not
  synchronized (time quality 0x20).
- An event buffer overflow (IIN2.3) is recorded as a gap once history is known (not at the very first session).
- Link status requests from the outstation are answered; unsolicited responses are stored and confirmed the same way.
- **Telemetry (optional, `output.postgres_shadow`):** every `telemetry_interval` the agent reads class 0 and writes one
  row with the current value of every point, by name (`{"BI": {...}, "AI": {...}, "Counter": {...}, "FrozenCounter": {...}}`, layout in
  [`sql/postgres_shadow.sql`](sql/postgres_shadow.sql)). These reads never reach the event table. Analog and counter
  events (deadband changes) are confirmed and dropped: the snapshots carry the measurements, and confirming them keeps
  the outstation's event buffer from overflowing.

**It never writes to the outstation:** no time synchronization, no clearing of the restart indication, no controls.
The only messages it sends are reads, application confirmations and link status answers.

## Usage

```bash
dnp3-agent -configPath plant.yml -device recloser -mode check     # read-only: class 0 state and IIN, as JSON lines
dnp3-agent -configPath plant.yml -device recloser -mode listen -metricsFile /var/lib/node_exporter/textfile_collector/relay_events_recloser.prom
dnp3-agent -configPath plant.yml -checkConfig
```

Configuration: [`configs/example.yml`](configs/example.yml). Run `-mode listen` as a long-running service; it exits on
SIGTERM, or with status 1 when the outstation answers with another address or rejects the class reads (configuration
mismatch). Metrics: `relay_events_*` (see relay-events).

## Supported objects

Events: binary inputs (g1v1, g1v2; events g2v1, g2v2, g2v3 with g51 CTO), double-bit inputs (g3v1, g3v2; events
g4v1-3). Telemetry: analog inputs (g30v1-6; events g32v1-8 are decoded and dropped), counters (g20v1, v2, v5, v6;
frozen g21v1, v2, v5, v6, v9, v10; events g22 and g23 dropped). Delta counters, analog outputs and binary outputs a
class poll may return are parsed only to skip them.

## Build and test

```bash
go build -o bin/dnp3-agent ./cmd/dnp3-agent
go test -race ./...     # codec tests and master tests against an in-process fake outstation
DNP3_AGENT_E2E_PG=postgres://user:pass@localhost:5432/scratch go test ./storage/...   # shadow writer against PostgreSQL
```

## License

MIT
