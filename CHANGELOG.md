# Changelog

## v0.1.0 (2026-11-25)

- First version: pure Go DNP3 master over TCP (link, transport and application layers).
- Events: binary and double-bit inputs with the outstation's time, recorded through relay-events (`listen` mode):
  confirmation only after storing, gaps on event buffer overflow, unsynchronized-time marking.
- Telemetry: analog inputs and counters (running and frozen); optional `postgres_shadow` output with a class 0
  snapshot every `telemetry_interval`, with names and scale factors per point in the config. Analog and counter
  events are confirmed and dropped.
- Read-only `check` and `check-events` modes.
