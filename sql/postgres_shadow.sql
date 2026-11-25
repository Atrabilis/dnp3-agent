-- Target table for the optional `postgres_shadow` output: one row per plant, device and telemetry snapshot (class 0
-- read), with every point's current value in a JSONB payload. Same generic "shadow" layout as other collectors, so
-- any outstation fits without changing the table. The agent does not create tables.
--
-- Indexes: the primary key is the ON CONFLICT target of the writer and also serves the usual reads (one device over
-- a time range: plant, device_name, slave_name, ts). No other index is needed until a query filters on the payload.

CREATE SCHEMA IF NOT EXISTS edge;

CREATE TABLE IF NOT EXISTS edge.example_dnp3_shadow (
  plant       TEXT        NOT NULL,
  ts          TIMESTAMPTZ NOT NULL,  -- when the snapshot was read (static values carry no outstation time)
  device_name TEXT        NOT NULL,
  slave_name  TEXT        NOT NULL,  -- group: always "telemetry"
  payload     JSONB       NOT NULL,
  series_key  TEXT        NOT NULL,  -- group again (keeps the generic key shape)
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (plant, device_name, slave_name, ts, series_key)
);

-- Payload: {"BI": {"Open(Any)": true, ...}, "DBI": {...}, "AI": {"Ia": 12.5, ...}, "Counter": {"CO Total": 41, ...},
--           "FrozenCounter": {...}, "flags": {"AI/Ia": "03"}, "iin": "0890", "iin_flags": ["need_time"]}
-- (frozen counters share indexes and names with the running counters, hence their own key; values are the
-- outstation's integers times the configured scale, e.g. 0.001 for a frequency sent in mHz.)
-- ("flags" lists only points whose quality is not plain "online"; names come from the agent config.)
-- Reading a measurement back:
--   SELECT ts, (payload #>> '{AI,Ia}')::double precision AS ia
--   FROM edge.example_dnp3_shadow
--   WHERE device_name = 'recloser' AND series_key = 'telemetry'
--   ORDER BY ts DESC LIMIT 10;
