package main

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/atrabilis/relay-events/storage/pgconn"
)

// Config is the agent YAML (one file per plant or device type).
type Config struct {
	Plant   string   `yaml:"plant"`
	Devices []Device `yaml:"devices"`
	Output  Output   `yaml:"output"`
}

// Device is one DNP3 outstation.
type Device struct {
	Name       string `yaml:"name"`
	Host       string `yaml:"host"`
	Port       int    `yaml:"port"`       // default 20000
	Outstation uint16 `yaml:"outstation"` // outstation link address
	Master     uint16 `yaml:"master"`     // our link address

	Timeout           string `yaml:"timeout"`            // response timeout, default 5s
	PollInterval      string `yaml:"poll_interval"`      // event poll period, default 2s
	IntegrityInterval string `yaml:"integrity_interval"` // class 0 read period, default 1h ("0s" = only at start)
	Heartbeat         string `yaml:"heartbeat"`          // default 30s
	// Class 0 snapshot period for output.postgres_shadow, default 60s ("0s" = only the session's own class 0 reads).
	TelemetryInterval string `yaml:"telemetry_interval"`

	// Point names by index, stored as the event reference (e.g. 7: SigOpenOc1F).
	BinaryInputs map[uint32]string `yaml:"binary_inputs"`
	DoubleInputs map[uint32]string `yaml:"double_inputs"`
	// Telemetry names (and scale factors) of analog inputs and counters, by index.
	AnalogInputs map[uint32]AnalogPoint `yaml:"analog_inputs"`
	Counters     map[uint32]string      `yaml:"counters"`
}

// AnalogPoint names an analog input; the outstation's value is multiplied by Scale (default 1).
type AnalogPoint struct {
	Name  string  `yaml:"name"`
	Scale float64 `yaml:"scale"`
}

// Output is the PostgreSQL events table (layout: sql/events.sql of relay-events) and, optionally, a shadow table for
// the telemetry snapshots (layout: sql/postgres_shadow.sql).
type Output struct {
	PostgresEvents pgconn.Config  `yaml:"postgres_events"`
	PostgresShadow *pgconn.Config `yaml:"postgres_shadow"`
}

// Load reads and validates a configuration file; unknown keys are rejected.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if err := c.validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &c, nil
}

func (c *Config) validate() error {
	if strings.TrimSpace(c.Plant) == "" {
		return fmt.Errorf("plant is empty")
	}
	if len(c.Devices) == 0 {
		return fmt.Errorf("devices is empty")
	}
	names := map[string]bool{}
	for i := range c.Devices {
		d := &c.Devices[i]
		if d.Name == "" || d.Host == "" {
			return fmt.Errorf("devices[%d]: name and host are required", i)
		}
		if names[d.Name] {
			return fmt.Errorf("device %q is repeated", d.Name)
		}
		names[d.Name] = true
		if d.Port == 0 {
			d.Port = 20000
		}
		if d.Port < 0 || d.Port > 65535 {
			return fmt.Errorf("device %q: port %d out of range", d.Name, d.Port)
		}
		if d.Outstation == d.Master {
			return fmt.Errorf("device %q: outstation and master addresses must differ", d.Name)
		}
		for field, v := range map[string]string{"timeout": d.Timeout, "poll_interval": d.PollInterval,
			"integrity_interval": d.IntegrityInterval, "heartbeat": d.Heartbeat, "telemetry_interval": d.TelemetryInterval} {
			if _, err := duration(v, 0); err != nil {
				return fmt.Errorf("device %q: %s: %w", d.Name, field, err)
			}
		}
	}
	if err := checkPG("postgres_events", c.Output.PostgresEvents); err != nil {
		return err
	}
	if sh := c.Output.PostgresShadow; sh != nil {
		if err := checkPG("postgres_shadow", *sh); err != nil {
			return err
		}
		if sh.Schema == c.Output.PostgresEvents.Schema && sh.Table == c.Output.PostgresEvents.Table {
			return fmt.Errorf("output.postgres_shadow and output.postgres_events must be different tables")
		}
	}
	return nil
}

func checkPG(name string, pg pgconn.Config) error {
	for field, v := range map[string]string{"host_env": pg.HostEnv, "port_env": pg.PortEnv, "user_env": pg.UserEnv,
		"password_env": pg.PasswordEnv, "database_env": pg.DatabaseEnv, "schema": pg.Schema, "table": pg.Table} {
		if strings.TrimSpace(v) == "" {
			return fmt.Errorf("output.%s.%s is empty", name, field)
		}
	}
	return nil
}

// Find returns the device with the given name.
func (c *Config) Find(name string) (*Device, bool) {
	for i := range c.Devices {
		if c.Devices[i].Name == name {
			return &c.Devices[i], true
		}
	}
	return nil, false
}

// duration parses a Go duration; empty means def. Negative values are rejected.
func duration(s string, def time.Duration) (time.Duration, error) {
	if strings.TrimSpace(s) == "" {
		return def, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("%q is not a non-negative duration", s)
	}
	return d, nil
}
