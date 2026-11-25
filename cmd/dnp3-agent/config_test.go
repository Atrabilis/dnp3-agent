package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const baseConfig = `plant: p
devices:
  - name: rc
    host: 192.0.2.10
    outstation: 3
    master: 5
    telemetry_interval: 30s
    analog_inputs:
      0: {name: Ia}
      3: {name: Uab, scale: 1}
    counters:
      6: CO Total
output:
  postgres_events: {host_env: H, port_env: P, user_env: U, password_env: W, database_env: D, schema: edge, table: rc_events}
`

func load(t *testing.T, body string) (*Config, error) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "c.yml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return Load(p)
}

func TestConfigTelemetry(t *testing.T) {
	c, err := load(t, baseConfig+"  postgres_shadow: {host_env: H, port_env: P, user_env: U, password_env: W, database_env: D, schema: edge, table: rc_shadow}\n")
	if err != nil {
		t.Fatal(err)
	}
	o := options(&c.Devices[0])
	if o.TelemetryInterval.Seconds() != 30 || o.AnalogNames[0] != "Ia" || o.CounterNames[6] != "CO Total" || o.AnalogScale[3] != 1 {
		t.Errorf("options: %+v", o)
	}
	if _, ok := o.AnalogScale[0]; ok {
		t.Error("a point without scale must not get one")
	}
	// No shadow output: still valid (events only).
	if c, err := load(t, baseConfig); err != nil || c.Output.PostgresShadow != nil {
		t.Errorf("events-only config: %v", err)
	}
}

func TestConfigRejects(t *testing.T) {
	for name, body := range map[string]string{
		"shadow incomplete": baseConfig + "  postgres_shadow: {host_env: H, schema: edge, table: rc_shadow}\n",
		"same table":        baseConfig + "  postgres_shadow: {host_env: H, port_env: P, user_env: U, password_env: W, database_env: D, schema: edge, table: rc_events}\n",
		"bad interval":      strings.Replace(baseConfig, "telemetry_interval: 30s", "telemetry_interval: soon", 1),
		"unknown key":       strings.Replace(baseConfig, "counters:", "counterz:", 1),
		"analog as string":  strings.Replace(baseConfig, "0: {name: Ia}", "0: Ia", 1),
	} {
		if _, err := load(t, body); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
