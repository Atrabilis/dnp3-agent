package master

import (
	"fmt"
	"time"

	"github.com/atrabilis/dnp3-agent/dnp3"
)

// Telemetry is one class 0 snapshot: the current value of every point the outstation reports, by name.
type Telemetry struct {
	At time.Time // when the last fragment arrived (the outstation does not time-stamp static values)
	// Payload is stored as is (JSON):
	//   {"BI": {"Open(Any)": true, ...}, "DBI": {...}, "AI": {"Ia": 12.5, ...}, "Counter": {"CO Total": 41, ...},
	//    "FrozenCounter": {...}, "flags": {"AI/Ia": "03", ...}, "iin": "0890", "iin_flags": ["need_time", ...]}
	// Frozen counters (g21) share indexes and names with the running counters, so they get their own key.
	// "flags" lists only points whose quality is not plain "online" (0x01), so a normal snapshot stays small.
	Payload map[string]any
}

func newTelemetry() *Telemetry {
	return &Telemetry{Payload: map[string]any{
		"BI": map[string]any{}, "DBI": map[string]any{}, "AI": map[string]any{}, "Counter": map[string]any{},
		"FrozenCounter": map[string]any{}, "flags": map[string]string{},
	}}
}

// add merges the static values of one response fragment (events in the same fragment are left to the event path).
func (t *Telemetry) add(s *session, r dnp3.Response) {
	flags := t.Payload["flags"].(map[string]string)
	for _, p := range r.Points {
		if p.Event {
			continue
		}
		ref := s.ref(p)
		if p.Double {
			t.Payload["DBI"].(map[string]any)[ref] = p.Value
			ref = "DBI/" + ref
		} else {
			t.Payload["BI"].(map[string]any)[ref] = p.Value == 1
			ref = "BI/" + ref
		}
		if p.Flags != 0x01 {
			flags[ref] = fmt.Sprintf("%02x", p.Flags)
		}
	}
	for _, m := range r.Measures {
		if m.Event {
			continue
		}
		ref, key := s.measureRef(m), m.Kind
		if m.Frozen {
			key = "FrozenCounter"
		}
		t.Payload[key].(map[string]any)[ref] = s.scaled(m)
		if m.HasFlags && m.Flags != 0x01 {
			flags[key+"/"+ref] = fmt.Sprintf("%02x", m.Flags)
		}
	}
	var names []string
	for _, b := range IINNames {
		if r.IIN&b.Bit != 0 {
			names = append(names, b.Name)
		}
	}
	t.Payload["iin"] = fmt.Sprintf("%04x", r.IIN)
	t.Payload["iin_flags"] = names
}
