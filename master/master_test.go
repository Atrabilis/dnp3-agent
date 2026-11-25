package master

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/atrabilis/dnp3-agent/dnp3"
	"github.com/atrabilis/relay-events/listen"
	"github.com/atrabilis/relay-events/listentest"
)

func logf(format string, args ...any) {
	if os.Getenv("DNP3_TEST_LOG") != "" {
		fmt.Fprintf(os.Stderr, "  log: "+format+"\n", args...)
	}
}

func opts(port int) Options {
	return Options{Host: "127.0.0.1", Port: port, Outstation: 3, Master: 1, Timeout: 500 * time.Millisecond,
		PollInterval: 50 * time.Millisecond, Heartbeat: 100 * time.Millisecond,
		BinaryNames: map[uint32]string{5: "SigOpen", 6: "SigOpenProt"}, Logf: logf}
}

type run struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup
	err    error
	m      *listen.Metrics
}

func start(o Options, sink listen.Sink) *run {
	ctx, cancel := context.WithCancel(context.Background())
	r := &run{cancel: cancel, m: listen.NewMetrics("", "rc", o.SourceName())}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.err = listen.Run(ctx, listen.Options{Source: Source(o), SourceName: o.SourceName(), Watchdog: 2 * time.Second,
			StopGrace: time.Second, BackoffMin: 50 * time.Millisecond, BackoffMax: 200 * time.Millisecond,
			Logf: logf, Metrics: r.m}, sink)
	}()
	return r
}

func (r *run) stop() error { r.cancel(); r.wg.Wait(); return r.err }

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func values(sink *listentest.MemSink, ref string) []listen.Event {
	var out []listen.Event
	for _, e := range sink.Snapshot() {
		if e.Kind == listen.KindValue && e.Ref == ref {
			out = append(out, e)
		}
	}
	return out
}

func TestEventsStoredThenConfirmed(t *testing.T) {
	os1 := newFakeOutstation(t, 3, 1)
	t0 := time.Date(2026, 11, 23, 14, 0, 0, 125e6, time.UTC)
	os1.addEvent(5, true, t0)
	os1.addEvent(6, true, t0.Add(40*time.Millisecond))
	os1.static[0], os1.static[1] = false, true
	sink := listentest.NewMemSink()
	r := start(opts(os1.port()), sink)
	waitFor(t, "events stored and confirmed", func() bool { return len(values(sink, "SigOpen")) == 1 && os1.pending() == 0 })
	waitFor(t, "static values", func() bool { return len(values(sink, "BI1")) == 1 })
	os1.addEvent(5, false, t0.Add(time.Second))
	waitFor(t, "a later event", func() bool { return len(values(sink, "SigOpen")) == 2 && os1.pending() == 0 })
	if err := r.stop(); err != nil {
		t.Fatal(err)
	}
	ev := values(sink, "SigOpen")[0]
	if ev.DeviceTS == nil || !ev.DeviceTS.Equal(t0) || ev.Reasons[0] != "event" || ev.Quality != "01" || ev.EntryID != "" {
		t.Errorf("stored event: %+v", ev)
	}
	var v map[string]any
	_ = json.Unmarshal(ev.Value, &v)
	if v["stVal"] != true || v["index"] != float64(5) {
		t.Errorf("value %s", ev.Value)
	}
	st := values(sink, "BI1")[0]
	if st.Reasons[0] != "static" || st.DeviceTS != nil {
		t.Errorf("static value: %+v", st)
	}
	if sink.Count(listen.KindSession) != 1 || sink.Count(listen.KindGap) != 0 {
		t.Errorf("sessions %d gaps %d", sink.Count(listen.KindSession), sink.Count(listen.KindGap))
	}
}

type failingSink struct {
	*listentest.MemSink
	mu   sync.Mutex
	fail bool
}

func (f *failingSink) Write(ctx context.Context, ev []listen.Event) error {
	f.mu.Lock()
	fail := f.fail
	f.mu.Unlock()
	if fail && len(ev) > 0 && ev[0].Kind == listen.KindValue {
		return errors.New("database unavailable")
	}
	return f.MemSink.Write(ctx, ev)
}

func TestNotConfirmedWhenNotStoredAndNoDuplicates(t *testing.T) {
	os1 := newFakeOutstation(t, 3, 1)
	t0 := time.Date(2026, 11, 23, 15, 0, 0, 0, time.UTC)
	os1.addEvent(6, true, t0)
	sink := &failingSink{MemSink: listentest.NewMemSink(), fail: true}
	r := start(opts(os1.port()), sink)
	waitFor(t, "a failed store", func() bool { reads, _ := os1.counts(); return r.m.Snapshot().Sessions >= 1 && reads >= 1 })
	time.Sleep(200 * time.Millisecond)
	if _, confirms := os1.counts(); os1.pending() != 1 || confirms != 0 {
		t.Fatalf("event confirmed although not stored: pending %d confirms %d", os1.pending(), confirms)
	}
	sink.mu.Lock()
	sink.fail = false
	sink.mu.Unlock()
	waitFor(t, "stored on a later session and then confirmed", func() bool { return os1.pending() == 0 })
	if err := r.stop(); err != nil {
		t.Fatal(err)
	}
	if n := len(values(sink.MemSink, "SigOpenProt")); n != 1 {
		t.Errorf("event stored %d times", n)
	}
}

func TestMultiFragmentResponseEachConfirmed(t *testing.T) {
	os1 := newFakeOutstation(t, 3, 1)
	os1.maxPerFr = 2
	t0 := time.Date(2026, 11, 23, 16, 0, 0, 0, time.UTC)
	for i := 0; i < 7; i++ {
		os1.addEvent(uint32(10+i), i%2 == 0, t0.Add(time.Duration(i)*time.Millisecond))
	}
	sink := listentest.NewMemSink()
	r := start(opts(os1.port()), sink)
	waitFor(t, "seven events", func() bool { return sink.Count(listen.KindValue) >= 7 && os1.pending() == 0 })
	if err := r.stop(); err != nil {
		t.Fatal(err)
	}
	if _, confirms := os1.counts(); confirms < 4 {
		t.Errorf("fragments confirmed: %d, want 4", confirms)
	}
}

func TestNeedTimeAndOverflowFlags(t *testing.T) {
	os1 := newFakeOutstation(t, 3, 1)
	os1.iin = dnp3.IINNeedTime | dnp3.IINEventBufferOverflow
	t0 := time.Date(2026, 11, 23, 17, 0, 0, 0, time.UTC)
	os1.addEvent(5, true, t0)
	sink := listentest.NewMemSink()
	r := start(opts(os1.port()), sink)
	waitFor(t, "event", func() bool { return len(values(sink, "SigOpen")) == 1 })
	ev := values(sink, "SigOpen")[0]
	if ev.TimeQuality == nil || *ev.TimeQuality&listen.TQClockNotSynchronized == 0 {
		t.Errorf("need-time IIN not reflected in time quality: %+v", ev.TimeQuality)
	}
	if r.m.Snapshot().UnsyncedStamps == 0 {
		t.Error("unsynced stamps not counted")
	}
	// The overflow flag on the first session is history the master never had: not a gap.
	if sink.Count(listen.KindGap) != 0 {
		t.Error("overflow at the start of the first session recorded as a gap")
	}
	if err := r.stop(); err != nil {
		t.Fatal(err)
	}
}

func TestAddressMismatchStopsForGood(t *testing.T) {
	os1 := newFakeOutstation(t, 3, 1)
	os1.srcAddr = 9
	err := listen.Run(context.Background(), listen.Options{Source: Source(opts(os1.port())), SourceName: "dnp3:3",
		Watchdog: 2 * time.Second, BackoffMin: 50 * time.Millisecond, Logf: logf}, listentest.NewMemSink())
	if !errors.Is(err, listen.ErrMismatch) {
		t.Fatalf("want ErrMismatch, got %v", err)
	}
}

func TestSilentOutstationAndReconnect(t *testing.T) {
	os1 := newFakeOutstation(t, 3, 1)
	os1.mu.Lock()
	os1.silent = true
	os1.mu.Unlock()
	sink := listentest.NewMemSink()
	r := start(opts(os1.port()), sink)
	waitFor(t, "a timed-out session", func() bool { return r.m.Snapshot().LastExit == listen.ExitLost })
	os1.mu.Lock()
	os1.silent = false
	os1.mu.Unlock()
	os1.addEvent(5, true, time.Now())
	waitFor(t, "recovery", func() bool { return len(values(sink, "SigOpen")) == 1 })
	os1.dropConnections()
	os1.addEvent(5, false, time.Now())
	waitFor(t, "reconnect after a dropped connection", func() bool { return len(values(sink, "SigOpen")) == 2 })
	if err := r.stop(); err != nil {
		t.Fatal(err)
	}
}

func TestAnswersLinkStatusRequest(t *testing.T) {
	os1 := newFakeOutstation(t, 3, 1)
	os1.linkReq = true
	sink := listentest.NewMemSink()
	r := start(opts(os1.port()), sink)
	select {
	case <-os1.linkResp:
	case <-time.After(3 * time.Second):
		t.Fatal("link status request not answered")
	}
	if err := r.stop(); err != nil {
		t.Fatal(err)
	}
}

func TestFramesForAnotherMasterAddressAreIgnored(t *testing.T) {
	// Seen on a real recloser: its link status requests go to the master address configured in it (5), not to ours.
	os1 := newFakeOutstation(t, 3, 1)
	os1.linkReq, os1.linkReqTo = true, 5
	sink := listentest.NewMemSink()
	r := start(opts(os1.port()), sink)
	os1.addEvent(5, true, time.Now())
	waitFor(t, "event after the foreign frame", func() bool { return len(values(sink, "SigOpen")) == 1 })
	if err := r.stop(); err != nil {
		t.Fatal(err)
	}
	if r.m.Snapshot().Sessions != 1 {
		t.Errorf("sessions %d: the foreign frame must not end the session", r.m.Snapshot().Sessions)
	}
}

func TestTelemetrySnapshotsAndAnalogEvents(t *testing.T) {
	os1 := newFakeOutstation(t, 3, 1)
	os1.static[0], os1.static[1] = false, true
	os1.analogs[0], os1.analogs[1], os1.analogs[2] = 125, -3, 23100
	os1.counters[0] = 41
	os1.analogEv = 3 // deadband events waiting in the outstation: must be confirmed, not stored
	os1.addEvent(5, true, time.Date(2026, 11, 25, 12, 0, 1, 0, time.UTC))
	var mu sync.Mutex
	var snaps []Telemetry
	o := opts(os1.port())
	o.TelemetryInterval = 150 * time.Millisecond
	o.AnalogNames = map[uint32]string{0: "Ia", 2: "Uab"}
	o.AnalogScale = map[uint32]float64{2: 0.001}
	o.CounterNames = map[uint32]string{0: "CO Total"}
	o.OnTelemetry = func(tm Telemetry) { mu.Lock(); snaps = append(snaps, tm); mu.Unlock() }
	sink := listentest.NewMemSink()
	r := start(o, sink)
	count := func() int { mu.Lock(); defer mu.Unlock(); return len(snaps) }
	waitFor(t, "three snapshots", func() bool { return count() >= 3 })
	waitFor(t, "analog events confirmed", func() bool { p, _ := os1.analogPending(); return p == 0 })
	waitFor(t, "binary event stored", func() bool { return len(values(sink, "SigOpen")) == 1 })
	if err := r.stop(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	last := snaps[len(snaps)-1]
	mu.Unlock()
	ai := last.Payload["AI"].(map[string]any)
	if ai["Ia"] != float64(125) || ai["AI1"] != float64(-3) || ai["Uab"] != 23.1 {
		t.Errorf("analog inputs: %v", ai)
	}
	if c := last.Payload["Counter"].(map[string]any); c["CO Total"] != float64(41) {
		t.Errorf("counters (a frozen counter with the same index must not overwrite them): %v", c)
	}
	if c := last.Payload["FrozenCounter"].(map[string]any); c["CO Total"] != float64(0) {
		t.Errorf("frozen counters: %v", c)
	}
	if bi := last.Payload["BI"].(map[string]any); bi["BI0"] != false || bi["BI1"] != true {
		t.Errorf("binary inputs: %v", bi)
	}
	if f := last.Payload["flags"].(map[string]string); len(f) != 1 || f["FrozenCounter/CO Total"] != "43" {
		t.Errorf("flags: only the never-frozen counter should be listed: %v", f)
	}
	if last.At.IsZero() {
		t.Error("snapshot without time")
	}
	// Only the class 0 read at the session start goes to the event table; the telemetry reads do not.
	if n := len(values(sink, "BI1")); n != 1 {
		t.Errorf("static rows in the event table: %d, want 1 (session start only)", n)
	}
	if _, class0 := os1.analogPending(); class0 < 3 {
		t.Errorf("class 0 reads: %d", class0)
	}
	for _, e := range sink.Snapshot() {
		if e.Kind == listen.KindValue && (e.Ref == "Ia" || e.Ref == "AI0") {
			t.Errorf("analog value stored as an event: %+v", e)
		}
	}
}

func TestNoTelemetryWithoutCallback(t *testing.T) {
	os1 := newFakeOutstation(t, 3, 1)
	os1.analogs[0] = 1
	o := opts(os1.port())
	o.TelemetryInterval = 50 * time.Millisecond // ignored: no OnTelemetry
	r := start(o, listentest.NewMemSink())
	time.Sleep(400 * time.Millisecond)
	if err := r.stop(); err != nil {
		t.Fatal(err)
	}
	if _, class0 := os1.analogPending(); class0 != 1 {
		t.Errorf("class 0 reads without a telemetry callback: %d, want 1 (session start)", class0)
	}
}
