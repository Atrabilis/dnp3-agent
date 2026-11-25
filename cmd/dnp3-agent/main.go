// Command dnp3-agent records binary and double-bit input events from DNP3 outstations (protection relays,
// reclosers) into PostgreSQL, through github.com/atrabilis/relay-events, and optionally periodic telemetry snapshots
// (binary inputs, analog inputs and counters) into a shadow table. Modes:
//
//	listen  run continuously for one device (-device): poll events, store them, confirm them only once stored
//	check         read-only: read class 0 (current state) once and print it
//	check-events  read-only: read class 1-3 events once and print them, without confirming (they stay in the
//	              outstation for the real master)
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	dotenv "github.com/joho/godotenv"

	"github.com/atrabilis/dnp3-agent/master"
	"github.com/atrabilis/dnp3-agent/storage/postgres_shadow"
	"github.com/atrabilis/relay-events/listen"
	"github.com/atrabilis/relay-events/storage/postgres_events"
)

var version = "dev"

func main() {
	configPath := flag.String("configPath", "", "Path to the YAML config file")
	envPath := flag.String("envPath", "/etc/dnp3-agent/dnp3-agent.env", "Path to the dotenv file with the output credentials")
	mode := flag.String("mode", "listen", "listen, check or check-events")
	deviceName := flag.String("device", "", "Device to use")
	metricsFile := flag.String("metricsFile", "", "Prometheus textfile for the listener metrics")
	checkConfig := flag.Bool("checkConfig", false, "Validate the config file and exit")
	showVersion := flag.Bool("version", false, "Print the version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return
	}
	if *configPath == "" {
		log.Fatal("-configPath is required")
	}
	cfg, err := Load(*configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	if *checkConfig {
		fmt.Printf("config OK: %d device(s)\n", len(cfg.Devices))
		return
	}
	d, ok := cfg.Find(*deviceName)
	if !ok {
		log.Fatalf("-device %q is not in the config", *deviceName)
	}
	o := options(d)
	switch *mode {
	case "check":
		os.Exit(check(o, 0))
	case "check-events":
		os.Exit(check(o, 1, 2, 3))
	case "listen":
		os.Exit(runListen(cfg, d, o, *envPath, *metricsFile))
	default:
		log.Fatalf("-mode %q: must be listen, check or check-events", *mode)
	}
}

func options(d *Device) master.Options {
	timeout, _ := duration(d.Timeout, 5*time.Second)
	poll, _ := duration(d.PollInterval, 2*time.Second)
	integrity, _ := duration(d.IntegrityInterval, time.Hour)
	hb, _ := duration(d.Heartbeat, 30*time.Second)
	telemetry, _ := duration(d.TelemetryInterval, time.Minute)
	names, scale := map[uint32]string{}, map[uint32]float64{}
	for i, a := range d.AnalogInputs {
		names[i] = a.Name
		if a.Scale != 0 {
			scale[i] = a.Scale
		}
	}
	return master.Options{Host: d.Host, Port: d.Port, Outstation: d.Outstation, Master: d.Master, Timeout: timeout,
		PollInterval: poll, IntegrityInterval: integrity, Heartbeat: hb, TelemetryInterval: telemetry,
		BinaryNames: d.BinaryInputs, DoubleNames: d.DoubleInputs, AnalogNames: names, AnalogScale: scale,
		CounterNames: d.Counters, Logf: log.Printf, Debug: os.Getenv("DNP3_DEBUG") != ""}
}

// check reads the given classes once and prints one JSON line per point; nothing is confirmed or written.
func check(o master.Options, classes ...int) int {
	ctx, cancel := context.WithTimeout(context.Background(), 4*o.Timeout+10*time.Second)
	defer cancel()
	rows, err := master.ReadOnce(ctx, o, classes...)
	if err != nil {
		log.Printf("check: %v", err)
		return listen.ExitLost
	}
	enc := json.NewEncoder(os.Stdout)
	for _, r := range rows {
		_ = enc.Encode(r)
	}
	return 0
}

func runListen(cfg *Config, d *Device, o master.Options, envPath, metricsFile string) int {
	if err := dotenv.Load(envPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		log.Printf("env file %s: %v", envPath, err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	sink, err := postgres_events.New(ctx, "events", cfg.Output.PostgresEvents, cfg.Plant, d.Name, o.SourceName())
	if err != nil {
		log.Printf("%v", err)
		return 1
	}
	defer sink.Close()
	if cfg.Output.PostgresShadow != nil {
		shadow, err := postgres_shadow.New(ctx, "shadow", *cfg.Output.PostgresShadow)
		if err != nil {
			log.Printf("%v", err)
			return 1
		}
		defer shadow.Close()
		snaps := make(chan master.Telemetry, 16)
		var mu sync.Mutex // guards closed: a late snapshot after shutdown must not send on the closed channel
		closed := false
		o.OnTelemetry = func(t master.Telemetry) {
			mu.Lock()
			defer mu.Unlock()
			if closed {
				return
			}
			select {
			case snaps <- t:
			default: // the database is slower than the snapshots: drop this one rather than block the session
				log.Printf("device %q: telemetry snapshot dropped, writer busy", d.Name)
			}
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			for t := range snaps {
				wctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				if err := shadow.Write(wctx, cfg.Plant, t.At, d.Name, "telemetry", t.Payload); err != nil {
					log.Printf("device %q: telemetry: %v", d.Name, err)
				}
				cancel()
			}
		}()
		defer func() {
			mu.Lock()
			closed = true
			close(snaps)
			mu.Unlock()
			<-done
		}()
	}
	log.Printf("listening to device %q (%s:%d, outstation %d), agent %s", d.Name, d.Host, d.Port, d.Outstation, version)
	timeout := o.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	hb := o.Heartbeat
	if hb <= 0 {
		hb = 30 * time.Second
	}
	err = listen.Run(ctx, listen.Options{
		Source: master.Source(o), SourceName: o.SourceName(), Watchdog: 3*hb + 2*timeout, StopGrace: 2*timeout + time.Second,
		Logf: log.Printf, Metrics: listen.NewMetrics(metricsFile, d.Name, o.SourceName()),
	}, sink)
	if err != nil {
		log.Printf("device %q: %v", d.Name, err)
		return 1
	}
	log.Printf("device %q: stopped", d.Name)
	return 0
}
