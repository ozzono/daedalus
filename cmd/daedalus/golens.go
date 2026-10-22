// The golens-metrics embed: the registry behind /full-metrics and the
// feed that fills it. The daemon builds it at boot (startGolens), mounts
// its dashboard beside the htmx UI (ui.go's routes), and points the relay
// ledger's append observer at it, so every recorded provider request
// lands in the registry as metric samples the dashboard charts. Retention
// rides the module's own sqlite layer — the decided posture, no parallel
// schema: the module's roll-up windows persist across restarts and past
// the ledger ring's horizon, while the per-request rows themselves stay
// the in-memory ledger's, served by `daedalus dash list` and the run
// detail.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/GoLens-Project/golens-metrics"
	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/metricsrelay"
)

// The fed metrics' names and label sets. A golens metric is one series
// with one dashboard panel (labels are last-seen display metadata, not
// series dimensions), so the names split where the operator reads them
// apart — tokens in/out and the two cache sides, the request count, and
// the request duration — and the labels carry the bounded dimensions
// `dash list` prints: worker, model, role, and the key alias; the request
// counter adds the HTTP status. Task and round are unbounded per-request
// values with no single-valued label meaning, so they stay out: the
// per-row views are `daedalus dash list` and the run detail (the
// roll-up-schema gap raised for ponytail-design).
const (
	golensTokensIn   = "daedalus_tokens_in_total"
	golensTokensOut  = "daedalus_tokens_out_total"
	golensCacheRead  = "daedalus_tokens_cache_read_total"
	golensCacheWrite = "daedalus_tokens_cache_write_total"
	golensRequests   = "daedalus_requests_total"
	golensDuration   = "daedalus_request_duration_seconds"
)

var (
	golensTokenLabels   = []string{"worker", "model", "role", "alias"}
	golensRequestLabels = []string{"worker", "model", "role", "status", "alias"}
	// golensDurationBounds spans a provider round's realistic latency in
	// seconds — the library's HTTP-shaped defaults (5ms..10s) would pin
	// every LLM call into the top bucket.
	golensDurationBounds = []float64{0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600}
)

// startGolens builds and starts the daemon's registry: sqlite retention
// (metrics.storage_backend/storage_path — empty backend defaults sqlite,
// empty path defaults beside the daemon's other files), the layout
// document beside it so a saved dashboard survives restarts, and the six
// ledger metrics registered up front so the samples land typed — a Record
// for an unknown name auto-registers a plain label-less counter, which is
// not what these are.
func startGolens(cfg config.Config) (*golens.Registry, error) {
	gc := golens.DefaultConfig()
	gc.ProjectName = "daedalus"
	gc.DashboardSubtitle = "run metrics"
	// Unlimited label combinations (0): the fed dimensions are bounded
	// vocabularies — the deployment's workers, roles, statuses, and the
	// models it actually uses — while the default 256 cap would drop
	// every new combination past the cap for good (the tracker never
	// prunes), silently amputating a freshly configured model from the
	// panels.
	gc.MaxLabelSeriesPerMetric = 0
	backend := cfg.Metrics.StorageBackend
	if backend == "" {
		backend = "sqlite"
	}
	gc.Storage.Backend = backend
	if backend == "sqlite" {
		gc.Storage.Path = cfg.Metrics.StoragePath
		if gc.Storage.Path == "" {
			gc.Storage.Path = filepath.Join(daemonDir, "metrics-golens.db")
		}
	}
	gc.UI.DashboardConfig = filepath.Join(daemonDir, "metrics-golens-layout.json")
	// sqlite creates its file, never its parent — a fresh host's first
	// `dash start` reaches here before anything else has made the daemon
	// dir exist.
	if err := os.MkdirAll(daemonDir, 0o755); err != nil {
		return nil, fmt.Errorf("create daemon dir: %w", err)
	}
	reg, err := golens.New(gc)
	if err != nil {
		return nil, err
	}
	reg.Register(golensTokensIn, golens.CounterType, "Ledger input tokens", golensTokenLabels, nil, 0, 0)
	reg.Register(golensTokensOut, golens.CounterType, "Ledger output tokens", golensTokenLabels, nil, 0, 0)
	reg.Register(golensCacheRead, golens.CounterType, "Ledger cache-read tokens", golensTokenLabels, nil, 0, 0)
	reg.Register(golensCacheWrite, golens.CounterType, "Ledger cache-write tokens", golensTokenLabels, nil, 0, 0)
	reg.Register(golensRequests, golens.CounterType, "Relayed provider requests", golensRequestLabels, nil, 0, 0)
	reg.Register(golensDuration, golens.HistogramType, "Provider request duration", golensTokenLabels, golensDurationBounds, 0, 0)
	if err := reg.Start(context.Background()); err != nil {
		reg.Close()
		return nil, err
	}
	return reg, nil
}

// feedGolensEntry is the ledger's append observer: one recorded provider
// request becomes the six samples the counters and the duration histogram
// fold — Record never blocks (golens ingests through a bounded queue), so
// the relay's request path does not wait on the dashboard. The empty model
// renders as the same unknown bucket the usage tables use: failed calls
// carry no model name.
func feedGolensEntry(reg *golens.Registry, e metricsrelay.Entry) {
	model := e.Model
	if model == "" {
		model = modelUnknown
	}
	base := []golens.Label{
		{Name: "worker", Value: e.Worker},
		{Name: "model", Value: model},
		{Name: "role", Value: e.Role},
		{Name: "alias", Value: e.Alias},
	}
	status := append(base, golens.Label{Name: "status", Value: strconv.Itoa(e.Status)})
	reg.Record(golensTokensIn, float64(e.InputTokens), base...)
	reg.Record(golensTokensOut, float64(e.OutputTokens), base...)
	reg.Record(golensCacheRead, float64(e.CacheReadTokens), base...)
	reg.Record(golensCacheWrite, float64(e.CacheWriteTokens), base...)
	reg.Record(golensRequests, 1, status...)
	reg.Record(golensDuration, float64(e.DurationMS)/1000, base...)
}
