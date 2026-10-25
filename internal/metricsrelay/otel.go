// The OTLP receiver: the /v1/metrics arm of the relay listener (mounted
// whenever the daemon serves, not gated — D5; metrics.enabled gates
// agent staging, not the port). OTLP/HTTP-JSON, decoded with plain struct
// JSON — the proto JSON mapping's shapes, no protobuf dependency. Only
// claude_code.token.usage delta datapoints fold into the ledger, one Entry
// per resource with at least one folded datapoint; every other metric in
// an export is ignored. Agent-reported cost is never parsed — cost stays
// render-time from the operator's price table (the package comment), and
// these token counters are what that pricing reads. /v1/traces and
// /v1/logs are acknowledged and discarded by the ServeHTTP dispatch: the
// round must never stall on an exporter that cannot deliver, and
// daedalus keeps no traces.
package metricsrelay

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// SourceOtel and SourceOtelExternal stamp Entry.Source for otel-folded
// usage: daedalus-staged rounds (D4 provenance below) fold as otel; any
// other exporter on the host folds as otel-external — a label only (D6),
// shown on the dashboards, excluded from the daedalus folds, and carried
// into the golens feed like everything else (D7).
const (
	SourceOtel         = "otel"
	SourceOtelExternal = "otel-external"
)

// otelMetricUsage is the one metric the receiver folds — claude-code's
// token counter, whose datapoints carry type=input/output/cacheRead/
// cacheCreation plus the model.
const otelMetricUsage = "claude_code.token.usage"

// otlpDelta is the OTLP aggregation-temporality enum value for delta sums;
// cumulative sums are skipped — every export of a cumulative counter would
// re-report the process's whole history and multiply-fold the tokens.
const otlpDelta = 1

// otlpBodyCap bounds one export's read: a bounded request, never an
// unbounded one.
const otlpBodyCap = 8 << 20

// otlpExport mirrors ExportMetricsServiceRequest's JSON shape: a repeated
// resource, each carrying attributes and one or more scope metric lists.
type otlpExport struct {
	ResourceMetrics []otlpResourceMetrics `json:"resourceMetrics"`
}

type otlpResourceMetrics struct {
	Resource     otlpResource       `json:"resource"`
	ScopeMetrics []otlpScopeMetrics `json:"scopeMetrics"`
}

type otlpResource struct {
	Attributes []otlpAttr `json:"attributes"`
}

type otlpScopeMetrics struct {
	Metrics []otlpMetric `json:"metrics"`
}

type otlpMetric struct {
	Name string   `json:"name"`
	Sum  *otlpSum `json:"sum"`
}

type otlpSum struct {
	DataPoints  []otlpDataPoint `json:"dataPoints"`
	Temporality int             `json:"aggregationTemporality"`
}

type otlpDataPoint struct {
	Attributes []otlpAttr `json:"attributes"`
	AsInt      otlpInt    `json:"asInt"`
	// AsDouble is the value oneof's other arm: real claude exports encode
	// every counter — integer token counts included — under asDouble (the
	// proto JSON mapping carries one arm or the other, never both).
	AsDouble float64 `json:"asDouble"`
}

// value resolves the point's value from whichever arm the export
// carried, the double truncated (token counts never leave double's exact
// integer range). Both arms absent — a malformed point — reads zero, and
// so does a double this unauthenticated listener has no business
// folding: NaN or a negative/overflowing magnitude would poison a
// cumulative sum with implementation-defined garbage, so it clamps to
// zero.
func (dp otlpDataPoint) value() int64 {
	if dp.AsInt != 0 {
		return int64(dp.AsInt)
	}
	if math.IsNaN(dp.AsDouble) || dp.AsDouble < 0 || dp.AsDouble >= 1<<62 {
		return 0
	}
	return int64(dp.AsDouble)
}

type otlpAttr struct {
	Key   string    `json:"key"`
	Value otlpValue `json:"value"`
}

type otlpValue struct {
	StringValue string `json:"stringValue"`
}

// otlpInt decodes an OTLP integer: the proto JSON mapping stringifies
// int64 (it cannot round-trip through JS numbers exactly), and a bare
// number is tolerated for hand-rolled exporters.
type otlpInt int64

func (v *otlpInt) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		return nil
	}
	n, err := strconv.ParseInt(strings.Trim(string(b), `"`), 10, 64)
	if err != nil {
		return fmt.Errorf("otlp int %s: %w", b, err)
	}
	*v = otlpInt(n)
	return nil
}

// serveOTLP folds one export into the ledger and answers the empty
// success body. Receipt time stamps the folded entries — the ledger's
// other entries stamp request start, equally relay-local.
func (r *Relay) serveOTLP(w http.ResponseWriter, req *http.Request) {
	body, err := io.ReadAll(io.LimitReader(req.Body, otlpBodyCap+1))
	if err != nil {
		http.Error(w, "metricsrelay: read export: "+err.Error(), http.StatusBadRequest)
		return
	}
	if len(body) > otlpBodyCap {
		http.Error(w, "metricsrelay: export exceeds the body cap", http.StatusRequestEntityTooLarge)
		return
	}
	var ex otlpExport
	if err := json.Unmarshal(body, &ex); err != nil {
		http.Error(w, "metricsrelay: bad OTLP export: "+err.Error(), http.StatusBadRequest)
		return
	}
	at := time.Now()
	for _, rm := range ex.ResourceMetrics {
		task, role, round, worker, daedalus := otelProvenance(rm.Resource.Attributes)
		e := Entry{At: at, Source: SourceOtel}
		if daedalus {
			e.Task, e.Role, e.Round, e.Worker = task, role, round, worker
		} else {
			e.Source = SourceOtelExternal
		}
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				foldOTLPTokens(&e, m)
			}
		}
		if (e.InputTokens | e.OutputTokens | e.CacheReadTokens | e.CacheWriteTokens) != 0 {
			r.ledger.Append(e)
			log.Printf("metricsrelay: otel usage source=%s worker=%s task=%s role=%s round=%s model=%s in=%d out=%d",
				e.Source, e.Worker, e.Task, e.Role, e.Round, e.Model, e.InputTokens, e.OutputTokens)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("{}"))
}

// foldOTLPTokens sums one metric's delta datapoints into e by token type;
// a metric that is not the token counter, or a sum that is not delta, is
// ignored whole. Entries with nothing folded are dropped by the caller.
func foldOTLPTokens(e *Entry, m otlpMetric) {
	if m.Name != otelMetricUsage || m.Sum == nil || m.Sum.Temporality != otlpDelta {
		return
	}
	for _, dp := range m.Sum.DataPoints {
		typ, model := otelPointAttrs(dp.Attributes)
		if model != "" && e.Model == "" {
			e.Model = model
		}
		switch typ {
		case "input":
			e.InputTokens += dp.value()
		case "output":
			e.OutputTokens += dp.value()
		case "cacheRead":
			e.CacheReadTokens += dp.value()
		case "cacheCreation":
			e.CacheWriteTokens += dp.value()
		}
	}
}

// otelProvenance splits staged identifiers from foreign resource
// attributes (D4): a claude round exports daedalus.task/role/round — all
// three present and non-empty is the daedalus signature — plus
// daedalus.worker, the usage-by-worker fold's key, carried alongside
// them; a codex round exports otel.environment="daedalus" and cannot
// attribute per round. Anything else is foreign traffic, folded as
// otel-external — and a foreign batch never adopts the worker name it
// may name, so only the signature's presence gates the fold.
func otelProvenance(attrs []otlpAttr) (task, role, round, worker string, daedalus bool) {
	env := ""
	for _, a := range attrs {
		switch a.Key {
		case "daedalus.task":
			task = a.Value.StringValue
		case "daedalus.role":
			role = a.Value.StringValue
		case "daedalus.round":
			round = a.Value.StringValue
		case "daedalus.worker":
			worker = a.Value.StringValue
		case "otel.environment":
			env = a.Value.StringValue
		}
	}
	if task != "" && role != "" && round != "" {
		return task, role, round, worker, true
	}
	return "", "", "", "", env == "daedalus"
}

// otelPointAttrs reads one datapoint's token type and model.
func otelPointAttrs(attrs []otlpAttr) (typ, model string) {
	for _, a := range attrs {
		switch a.Key {
		case "type":
			typ = a.Value.StringValue
		case "model":
			model = a.Value.StringValue
		}
	}
	return typ, model
}
