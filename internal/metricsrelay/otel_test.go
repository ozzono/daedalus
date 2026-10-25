package metricsrelay

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http"
	"testing"
	"time"
)

// TestOTLPDispatchAndStubs pins the receiver's mounting on the relay
// listener: POST /v1/metrics reaches the OTLP arm, the traces and logs
// arms are acknowledged-and-discarded stubs (the round must never stall on
// an exporter that cannot deliver), and any other method misses into the
// relay's default 404 — the exporter protocol is a POST.
func TestOTLPDispatchAndStubs(t *testing.T) {
	_, root := startRelay(t, &Ledger{}, NewRegistry(time.Minute))

	for _, path := range []string{"/v1/traces", "/v1/logs"} {
		status, body := postRelay(t, root+path, http.Header{}, []byte(`{}`))
		if status != http.StatusOK || string(body) != "{}" {
			t.Errorf("POST %s = (%d) %s, want the empty success body", path, status, body)
		}
	}

	resp, err := http.Get(root + "/v1/metrics")
	if err != nil {
		t.Fatalf("GET /v1/metrics: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("GET /v1/metrics = %d, want the default 404", resp.StatusCode)
	}
}

// TestOTLPExportRejections pins the bounded-request rule at the HTTP face:
// a body the JSON decoder cannot read is a 400, and a body past the cap is
// refused with 413 before any parsing — an export is a bounded request,
// never an unbounded one — and neither leaves a trace in the ledger.
func TestOTLPExportRejections(t *testing.T) {
	ledger := &Ledger{}
	_, root := startRelay(t, ledger, NewRegistry(time.Minute))

	status, body := postRelay(t, root+"/v1/metrics", http.Header{}, []byte("not json"))
	if status != http.StatusBadRequest {
		t.Errorf("bad export status = %d (%s), want 400", status, body)
	}

	status, body = postRelay(t, root+"/v1/metrics", http.Header{}, bytes.Repeat([]byte("a"), otlpBodyCap+1))
	if status != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized export status = %d (%s), want 413", status, body)
	}
	if ledger.Len() != 0 {
		t.Errorf("rejected exports left %d ledger entries, want 0", ledger.Len())
	}
}

// ledgerFold returns the whole ledger newest-first — the fold assertions
// read it instead of trusting entry order.
func ledgerFold(t *testing.T, l *Ledger) []Entry {
	t.Helper()
	entries, _, _ := l.Query("", 1, 1000)
	return entries
}

// TestOTLPFoldStampsDaedalusProvenance pins the claude-staged shape end to
// end: a resource carrying all three daedalus identifiers folds as a
// single otel entry attributed to the round — with the daedalus.worker
// attribute riding beside them, the usage-by-worker fold's key — its delta
// datapoints summed by token type, the model read off the first datapoint
// that names one — and the answer is the empty success body. An otel row
// carries no request-level signal: no wire, status, or duration.
func TestOTLPFoldStampsDaedalusProvenance(t *testing.T) {
	ledger := &Ledger{}
	_, root := startRelay(t, ledger, NewRegistry(time.Minute))

	body, err := json.Marshal(map[string]any{
		"resourceMetrics": []map[string]any{{
			"resource": map[string]any{"attributes": []map[string]any{
				{"key": "service.name", "value": map[string]any{"stringValue": "claude-code"}},
				{"key": "daedalus.worker", "value": map[string]any{"stringValue": "w-otel"}},
				{"key": "daedalus.task", "value": map[string]any{"stringValue": "wf-otel"}},
				{"key": "daedalus.role", "value": map[string]any{"stringValue": "dev"}},
				{"key": "daedalus.round", "value": map[string]any{"stringValue": "3"}},
			}},
			"scopeMetrics": []map[string]any{{
				"metrics": []map[string]any{{
					"name": otelMetricUsage,
					"sum": map[string]any{
						"aggregationTemporality": otlpDelta,
						"dataPoints": []map[string]any{
							{"asInt": "100", "attributes": []map[string]any{
								{"key": "type", "value": map[string]any{"stringValue": "input"}},
								{"key": "model", "value": map[string]any{"stringValue": "claude-opus-5"}},
							}},
							{"asInt": "20", "attributes": []map[string]any{
								{"key": "type", "value": map[string]any{"stringValue": "output"}},
							}},
							{"asInt": "7", "attributes": []map[string]any{
								{"key": "type", "value": map[string]any{"stringValue": "cacheRead"}},
							}},
							{"asInt": "3", "attributes": []map[string]any{
								{"key": "type", "value": map[string]any{"stringValue": "cacheCreation"}},
							}},
						},
					},
				}},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("marshal export: %v", err)
	}

	before := time.Now().Add(-time.Second)
	status, reply := postRelay(t, root+"/v1/metrics", http.Header{}, body)
	if status != http.StatusOK || string(reply) != "{}" {
		t.Fatalf("POST /v1/metrics = (%d) %s, want 200 with the empty success body", status, reply)
	}

	entries := ledgerFold(t, ledger)
	if len(entries) != 1 {
		t.Fatalf("ledger holds %d entries, want one folded entry", len(entries))
	}
	e := entries[0]
	if e.Source != SourceOtel || e.Task != "wf-otel" || e.Role != "dev" || e.Round != "3" || e.Worker != "w-otel" {
		t.Errorf("entry attribution = (%s %s %s %s worker %s), want the otel source and the staged identifiers", e.Source, e.Task, e.Role, e.Round, e.Worker)
	}
	if e.InputTokens != 100 || e.OutputTokens != 20 || e.CacheReadTokens != 7 || e.CacheWriteTokens != 3 {
		t.Errorf("entry usage = %+v, want the delta datapoints summed by type", e)
	}
	if e.Model != "claude-opus-5" {
		t.Errorf("entry model = %q, want the datapoint's model", e.Model)
	}
	// Receipt time stamps the fold — the ledger's other entries stamp
	// request start, equally relay-local.
	if e.At.Before(before) || e.At.After(time.Now().Add(time.Second)) {
		t.Errorf("entry stamped %v, want a receipt time near now", e.At)
	}
	if e.Wire != "" || e.Status != 0 || e.DurationMS != 0 {
		t.Errorf("entry = %+v, want the request-level fields empty (no relay saw this traffic)", e)
	}
}

// TestOTLPExternalProvenance pins the D4 split from the other side: the
// origin label rides staged-identifier presence — a claude round's full
// identifier trio folds as otel with its attribution (pinned above), a
// codex round's environment label alone still folds as a daedalus round
// but carries no per-round attribution to file it under, and anything
// else — a partial identifier set, foreign traffic, no attributes at all —
// folds labeled otel-external, its tokens kept for the full-metrics face.
func TestOTLPExternalProvenance(t *testing.T) {
	ledger := &Ledger{}
	_, root := startRelay(t, ledger, NewRegistry(time.Minute))

	cases := []struct {
		name    string
		attrs   [][2]string
		wantSrc string
	}{
		{"codex's environment label alone", [][2]string{{"otel.environment", "daedalus"}, {"service.name", "codex"}}, SourceOtel},
		{"a partial identifier set", [][2]string{{"daedalus.task", "wf-x"}, {"daedalus.role", "dev"}}, SourceOtelExternal},
		{"a foreign batch naming a worker", [][2]string{{"daedalus.worker", "w-foreign"}, {"service.name", "other"}}, SourceOtelExternal},
		{"foreign traffic", [][2]string{{"otel.environment", "acme"}, {"service.name", "other"}}, SourceOtelExternal},
		{"no attributes at all", nil, SourceOtelExternal},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			before := ledger.Len()
			attrs := make([]map[string]any, 0, len(c.attrs))
			for _, kv := range c.attrs {
				attrs = append(attrs, map[string]any{"key": kv[0], "value": map[string]any{"stringValue": kv[1]}})
			}
			body, err := json.Marshal(map[string]any{
				"resourceMetrics": []map[string]any{{
					"resource": map[string]any{"attributes": attrs},
					"scopeMetrics": []map[string]any{{
						"metrics": []map[string]any{{
							"name": otelMetricUsage,
							"sum": map[string]any{
								"aggregationTemporality": otlpDelta,
								"dataPoints": []map[string]any{
									{"asInt": "50", "attributes": []map[string]any{{"key": "type", "value": map[string]any{"stringValue": "input"}}}},
								},
							},
						}},
					}},
				}},
			})
			if err != nil {
				t.Fatalf("marshal export: %v", err)
			}
			status, _ := postRelay(t, root+"/v1/metrics", http.Header{}, body)
			if status != http.StatusOK {
				t.Fatalf("status = %d, want the export accepted", status)
			}
			entries := ledgerFold(t, ledger)
			if len(entries) != before+1 {
				t.Fatalf("ledger grew %d, want the external fold kept", len(entries)-before)
			}
			e := entries[0]
			if e.Source != c.wantSrc {
				t.Errorf("source = %q, want %q", e.Source, c.wantSrc)
			}
			if e.Task != "" || e.Role != "" || e.Round != "" || e.Worker != "" {
				t.Errorf("entry attributed to (%s %s %s worker %s), want no attribution off foreign traffic — a foreign batch never adopts the worker name it may carry", e.Task, e.Role, e.Round, e.Worker)
			}
			if e.InputTokens != 50 {
				t.Errorf("tokens = %d, want the datapoint folded beside the label", e.InputTokens)
			}
		})
	}
}

// TestOTLPIgnoresForeignMetricsAndCumulative pins the fold's filters: only
// the token counter's delta sum folds — a foreign metric name, a
// cumulative sum (every export of one would re-report the process's whole
// history and multiply-fold the tokens), and an unknown token type are
// ignored whole, and a resource with nothing folded records no entry at
// all — a zero-valued delta parses, sums to nothing, and is dropped.
func TestOTLPIgnoresForeignMetricsAndCumulative(t *testing.T) {
	ledger := &Ledger{}
	_, root := startRelay(t, ledger, NewRegistry(time.Minute))

	ident := func(task, role, round string) []map[string]any {
		return []map[string]any{
			{"key": "daedalus.task", "value": map[string]any{"stringValue": task}},
			{"key": "daedalus.role", "value": map[string]any{"stringValue": role}},
			{"key": "daedalus.round", "value": map[string]any{"stringValue": round}},
		}
	}
	usage := func(temporality int, dps ...map[string]any) map[string]any {
		return map[string]any{"name": otelMetricUsage, "sum": map[string]any{
			"aggregationTemporality": temporality, "dataPoints": dps,
		}}
	}
	pt := func(typ string, n string) map[string]any {
		return map[string]any{"asInt": n, "attributes": []map[string]any{
			{"key": "type", "value": map[string]any{"stringValue": typ}},
		}}
	}

	body, err := json.Marshal(map[string]any{
		"resourceMetrics": []map[string]any{
			{
				// Claude-staged, but every metric in it is ignorable: a
				// foreign name, a cumulative sum, an unknown token type.
				"resource": map[string]any{"attributes": ident("wf-a", "dev", "1")},
				"scopeMetrics": []map[string]any{{"metrics": []map[string]any{
					{"name": "claude_code.active_time_total", "sum": map[string]any{
						"aggregationTemporality": otlpDelta, "dataPoints": []map[string]any{{"asInt": "9"}},
					}},
					usage(2, pt("input", "5000")), // cumulative: skipped whole
					usage(otlpDelta, pt("sessionId", "1")),
				}}},
			},
			{
				// The second resource carries one foldable datapoint.
				"resource":     map[string]any{"attributes": ident("wf-b", "test", "2")},
				"scopeMetrics": []map[string]any{{"metrics": []map[string]any{usage(otlpDelta, pt("input", "11"))}}},
			},
			{
				// A zero-valued fold: parsed, summed to nothing, dropped.
				"resource":     map[string]any{"attributes": ident("wf-c", "dev", "3")},
				"scopeMetrics": []map[string]any{{"metrics": []map[string]any{usage(otlpDelta, pt("input", "0"))}}},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal export: %v", err)
	}

	if status, _ := postRelay(t, root+"/v1/metrics", http.Header{}, body); status != http.StatusOK {
		t.Fatalf("status = %d, want the export accepted with its ignorable metrics", status)
	}
	entries := ledgerFold(t, ledger)
	if len(entries) != 1 {
		t.Fatalf("ledger holds %d entries, want only wf-b's fold", len(entries))
	}
	if entries[0].Task != "wf-b" || entries[0].InputTokens != 11 {
		t.Errorf("entry = %+v, want wf-b's 11 input tokens", entries[0])
	}
}

// TestOTLPIntDecodes pins the proto-JSON integer mapping: int64 arrives
// stringified and a bare number is tolerated for hand-rolled exporters,
// while a non-integer string is a decode error and null leaves the field
// untouched.
func TestOTLPIntDecodes(t *testing.T) {
	var v otlpInt
	if err := json.Unmarshal([]byte(`"123"`), &v); err != nil || v != 123 {
		t.Errorf(`stringified decode = (%d, %v), want 123`, v, err)
	}
	v = 0
	if err := json.Unmarshal([]byte(`456`), &v); err != nil || v != 456 {
		t.Errorf("bare decode = (%d, %v), want 456", v, err)
	}
	v = 7
	if err := json.Unmarshal([]byte(`null`), &v); err != nil || v != 7 {
		t.Errorf("null decode = (%d, %v), want the field left untouched", v, err)
	}
	if err := json.Unmarshal([]byte(`"1.5"`), &v); err == nil {
		t.Errorf("fractional decode = %v, want an error", v)
	}
}

// TestOTLPFoldReadsTheDoubleArm pins the value oneof's other arm: real
// claude exports encode every counter — integer token counts included —
// under asDouble, so a double-armed export folds its exact integer values
// (a fractional double truncates; token counts never leave double's exact
// integer range), and a hostile negative double clamps to zero rather than
// poisoning a cumulative counter.
func TestOTLPFoldReadsTheDoubleArm(t *testing.T) {
	ledger := &Ledger{}
	_, root := startRelay(t, ledger, NewRegistry(time.Minute))

	body, err := json.Marshal(map[string]any{
		"resourceMetrics": []map[string]any{{
			"resource": map[string]any{"attributes": []map[string]any{
				{"key": "daedalus.task", "value": map[string]any{"stringValue": "wf-d"}},
				{"key": "daedalus.role", "value": map[string]any{"stringValue": "dev"}},
				{"key": "daedalus.round", "value": map[string]any{"stringValue": "1"}},
			}},
			"scopeMetrics": []map[string]any{{
				"metrics": []map[string]any{{
					"name": otelMetricUsage,
					"sum": map[string]any{
						"aggregationTemporality": otlpDelta,
						"dataPoints": []map[string]any{
							{"asDouble": 1000.0, "attributes": []map[string]any{
								{"key": "type", "value": map[string]any{"stringValue": "input"}},
								{"key": "model", "value": map[string]any{"stringValue": "claude-opus-5"}},
							}},
							{"asDouble": 20.5, "attributes": []map[string]any{
								{"key": "type", "value": map[string]any{"stringValue": "output"}},
							}},
							{"asDouble": -5, "attributes": []map[string]any{
								{"key": "type", "value": map[string]any{"stringValue": "cacheRead"}},
							}},
						},
					},
				}},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("marshal export: %v", err)
	}

	if status, _ := postRelay(t, root+"/v1/metrics", http.Header{}, body); status != http.StatusOK {
		t.Fatalf("status = %d, want the double-armed export accepted", status)
	}
	entries := ledgerFold(t, ledger)
	if len(entries) != 1 {
		t.Fatalf("ledger holds %d entries, want one folded entry", len(entries))
	}
	e := entries[0]
	if e.InputTokens != 1000 || e.OutputTokens != 20 || e.CacheReadTokens != 0 {
		t.Errorf("entry usage = in %d out %d read %d, want the doubles folded (1000, 20.5 truncated to 20, the negative clamped to 0)", e.InputTokens, e.OutputTokens, e.CacheReadTokens)
	}
}

// TestOTLPDataPointValueResolvesTheArms pins the point's value resolution
// at the arms themselves: a present asInt wins, a double truncates, and
// the doubles this unauthenticated listener has no business folding — NaN,
// a negative, a magnitude past int64's exact range — read zero, as does a
// malformed point carrying neither arm.
func TestOTLPDataPointValueResolvesTheArms(t *testing.T) {
	cases := []struct {
		name string
		dp   otlpDataPoint
		want int64
	}{
		{"the int arm", otlpDataPoint{AsInt: 42}, 42},
		{"the double arm truncates", otlpDataPoint{AsDouble: 10.9}, 10},
		{"a whole double", otlpDataPoint{AsDouble: 7}, 7},
		{"a NaN double", otlpDataPoint{AsDouble: math.NaN()}, 0},
		{"a negative double", otlpDataPoint{AsDouble: -1}, 0},
		{"an overflowing double", otlpDataPoint{AsDouble: float64(1 << 62)}, 0},
		{"an infinite double", otlpDataPoint{AsDouble: math.Inf(1)}, 0},
		{"neither arm", otlpDataPoint{}, 0},
	}
	for _, c := range cases {
		if got := c.dp.value(); got != c.want {
			t.Errorf("%s: value() = %d, want %d", c.name, got, c.want)
		}
	}
}
