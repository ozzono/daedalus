package activities

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ozzono/daedalus/internal/config"
)

// classifyServer stubs the decision endpoint's systemone surface: every
// request is recorded and answered with the named choice and confidence as
// the "verdict" question's scored answer. The request body is decoded into
// a locally-tagged shape — not the production structs — so a wire-tag drift
// in the implementation fails these pins. The requests slice and a mutex
// keep the handlers race-safe under the -race-defaulted suite.
type classifyServer struct {
	srv      *httptest.Server
	mu       sync.Mutex
	requests []classifyRequest
	choice   string
	conf     float64
	status   int
	rawBody  string // when non-empty, sent verbatim instead of the scored answer
}

type classifyRequest struct {
	method      string
	path        string
	model       string
	auth        string
	contentType string
	// state is the request's state field — the marker-less review the
	// decision model must classify.
	state string
	// questions is the request's named question map, decoded with the wire
	// tags the endpoint contract fixes.
	questions map[string]struct {
		Type         string            `json:"type"`
		Instructions string            `json:"instructions"`
		Criteria     map[string]string `json:"criteria"`
	}
}

func newClassifyServer(t *testing.T, choice string, conf float64) *classifyServer {
	t.Helper()
	cs := &classifyServer{choice: choice, conf: conf, status: http.StatusOK}
	cs.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Model     string `json:"model"`
			State     string `json:"state"`
			Questions map[string]struct {
				Type         string            `json:"type"`
				Instructions string            `json:"instructions"`
				Criteria     map[string]string `json:"criteria"`
			} `json:"questions"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		cs.mu.Lock()
		cs.requests = append(cs.requests, classifyRequest{
			method:      r.Method,
			path:        r.URL.Path,
			model:       body.Model,
			auth:        r.Header.Get("Authorization"),
			contentType: r.Header.Get("Content-Type"),
			state:       body.State,
			questions:   body.Questions,
		})
		cs.mu.Unlock()
		if cs.status != http.StatusOK {
			http.Error(w, "boom", cs.status)
			return
		}
		payload := cs.rawBody
		if payload == "" {
			b, _ := json.Marshal(map[string]any{
				"answers": map[string]any{
					"verdict": map[string]any{
						"type":          "choice",
						"choice":        cs.choice,
						"probabilities": map[string]float64{cs.choice: cs.conf},
						"confidence":    cs.conf,
					},
				},
			})
			payload = string(b)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(payload))
	}))
	t.Cleanup(cs.srv.Close)
	return cs
}

func (cs *classifyServer) count() int {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return len(cs.requests)
}

func (cs *classifyServer) last() classifyRequest {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.requests[len(cs.requests)-1]
}

// armVerdictRepair points the repair layer at the stub endpoint the way the
// worker env does and arms the optional key.
func armVerdictRepair(t *testing.T, cs *classifyServer, key string) {
	t.Helper()
	t.Setenv(config.VerdictRepairURLEnv, cs.srv.URL)
	t.Setenv(config.VerdictRepairModelEnv, "tev1:latest")
	if key != "" {
		t.Setenv(config.VerdictRepairKeyEnv, key)
	}
}

// assertVerdictQuestion pins the classify request's question shape: the
// named "verdict" question is a choice question whose criteria offer
// exactly the parser's marker set — the choice set cannot drift from the
// set the verdict parser exact-matches — each with a non-empty description.
func assertVerdictQuestion(t *testing.T, req classifyRequest) {
	t.Helper()
	q, ok := req.questions["verdict"]
	if !ok {
		t.Fatalf("questions = %v, want a named \"verdict\" question", req.questions)
	}
	if q.Type != "choice" {
		t.Errorf("verdict question type = %q, want \"choice\" (the answer is a scored pick, not generated text)", q.Type)
	}
	if q.Instructions == "" {
		t.Error("verdict question instructions are empty, want the question instructed")
	}
	if len(q.Criteria) != len(verdictMarkers) {
		t.Errorf("criteria = %v, want exactly the %d markers the verdict parser accepts", q.Criteria, len(verdictMarkers))
	}
	for _, marker := range verdictMarkers {
		if q.Criteria[marker] == "" {
			t.Errorf("criteria = %v, want a non-empty description for the marker %q", q.Criteria, marker)
		}
	}
}

// TestRepairReviewVerdictPromotes pins the repair's one routing decision at
// the promotion boundary — and the systemone wire that feeds it: the request
// is a POST to <url>/systemone carrying the configured model, the marker-less
// review as state data, and the named verdict choice question; the scored
// answer's choice and confidence ride back as the round's annotation. An
// APPROVED classification at repairPromotionThreshold promotes the
// marker-less round to approval — the promoted round ends the review loop
// exactly as a parsed approval would (the workflow decides approval before
// its strike accounting) — while one below it, and every other verdict at
// any confidence, only annotates the round (LikelyVerdict and
// LikelyConfidence) and keeps it a changes-requested round:
// NEEDS_MAINTAINER and REBUILD inferences never trigger their lifecycle
// actions, and the repair outcome never touches strike accounting (only the
// workflow counts strikes). The 0.9/0.899 pair is the threshold's mutation
// pin both directions: moving the constant in either direction flips one
// case.
func TestRepairReviewVerdictPromotes(t *testing.T) {
	cases := []struct {
		label        string
		choice       string
		conf         float64
		key          string
		wantApproved bool
		wantVerdict  string
		wantConf     float64
	}{
		{"APPROVED at the threshold promotes", "APPROVED", 0.9, "", true, "APPROVED", 0.9},
		{"APPROVED below the threshold only annotates", "APPROVED", 0.899, "", false, "APPROVED", 0.899},
		{"APPROVED above the threshold promotes", "APPROVED", 1, "", true, "APPROVED", 1},
		{"confidence zero is in range and never promotes", "APPROVED", 0, "", false, "APPROVED", 0},
		{"changes-requested annotates without routing", "CHANGES_REQUESTED", 0.99, "", false, "CHANGES_REQUESTED", 0.99},
		{"needs-maintainer annotates without parking", "NEEDS_MAINTAINER", 0.99, "", false, "NEEDS_MAINTAINER", 0.99},
		{"rebuild annotates without routing", "REBUILD", 0.7, "", false, "REBUILD", 0.7},
		{"an armed key rides the request", "APPROVED", 0.95, "sk-local", true, "APPROVED", 0.95},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			cs := newClassifyServer(t, tc.choice, tc.conf)
			armVerdictRepair(t, cs, tc.key)
			in := ReviewResult{NoVerdict: true, Comments: "looks good, but the marker line never arrived"}

			out := repairReviewVerdict(context.Background(), in)

			if cs.count() != 1 {
				t.Fatalf("decision endpoint dialed %d times, want exactly one classify call", cs.count())
			}
			req := cs.last()
			if req.method != http.MethodPost {
				t.Errorf("request method = %q, want POST (the systemone surface serves POST only)", req.method)
			}
			if req.path != "/systemone" {
				t.Errorf("request path = %q, want /systemone (the configured url is the endpoint's /v1 base)", req.path)
			}
			if req.model != "tev1:latest" {
				t.Errorf("request model = %q, want the configured model name", req.model)
			}
			if req.contentType != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", req.contentType)
			}
			wantAuth := ""
			if tc.key != "" {
				wantAuth = "Bearer " + tc.key
			}
			if req.auth != wantAuth {
				t.Errorf("Authorization = %q, want %q (the key is optional)", req.auth, wantAuth)
			}
			// The state carries the round's marker-less output verbatim —
			// a request that loses the review text classifies nothing —
			// and the verdict question offers the parser's marker set.
			if req.state != in.Comments {
				t.Errorf("request state = %q, want the marker-less review text carried as data", req.state)
			}
			assertVerdictQuestion(t, req)
			if out.LikelyVerdict != tc.wantVerdict || out.LikelyConfidence != tc.wantConf {
				t.Errorf("annotation = %q/%v, want %q/%v", out.LikelyVerdict, out.LikelyConfidence, tc.wantVerdict, tc.wantConf)
			}
			if out.Approved != tc.wantApproved {
				t.Errorf("Approved = %v, want %v (confidence %v vs the 0.9 promotion threshold)", out.Approved, tc.wantApproved, tc.wantConf)
			}
		})
	}

	t.Run("a trailing-slash url still hits /systemone", func(t *testing.T) {
		cs := newClassifyServer(t, "APPROVED", 0.95)
		armVerdictRepair(t, cs, "")
		t.Setenv(config.VerdictRepairURLEnv, cs.srv.URL+"/")
		in := ReviewResult{NoVerdict: true, Comments: "folded"}

		repairReviewVerdict(context.Background(), in)

		if cs.count() != 1 {
			t.Fatalf("decision endpoint dialed %d times, want exactly one classify call", cs.count())
		}
		if req := cs.last(); req.path != "/systemone" {
			t.Errorf("request path = %q, want /systemone (the url's trailing slash is trimmed, not doubled)", req.path)
		}
	})
}

// TestRepairReviewVerdictFailOpen pins the layer's contract on every
// failure: an unconfigured, budget-starved, unreachable, erroring,
// garbage-answering endpoint returns the result untouched — the
// pre-repair behavior byte for byte (changes-requested, no annotation),
// so the marker-less strike budget stays the layer's only retry policy.
// On the systemone wire the answer is a scored choice, so garbage is any
// body the answers decode cannot stomach, a missing or unknown choice
// (the choice is exact-matched against the parser's marker set — never
// normalized), or a confidence off the [0,1] scale.
func TestRepairReviewVerdictFailOpen(t *testing.T) {
	garbage := []struct {
		label   string
		choice  string
		conf    float64
		rawBody string
	}{
		{"prose instead of JSON", "", 0, "The review looks fine to me, approved."},
		{"an unknown choice", "MAYBE", 0.99, ""},
		{"a lowercase choice is not normalized", "approved", 0.99, ""},
		{"a different confidence scale", "APPROVED", 90, ""},
		{"a negative confidence", "APPROVED", -0.1, ""},
		{"an empty answers map", "", 0, `{"answers": {}}`},
		{"no answer for the verdict question", "", 0, `{"answers": {"sentiment": {"type": "choice", "choice": "APPROVED", "confidence": 1}}}`},
	}
	for _, tc := range garbage {
		t.Run(tc.label, func(t *testing.T) {
			cs := newClassifyServer(t, tc.choice, tc.conf)
			cs.rawBody = tc.rawBody
			armVerdictRepair(t, cs, "")
			in := ReviewResult{NoVerdict: true, Comments: "connection reset by peer"}

			out := repairReviewVerdict(context.Background(), in)

			if out != in {
				t.Errorf("result = %+v, want it untouched by the failed repair", out)
			}
			if cs.count() != 1 {
				t.Fatalf("decision endpoint dialed %d times, want exactly one classify attempt", cs.count())
			}
		})
	}

	t.Run("unconfigured is silent fail-open", func(t *testing.T) {
		t.Setenv(config.VerdictRepairURLEnv, "")
		t.Setenv(config.VerdictRepairModelEnv, "")
		t.Setenv(config.VerdictRepairKeyEnv, "")
		in := ReviewResult{NoVerdict: true, Comments: "folded"}
		if out := repairReviewVerdict(context.Background(), in); out != in {
			t.Errorf("result = %+v, want it untouched with the layer off", out)
		}
	})

	t.Run("url without model is still off", func(t *testing.T) {
		t.Setenv(config.VerdictRepairURLEnv, "http://127.0.0.1:1/v1")
		t.Setenv(config.VerdictRepairModelEnv, "")
		in := ReviewResult{NoVerdict: true, Comments: "folded"}
		if out := repairReviewVerdict(context.Background(), in); out != in {
			t.Errorf("result = %+v, want it untouched with the model unset", out)
		}
	})

	t.Run("an unreachable endpoint fails open", func(t *testing.T) {
		dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		dead.Close() // the port is closed; the URL still names it
		t.Setenv(config.VerdictRepairURLEnv, dead.URL)
		t.Setenv(config.VerdictRepairModelEnv, "tev1:latest")
		in := ReviewResult{NoVerdict: true, Comments: "folded"}
		if out := repairReviewVerdict(context.Background(), in); out != in {
			t.Errorf("result = %+v, want it untouched when the endpoint is unreachable", out)
		}
	})

	t.Run("an HTTP error status fails open", func(t *testing.T) {
		cs := newClassifyServer(t, "APPROVED", 1)
		cs.status = http.StatusInternalServerError
		armVerdictRepair(t, cs, "")
		in := ReviewResult{NoVerdict: true, Comments: "folded"}
		if out := repairReviewVerdict(context.Background(), in); out != in {
			t.Errorf("result = %+v, want it untouched on a non-200", out)
		}
	})

	t.Run("a malformed answers body fails open", func(t *testing.T) {
		cs := newClassifyServer(t, "", 0)
		cs.rawBody = "<html>gateway garbage</html>"
		armVerdictRepair(t, cs, "")
		in := ReviewResult{NoVerdict: true, Comments: "folded"}
		if out := repairReviewVerdict(context.Background(), in); out != in {
			t.Errorf("result = %+v, want it untouched on an unparseable body", out)
		}
	})
}

// TestClassifyReviewVerdictHeadroom pins the budget discipline: the classify
// call rides the review activity's remaining StartToClose budget, so a round
// that brushed that ceiling skips the dial entirely (dialing anyway would
// fail the whole activity at StartToClose and destroy the round — the
// fail-closed the headroom check exists to prevent) while a round with room,
// or no deadline at all, dials exactly once.
func TestClassifyReviewVerdictHeadroom(t *testing.T) {
	cs := newClassifyServer(t, "CHANGES_REQUESTED", 0.5)
	armVerdictRepair(t, cs, "")

	t.Run("no deadline dials", func(t *testing.T) {
		verdict, conf, ok := classifyReviewVerdict(context.Background(), "changes requested")
		if !ok || verdict != "CHANGES_REQUESTED" || conf != 0.5 {
			t.Errorf("classify = (%q, %v, %v), want the answer through", verdict, conf, ok)
		}
		if cs.count() != 1 {
			t.Fatalf("endpoint dialed %d times, want 1", cs.count())
		}
	})

	t.Run("less than the call budget left skips the dial", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), repairTimeout-time.Second)
		defer cancel()
		if _, _, ok := classifyReviewVerdict(ctx, "changes requested"); ok {
			t.Error("classify ok = true, want the skip when the full call cannot fit the remaining budget")
		}
		if n := cs.count(); n != 1 {
			t.Errorf("endpoint dialed %d more times, want the dial skipped entirely", n-1)
		}
	})

	t.Run("an expired deadline skips the dial", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), -time.Second)
		defer cancel()
		if _, _, ok := classifyReviewVerdict(ctx, "changes requested"); ok {
			t.Error("classify ok = true, want the skip on an expired budget")
		}
		if n := cs.count(); n != 1 {
			t.Errorf("endpoint dialed %d more times, want the dial skipped entirely", n-1)
		}
	})

	t.Run("room for the full call dials", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*repairTimeout)
		defer cancel()
		if _, _, ok := classifyReviewVerdict(ctx, "changes requested"); !ok {
			t.Error("classify ok = false, want the dial when the budget fits")
		}
		if n := cs.count(); n != 2 {
			t.Errorf("endpoint dialed %d times total, want 2", n)
		}
	})
}
