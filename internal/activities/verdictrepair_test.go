package activities

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ozzono/daedalus/internal/config"
)

// classifyServer stubs the decision endpoint: every request is recorded and
// answered with the given content as the single choice's message. The
// requests slice and a mutex keep the handlers race-safe under the
// -race-defaulted suite.
type classifyServer struct {
	srv      *httptest.Server
	mu       sync.Mutex
	requests []classifyRequest
	content  string
	status   int
	rawBody  string // when non-empty, sent verbatim instead of the wrapper
}

type classifyRequest struct {
	path        string
	model       string
	auth        string
	contentType string
	// content is the first message's payload — the classify prompt the
	// marker-less review must ride in on.
	content string
}

func newClassifyServer(t *testing.T, content string) *classifyServer {
	t.Helper()
	cs := &classifyServer{content: content, status: http.StatusOK}
	cs.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Model    string `json:"model"`
			Messages []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"messages"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		prompt := ""
		if len(body.Messages) > 0 {
			prompt = body.Messages[0].Content
		}
		cs.mu.Lock()
		cs.requests = append(cs.requests, classifyRequest{
			path:        r.URL.Path,
			model:       body.Model,
			auth:        r.Header.Get("Authorization"),
			contentType: r.Header.Get("Content-Type"),
			content:     prompt,
		})
		cs.mu.Unlock()
		if cs.status != http.StatusOK {
			http.Error(w, "boom", cs.status)
			return
		}
		payload := cs.rawBody
		if payload == "" {
			b, _ := json.Marshal(map[string]any{
				"choices": []map[string]any{
					{"message": map[string]string{"role": "assistant", "content": cs.content}},
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
	t.Setenv(config.VerdictRepairModelEnv, "qwen2.5-coder:3b")
	if key != "" {
		t.Setenv(config.VerdictRepairKeyEnv, key)
	}
}

// TestRepairReviewVerdictPromotes pins the repair's one routing decision at
// the promotion boundary: an APPROVED classification at repairPromotionThreshold
// promotes the marker-less round to approval — the promoted round ends the
// review loop exactly as a parsed approval would (the workflow decides
// approval before its strike accounting) — while one below it, and every
// other verdict at any confidence, only annotates the round (LikelyVerdict
// and LikelyConfidence) and keeps it a changes-requested round: NEEDS_MAINTAINER
// and REBUILD inferences never trigger their lifecycle actions, and the
// repair outcome never touches strike accounting (only the workflow counts
// strikes). The 0.9/0.899 pair is the threshold's mutation pin both
// directions: moving the constant in either direction flips one case.
func TestRepairReviewVerdictPromotes(t *testing.T) {
	cases := []struct {
		label        string
		content      string
		key          string
		wantApproved bool
		wantVerdict  string
		wantConf     float64
	}{
		{"APPROVED at the threshold promotes", `{"verdict": "APPROVED", "confidence": 0.9}`, "", true, "APPROVED", 0.9},
		{"APPROVED below the threshold only annotates", `{"verdict": "APPROVED", "confidence": 0.899}`, "", false, "APPROVED", 0.899},
		{"APPROVED above the threshold promotes", `{"verdict": "APPROVED", "confidence": 1}`, "", true, "APPROVED", 1},
		{"confidence zero is in range and never promotes", `{"verdict": "APPROVED", "confidence": 0}`, "", false, "APPROVED", 0},
		{"changes-requested annotates without routing", `{"verdict": "CHANGES_REQUESTED", "confidence": 0.99}`, "", false, "CHANGES_REQUESTED", 0.99},
		{"needs-maintainer annotates without parking", `{"verdict": "NEEDS_MAINTAINER", "confidence": 0.99}`, "", false, "NEEDS_MAINTAINER", 0.99},
		{"rebuild annotates without routing", `{"verdict": "REBUILD", "confidence": 0.7}`, "", false, "REBUILD", 0.7},
		{"the answer is normalized, not exact-matched", `{"verdict": " approved ", "confidence": 0.95}`, "", true, "APPROVED", 0.95},
		{"fenced JSON answers fine", "```json\n{\"verdict\": \"APPROVED\", \"confidence\": 0.95}\n```", "", true, "APPROVED", 0.95},
		{"an armed key rides the request", `{"verdict": "APPROVED", "confidence": 0.95}`, "sk-local", true, "APPROVED", 0.95},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			cs := newClassifyServer(t, tc.content)
			armVerdictRepair(t, cs, tc.key)
			in := ReviewResult{NoVerdict: true, Comments: "looks good, but the marker line never arrived"}

			out := repairReviewVerdict(context.Background(), in)

			if cs.count() != 1 {
				t.Fatalf("decision endpoint dialed %d times, want exactly one classify call", cs.count())
			}
			req := cs.last()
			if req.path != "/chat/completions" {
				t.Errorf("request path = %q, want /chat/completions (the configured url is a chat-completions base)", req.path)
			}
			if req.model != "qwen2.5-coder:3b" {
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
			// The classify prompt must carry the round's marker-less output
			// — a request that loses the review text classifies nothing —
			// and name the choice set the answer must pick from.
			if !strings.Contains(req.content, "looks good, but the marker line never arrived") {
				t.Errorf("request content = %q, want the marker-less review text riding the prompt", req.content)
			}
			for _, marker := range verdictMarkers {
				if !strings.Contains(req.content, marker) {
					t.Errorf("request content = %q, want it to name the marker %q", req.content, marker)
				}
			}
			if out.LikelyVerdict != tc.wantVerdict || out.LikelyConfidence != tc.wantConf {
				t.Errorf("annotation = %q/%v, want %q/%v", out.LikelyVerdict, out.LikelyConfidence, tc.wantVerdict, tc.wantConf)
			}
			if out.Approved != tc.wantApproved {
				t.Errorf("Approved = %v, want %v (confidence %v vs the 0.9 promotion threshold)", out.Approved, tc.wantApproved, tc.wantConf)
			}
		})
	}
}

// TestRepairReviewVerdictFailOpen pins the layer's contract on every
// failure: an unconfigured, budget-starved, unreachable, erroring,
// garbage-answering endpoint returns the result untouched — the
// pre-repair behavior byte for byte (changes-requested, no annotation),
// so the marker-less strike budget stays the layer's only retry policy.
func TestRepairReviewVerdictFailOpen(t *testing.T) {
	garbage := []struct {
		label   string
		content string
	}{
		{"prose instead of JSON", "The review looks fine to me, approved."},
		{"prose around the object", `Sure! {"verdict": "APPROVED", "confidence": 0.99}`},
		{"two fenced blocks", "```json\n{\"verdict\": \"APPROVED\"}\n```\n```json\n{\"confidence\": 1}\n```"},
		{"unknown verdict", `{"verdict": "MAYBE", "confidence": 0.99}`},
		{"a different confidence scale", `{"verdict": "APPROVED", "confidence": 90}`},
		{"a negative confidence", `{"verdict": "APPROVED", "confidence": -0.1}`},
		{"an empty answer", ""},
	}
	for _, tc := range garbage {
		t.Run(tc.label, func(t *testing.T) {
			cs := newClassifyServer(t, tc.content)
			armVerdictRepair(t, cs, "")
			in := ReviewResult{NoVerdict: true, Comments: "connection reset by peer"}

			out := repairReviewVerdict(context.Background(), in)

			if out != in {
				t.Errorf("result = %+v, want it untouched by the failed repair", out)
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
		t.Setenv(config.VerdictRepairModelEnv, "qwen2.5-coder:3b")
		in := ReviewResult{NoVerdict: true, Comments: "folded"}
		if out := repairReviewVerdict(context.Background(), in); out != in {
			t.Errorf("result = %+v, want it untouched when the endpoint is unreachable", out)
		}
	})

	t.Run("an HTTP error status fails open", func(t *testing.T) {
		cs := newClassifyServer(t, `{"verdict": "APPROVED", "confidence": 1}`)
		cs.status = http.StatusInternalServerError
		armVerdictRepair(t, cs, "")
		in := ReviewResult{NoVerdict: true, Comments: "folded"}
		if out := repairReviewVerdict(context.Background(), in); out != in {
			t.Errorf("result = %+v, want it untouched on a non-200", out)
		}
	})

	t.Run("a malformed completion body fails open", func(t *testing.T) {
		cs := newClassifyServer(t, "")
		cs.rawBody = "<html>gateway garbage</html>"
		armVerdictRepair(t, cs, "")
		in := ReviewResult{NoVerdict: true, Comments: "folded"}
		if out := repairReviewVerdict(context.Background(), in); out != in {
			t.Errorf("result = %+v, want it untouched on an unparseable body", out)
		}
	})

	t.Run("a completion with no choices fails open", func(t *testing.T) {
		cs := newClassifyServer(t, "")
		cs.rawBody = `{"choices": []}`
		armVerdictRepair(t, cs, "")
		in := ReviewResult{NoVerdict: true, Comments: "folded"}
		if out := repairReviewVerdict(context.Background(), in); out != in {
			t.Errorf("result = %+v, want it untouched with no choices", out)
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
	cs := newClassifyServer(t, `{"verdict": "CHANGES_REQUESTED", "confidence": 0.5}`)
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

// TestStripJSONFence pins the fence tolerance at the shapes small local
// models actually emit — and the ones it must not pretend to fix: the strip
// only succeeds when the content becomes a single JSON object, and any
// shape it cannot make single fails the downstream unmarshal (the caller's
// fail-open).
func TestStripJSONFence(t *testing.T) {
	cases := []struct {
		label string
		in    string
		want  string
	}{
		{"bare JSON passes through trimmed", "  {\"a\": 1}  ", `{"a": 1}`},
		{"a fence with an info string", "```json\n{\"a\": 1}\n```", `{"a": 1}`},
		{"a bare fence", "```\n{\"a\": 1}\n```", `{"a": 1}`},
		{"an unclosed fence keeps the body", "```json\n{\"a\": 1}", `{"a": 1}`},
		{"trailing text after the closer is cut", "```json\n{\"a\": 1}\n```\nhope this helps", `{"a": 1}`},
		{"prose around a block is not repaired", "Here you go:\n```json\n{\"a\": 1}\n```", "Here you go:\n```json\n{\"a\": 1}\n```"},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			if got := stripJSONFence(tc.in); got != tc.want {
				t.Errorf("stripJSONFence(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
