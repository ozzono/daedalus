package activities

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/ozzono/daedalus/internal/config"
)

// clearSalvageEnv unsets the parsing seat's whole env channel — the parser
// model var and the openai upstream/key exports — restoring each value on
// cleanup. The suite's TestMain scrub covers the model var (a
// WorkerEnvVars member) but not the openai exports, which ride the
// ambient passthrough on serving hosts.
func clearSalvageEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		config.SlimParserModelEnv,
		"OPENAI_API_BASE",
		"OPENAI_BASE_URL",
		"OPENAI_API_KEY",
	} {
		old, had := os.LookupEnv(name)
		os.Unsetenv(name)
		t.Cleanup(func() {
			if had {
				os.Setenv(name, old)
			} else {
				os.Unsetenv(name)
			}
		})
	}
}

// salvageServer stubs the openai-channel chat-completions endpoint: every
// request is recorded and answered with the configured body and status.
// The request is decoded into a locally-tagged shape — not the production
// wire structs, which this package does not own — so a tag drift in the
// implementation fails these pins.
type salvageServer struct {
	srv    *httptest.Server
	mu     sync.Mutex
	calls  []salvageRequest
	status int
	raw    string // when non-empty, sent verbatim instead of the completion
}

type salvageRequest struct {
	method string
	path   string
	model  string
	prompt string
	stream *bool
	auth   string
}

func newSalvageServer(t *testing.T, reply string) *salvageServer {
	t.Helper()
	s := &salvageServer{status: http.StatusOK}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Model    string `json:"model"`
			Messages []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"messages"`
			Stream *bool `json:"stream"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		s.mu.Lock()
		s.calls = append(s.calls, salvageRequest{
			method: r.Method,
			path:   r.URL.Path,
			model:  body.Model,
			prompt: strings.Join(func() []string {
				var parts []string
				for _, m := range body.Messages {
					parts = append(parts, m.Role+": "+m.Content)
				}
				return parts
			}(), "\n"),
			stream: body.Stream,
			auth:   r.Header.Get("Authorization"),
		})
		s.mu.Unlock()
		if s.status != http.StatusOK {
			http.Error(w, "boom", s.status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(s.raw))
	}))
	t.Cleanup(s.srv.Close)
	s.raw = reply
	return s
}

func (s *salvageServer) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func (s *salvageServer) last() salvageRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[len(s.calls)-1]
}

// completionBody wraps one reply into the chat-completions envelope the
// activity decodes.
func completionBody(content string) string {
	b, _ := json.Marshal(map[string]any{
		"choices": []map[string]any{
			{"message": map[string]any{"role": "assistant", "content": content}},
		},
	})
	return string(b)
}

// TestSlimParseSalvageActivity pins the parsing seat's dial and its
// fail-open contract. The dial: one non-streaming chat completion at the
// openai channel's upstream (OPENAI_API_BASE, OPENAI_BASE_URL as the
// fallback) with the parser model (DAEDALUS_SLIM_PARSER_MODEL), the
// workflow-built transcription prompt as the single user message, and the
// ambient key as the bearer when one is exported. The contract: every
// unusable answer — an unconfigured seat, an unreachable endpoint, a
// non-200, a non-completion body, an empty reply — returns OK=false with a
// nil error, so the caller parks exactly as the unarmed path would have
// and the parse AI may only recover a doomed round, never wedge the run.
func TestSlimParseSalvageActivity(t *testing.T) {
	const prompt = "transcribe the plan into the raw JSON array"

	t.Run("the dial and the reply", func(t *testing.T) {
		clearSalvageEnv(t)
		srv := newSalvageServer(t, completionBody("[{\"id\":1}]"))
		t.Setenv(config.SlimParserModelEnv, "qwen2.5-coder:3b-parser")
		t.Setenv("OPENAI_API_BASE", srv.srv.URL+"/v1")
		t.Setenv("OPENAI_API_KEY", "sk-ambient")

		out, err := SlimParseSalvageActivity(context.Background(), SlimParseSalvageInput{Prompt: prompt})
		if err != nil {
			t.Fatalf("SlimParseSalvageActivity: %v", err)
		}
		if !out.OK || out.Text != "[{\"id\":1}]" {
			t.Errorf("output = %+v, want OK with the seat's raw reply", out)
		}
		if srv.count() != 1 {
			t.Fatalf("endpoint hit %d times, want exactly one call", srv.count())
		}
		req := srv.last()
		if req.method != http.MethodPost || req.path != "/v1/chat/completions" {
			t.Errorf("request = %s %s, want POST /v1/chat/completions", req.method, req.path)
		}
		if req.model != "qwen2.5-coder:3b-parser" {
			t.Errorf("request model = %q, want the parser model env", req.model)
		}
		if req.prompt != "user: "+prompt {
			t.Errorf("request messages = %q, want the transcription prompt as the single user message", req.prompt)
		}
		if req.stream == nil || *req.stream {
			t.Errorf("request stream = %v, want the explicit non-streaming false", req.stream)
		}
		if req.auth != "Bearer sk-ambient" {
			t.Errorf("request Authorization = %q, want the ambient key as the bearer", req.auth)
		}
	})

	t.Run("a trailing slash on the base is trimmed", func(t *testing.T) {
		clearSalvageEnv(t)
		srv := newSalvageServer(t, completionBody("ok"))
		t.Setenv(config.SlimParserModelEnv, "parser")
		t.Setenv("OPENAI_API_BASE", srv.srv.URL+"/v1/")

		if out, err := SlimParseSalvageActivity(context.Background(), SlimParseSalvageInput{Prompt: prompt}); err != nil || !out.OK {
			t.Fatalf("output = %+v, err %v, want OK", out, err)
		}
		if path := srv.last().path; path != "/v1/chat/completions" {
			t.Errorf("request path = %q, want /v1/chat/completions without a doubled slash", path)
		}
	})

	t.Run("OPENAI_BASE_URL serves when the api base is absent", func(t *testing.T) {
		clearSalvageEnv(t)
		srv := newSalvageServer(t, completionBody("ok"))
		t.Setenv(config.SlimParserModelEnv, "parser")
		t.Setenv("OPENAI_BASE_URL", srv.srv.URL)

		if out, err := SlimParseSalvageActivity(context.Background(), SlimParseSalvageInput{Prompt: prompt}); err != nil || !out.OK {
			t.Fatalf("output = %+v, err %v, want OK", out, err)
		}
		if srv.count() != 1 {
			t.Fatalf("endpoint hit %d times, want the fallback base dialed", srv.count())
		}
	})

	t.Run("the api base wins when both spellings are set", func(t *testing.T) {
		clearSalvageEnv(t)
		srv := newSalvageServer(t, completionBody("ok"))
		dead := newSalvageServer(t, completionBody("ok"))
		t.Setenv(config.SlimParserModelEnv, "parser")
		t.Setenv("OPENAI_API_BASE", srv.srv.URL)
		t.Setenv("OPENAI_BASE_URL", dead.srv.URL)

		if out, err := SlimParseSalvageActivity(context.Background(), SlimParseSalvageInput{Prompt: prompt}); err != nil || !out.OK {
			t.Fatalf("output = %+v, err %v, want OK", out, err)
		}
		if srv.count() != 1 || dead.count() != 0 {
			t.Fatalf("hits %d/%d, want the api base dialed and the base url untouched", srv.count(), dead.count())
		}
	})

	t.Run("no ambient key sends no bearer", func(t *testing.T) {
		clearSalvageEnv(t)
		srv := newSalvageServer(t, completionBody("ok"))
		t.Setenv(config.SlimParserModelEnv, "parser")
		t.Setenv("OPENAI_API_BASE", srv.srv.URL)

		if out, err := SlimParseSalvageActivity(context.Background(), SlimParseSalvageInput{Prompt: prompt}); err != nil || !out.OK {
			t.Fatalf("output = %+v, err %v, want OK", out, err)
		}
		if auth := srv.last().auth; auth != "" {
			t.Errorf("request Authorization = %q, want none without an ambient key", auth)
		}
	})

	// The fail-open family: each case parks the round exactly as the
	// unarmed path would have — OK=false, nil error.
	for _, c := range []struct {
		name      string
		model     string
		base      bool // arm OPENAI_API_BASE with the server
		prompt    string
		status    int
		raw       string
		wantDials int
	}{
		{name: "an empty prompt never dials", prompt: ""},
		{name: "an unconfigured parser model never dials", base: true, prompt: prompt},
		{name: "no upstream configured never dials", model: "parser", prompt: prompt},
		{name: "a non-200 answers unrecovered", model: "parser", base: true, prompt: prompt, status: http.StatusInternalServerError, raw: completionBody("late"), wantDials: 1},
		{name: "a non-completion body answers unrecovered", model: "parser", base: true, prompt: prompt, raw: "<html>gateway</html>", wantDials: 1},
		{name: "an empty choices array answers unrecovered", model: "parser", base: true, prompt: prompt, raw: `{"choices":[]}`, wantDials: 1},
		{name: "an empty reply content answers unrecovered", model: "parser", base: true, prompt: prompt, raw: completionBody(""), wantDials: 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			clearSalvageEnv(t)
			srv := newSalvageServer(t, c.raw)
			if c.status != 0 {
				srv.status = c.status
			}
			if c.model != "" {
				t.Setenv(config.SlimParserModelEnv, c.model)
			}
			if c.base {
				t.Setenv("OPENAI_API_BASE", srv.srv.URL)
			}

			out, err := SlimParseSalvageActivity(context.Background(), SlimParseSalvageInput{Prompt: c.prompt})
			if err != nil {
				t.Fatalf("error = %v, want nil — the seat may only fail open", err)
			}
			if out.OK || out.Text != "" {
				t.Errorf("output = %+v, want the zero output", out)
			}
			if got := srv.count(); got != c.wantDials {
				t.Errorf("endpoint hit %d times, want %d", got, c.wantDials)
			}
		})
	}
}
