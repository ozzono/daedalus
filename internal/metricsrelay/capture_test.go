package metricsrelay

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"
)

// TestCapturesRing pins the capture store's discipline: a record files
// under its task/role/round key, a repeat Put replaces rather than appends
// without refreshing the eviction order (last write wins, oldest-insert
// evicts), the cap evicts oldest-first, and a key with no per-round
// attribution — an empty task or round — files nothing.
func TestCapturesRing(t *testing.T) {
	var c Captures
	if _, ok := c.Get("t1", "dev", "1"); ok {
		t.Errorf("zero-value Captures answered a Get with a record")
	}

	c.Put("t1", "dev", "1", CapturedRound{Prompt: "p1", Response: "r1"})
	got, ok := c.Get("t1", "dev", "1")
	if !ok || got.Prompt != "p1" || got.Response != "r1" {
		t.Errorf("Get = (%+v, %v), want the stored record", got, ok)
	}
	if _, ok := c.Get("t1", "test", "1"); ok {
		t.Errorf("a different role answered for t1's record")
	}

	// Replacement: the same key's second write wins, and it must not
	// refresh the eviction order — t0 is replaced below while the ring
	// holds t1 and t0, so the insert that crosses the cap evicts t1 and
	// the next one evicts t0 in its original position, never the record
	// written most recently.
	c.Put("t0", "dev", "0", CapturedRound{Prompt: "old"})
	c.Put("t0", "dev", "0", CapturedRound{Prompt: "new"})
	for i := 1; i <= captureCap-2; i++ {
		c.Put("t", "dev", strconv.Itoa(i), CapturedRound{})
	}
	c.Put("t", "dev", strconv.Itoa(captureCap-1), CapturedRound{})
	if _, ok := c.Get("t1", "dev", "1"); ok {
		t.Fatalf("the oldest record survived the insert that crossed the cap")
	}
	if got, ok := c.Get("t0", "dev", "0"); !ok || got.Prompt != "new" {
		t.Fatalf("replaced record = (%+v, %v), want the last write still resident", got, ok)
	}
	// The next insert evicts t0's slot — its original position, proving
	// the replacement did not buy it a fresh lease.
	c.Put("t", "dev", strconv.Itoa(captureCap), CapturedRound{})
	if _, ok := c.Get("t0", "dev", "0"); ok {
		t.Errorf("the replaced record survived past the cap; want oldest-insert eviction")
	}
	if _, ok := c.Get("t", "dev", "1"); !ok {
		t.Errorf("the second-oldest record was evicted early")
	}

	for _, key := range [][3]string{{"", "dev", "1"}, {"t1", "dev", ""}} {
		c.Put(key[0], key[1], key[2], CapturedRound{Prompt: "homeless"})
		if _, ok := c.Get(key[0], key[1], key[2]); ok {
			t.Errorf("Put with task=%q round=%q filed a record; an unattributable round has nothing to file under", key[0], key[2])
		}
	}
}

// TestServeCaptureEndpoint pins the -cot reader's endpoint: a stored round
// answers 200 with the CapturedRound's JSON wire shape (the CLI decodes
// those field names), an absent one answers 404 — the capture-absent case
// the reader degrades on, never an error shape.
func TestServeCaptureEndpoint(t *testing.T) {
	r, root := startRelay(t, &Ledger{}, NewRegistry(time.Minute))
	at := time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)
	r.captures.Put("wf-1", "dev", "3", CapturedRound{At: at, Prompt: "the prompt", Response: "the answer"})

	resp, err := http.Get(root + "/capture?task=wf-1&role=dev&round=3")
	if err != nil {
		t.Fatalf("GET /capture: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /capture = %d, want 200", resp.StatusCode)
	}
	var cr CapturedRound
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		t.Fatalf("decode capture: %v", err)
	}
	if !cr.At.Equal(at) || cr.Prompt != "the prompt" || cr.Response != "the answer" {
		t.Errorf("capture = %+v, want the stored record on the documented wire shape", cr)
	}

	resp2, err := http.Get(root + "/capture?task=wf-1&role=dev&round=9")
	if err != nil {
		t.Fatalf("GET absent /capture: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("GET absent /capture = %d, want 404 (the degrade-on case)", resp2.StatusCode)
	}
}

// TestCappedBuffer pins the tee's sink: it keeps its first cap bytes,
// discards the rest, always claims the full write (the tee must never fail
// a forward over capture bookkeeping), and says so inline — exactly one
// truncation marker, appended by String only when the cap bit set.
func TestCappedBuffer(t *testing.T) {
	t.Run("within the cap the bytes land whole", func(t *testing.T) {
		c := newCappedBuffer(8)
		if n, err := c.Write([]byte("hello")); n != 5 || err != nil {
			t.Fatalf("Write = (%d, %v), want (5, nil)", n, err)
		}
		if got := c.String(); got != "hello" {
			t.Errorf("String = %q, want the bytes verbatim", got)
		}
	})

	t.Run("overflow truncates and marks once", func(t *testing.T) {
		c := newCappedBuffer(8)
		if n, _ := c.Write([]byte("1234567890")); n != 10 {
			t.Fatalf("Write = %d, want the full length claimed", n)
		}
		c.Write([]byte("more"))
		want := "12345678" + "\n[daedalus: capture truncated at 8 bytes]"
		if got := c.String(); got != want {
			t.Errorf("String = %q, want the first 8 bytes with one marker", got)
		}
	})

	t.Run("a write entirely past the cap only marks", func(t *testing.T) {
		c := newCappedBuffer(4)
		c.Write([]byte("abcd"))
		c.Write([]byte("efghij"))
		want := "abcd" + "\n[daedalus: capture truncated at 4 bytes]"
		if got := c.String(); got != want {
			t.Errorf("String = %q, want the untouched head with one marker", got)
		}
	})

	t.Run("an empty write never sets the bit", func(t *testing.T) {
		c := newCappedBuffer(4)
		c.Write(nil)
		if got := c.String(); got != "" {
			t.Errorf("String = %q, want empty with no marker", got)
		}
	})
}

// TestAssistantText pins the response-side extraction: the assistant text —
// thinking plus answer — out of the four documented wire shapes, joined
// with blank lines in arrival order, and "" for anything that is not one
// of them (an error page carries no assistant text to find).
func TestAssistantText(t *testing.T) {
	t.Run("plain anthropic content blocks", func(t *testing.T) {
		got := assistantText([]byte(`{"content":[{"type":"text","text":"answer"},{"type":"thinking","thinking":"thought"}]}`))
		if got != "answer\n\nthought" {
			t.Errorf("assistantText = %q, want the blocks joined in order", got)
		}
	})

	t.Run("plain openai choice", func(t *testing.T) {
		got := assistantText([]byte(`{"choices":[{"message":{"content":"the answer"}}]}`))
		if got != "the answer" {
			t.Errorf("assistantText = %q, want the message content", got)
		}
	})

	t.Run("both shapes in one body concatenate", func(t *testing.T) {
		got := assistantText([]byte(`{"content":[{"type":"text","text":"one"}],"choices":[{"message":{"content":"two"}}]}`))
		if got != "one\n\ntwo" {
			t.Errorf("assistantText = %q, want both sides joined", got)
		}
	})

	t.Run("anthropic sse deltas", func(t *testing.T) {
		body := "event: content_block_delta\n" +
			`data: {"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"step "}}` + "\n\n" +
			`data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"final"}}` + "\n\n" +
			`data: {"type":"message_stop"}` + "\n\n"
		if got := assistantText([]byte(body)); got != "step \n\nfinal" {
			t.Errorf("assistantText = %q, want the deltas joined", got)
		}
	})

	t.Run("openai sse chunks", func(t *testing.T) {
		body := `data: {"choices":[{"delta":{"content":"hel"}}]}` + "\n\n" +
			`data: {"choices":[{"delta":{"content":"lo"}}]}` + "\n\n" +
			"data: [DONE]\n\n"
		if got := assistantText([]byte(body)); got != "hel\n\nlo" {
			t.Errorf("assistantText = %q, want the chunk contents joined past [DONE]", got)
		}
	})

	t.Run("unrecognized bodies extract to nothing", func(t *testing.T) {
		for _, body := range []string{
			`{"error":{"type":"overloaded","message":"try again"}}`,
			`<html>502 Bad Gateway</html>`,
			`{"content":[{"type":"text","text":""}]}`,
			"",
		} {
			if got := assistantText([]byte(body)); got != "" {
				t.Errorf("assistantText(%q) = %q, want empty", body, got)
			}
		}
	})
}

// TestProxyCaptureTeesKeyedRounds pins the relay-side capture: a keyed,
// inspected round's request body and its response's assistant text file
// under the round's attribution — the prompt side teed on the way upstream
// (the upstream still receives the client's whole body), the response side
// read off the same buffered body the usage ledger reads — while unkeyed
// rounds, non-inspected shapes, and refused tokens file nothing.
func TestProxyCaptureTeesKeyedRounds(t *testing.T) {
	prompt := []byte(`{"model":"glm-4","messages":[{"role":"user","content":"fix the bug"}]}`)

	t.Run("keyed openai round captures both sides", func(t *testing.T) {
		rec := &recorder{}
		up := stubUpstream(rec, http.StatusOK, []byte(`{"choices":[{"message":{"content":"done"}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`))
		defer up.Close()

		ledger := &Ledger{}
		registry := NewRegistry(time.Minute)
		r, root := startRelay(t, ledger, registry)
		registerRelay(t, root, "w", map[string]string{"openai": up.URL + "/v1"}, map[string]string{"openai": "sk-real"}, nil)

		h := http.Header{}
		h.Set("Authorization", "Bearer task-w1-role-dev-round-3-rk-"+testToken)
		stagedURL := staged(root, "w", WireOpenAI, up.URL+"/v1")
		if status, _ := postRelay(t, stagedURL+"/chat/completions", h, prompt); status != http.StatusOK {
			t.Fatalf("status = %d, want the forward through", status)
		}
		reqs := rec.all()
		if len(reqs) != 1 || !bytes.Equal(reqs[0].Body, prompt) {
			t.Fatalf("upstream body = %s, want the client's whole request teed through untouched", reqs[0].Body)
		}
		cr, ok := r.captures.Get("w1", "dev", "3")
		if !ok {
			t.Fatal("no capture filed for the keyed round")
		}
		if cr.Prompt != string(prompt) {
			t.Errorf("capture prompt = %q, want the request body verbatim", cr.Prompt)
		}
		if cr.Response != "done" {
			t.Errorf("capture response = %q, want the assistant text off the reply", cr.Response)
		}
		if cr.At.IsZero() {
			t.Errorf("capture stamped with the zero time")
		}
		if ledger.Len() != 1 {
			t.Errorf("ledger holds %d entries, want the usage row beside the capture", ledger.Len())
		}
	})

	t.Run("sse response extracts the streamed text", func(t *testing.T) {
		rec := &recorder{}
		sse := "data: {\"choices\":[{\"delta\":{\"content\":\"hel\"}}]}\n\n" +
			"data: {\"choices\":[{\"delta\":{\"content\":\"lo\"}}]}\n\ndata: [DONE]\n\n"
		up := stubUpstream(rec, http.StatusOK, []byte(sse))
		defer up.Close()

		r, root := startRelay(t, &Ledger{}, NewRegistry(time.Minute))
		registerRelay(t, root, "w", map[string]string{"openai": up.URL + "/v1"}, nil, nil)
		h := http.Header{}
		h.Set("Authorization", "Bearer task-w2-role-test-round-1-rk-"+testToken)
		stagedURL := staged(root, "w", WireOpenAI, up.URL+"/v1")
		if status, body := postRelay(t, stagedURL+"/chat/completions", h, prompt); status != http.StatusOK || !bytes.Equal(body, []byte(sse)) {
			t.Fatalf("relayed reply = (%d) %s, want the stream bytes verbatim", status, body)
		}
		if cr, ok := r.captures.Get("w2", "test", "1"); !ok || cr.Response != "hel\n\nlo" {
			t.Errorf("capture = (%+v, %v), want the deltas joined", cr, ok)
		}
	})

	t.Run("keyed anthropic round captures the anthropic shape", func(t *testing.T) {
		rec := &recorder{}
		up := stubUpstream(rec, http.StatusOK, []byte(`{"content":[{"type":"text","text":"what"},{"type":"thinking","thinking":"why"}],"usage":{"input_tokens":1}}`))
		defer up.Close()

		r, root := startRelay(t, &Ledger{}, NewRegistry(time.Minute))
		registerRelay(t, root, "w", map[string]string{"anthropic": up.URL}, map[string]string{"anthropic": "sk-real"}, nil)
		h := http.Header{}
		h.Set("X-Api-Key", "task-w3-role-review-round-2-rk-"+testToken)
		stagedURL := staged(root, "w", WireAnthropic, up.URL)
		if status, _ := postRelay(t, stagedURL+"/v1/messages", h, prompt); status != http.StatusOK {
			t.Fatalf("status = %d, want the forward through", status)
		}
		if cr, ok := r.captures.Get("w3", "review", "2"); !ok || cr.Response != "what\n\nwhy" {
			t.Errorf("capture = (%+v, %v), want thinking and text joined in arrival order", cr, ok)
		}
	})

	t.Run("unkeyed and non-inspected rounds file nothing", func(t *testing.T) {
		rec := &recorder{}
		up := stubUpstream(rec, http.StatusOK, openaiCompletionReply())
		defer up.Close()

		r, root := startRelay(t, &Ledger{}, NewRegistry(time.Minute))
		registerRelay(t, root, "w", map[string]string{"openai": up.URL + "/v1"}, nil, nil)
		stagedURL := staged(root, "w", WireOpenAI, up.URL+"/v1")

		h := http.Header{}
		h.Set("Authorization", "Bearer sk-plain-unkeyed")
		if status, _ := postRelay(t, stagedURL+"/chat/completions", h, prompt); status != http.StatusOK {
			t.Fatalf("unkeyed round status = %d, want the forward through", status)
		}
		if status, _ := postRelay(t, stagedURL+"/v1/embeddings", http.Header{}, prompt); status != http.StatusOK {
			t.Fatalf("non-inspected round status = %d, want the forward through", status)
		}
		if got := len(rec.all()); got != 2 {
			t.Fatalf("upstream saw %d requests, want both forwards", got)
		}
		// The unkeyed round parsed to no attribution at all, and the
		// embeddings POST is not an inspected shape: neither may file a
		// capture under any key.
		for _, key := range [][3]string{{"", "", ""}, {"w", "", ""}} {
			if _, ok := r.captures.Get(key[0], key[1], key[2]); ok {
				t.Errorf("a capture leaked under the (%q %q %q) key", key[0], key[1], key[2])
			}
		}
	})

	t.Run("a refused token files nothing", func(t *testing.T) {
		rec := &recorder{}
		up := stubUpstream(rec, http.StatusOK, openaiCompletionReply())
		defer up.Close()

		r, root := startRelay(t, &Ledger{}, NewRegistry(time.Minute))
		registerRelay(t, root, "w", map[string]string{"openai": up.URL + "/v1"}, nil, nil)
		h := http.Header{}
		h.Set("Authorization", "Bearer task-w4-role-dev-round-1-rk-wrongtoken")
		if status, _ := postRelay(t, staged(root, "w", WireOpenAI, up.URL+"/v1")+"/chat/completions", h, prompt); status != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", status)
		}
		if _, ok := r.captures.Get("w4", "dev", "1"); ok {
			t.Errorf("the refused round filed a capture")
		}
		if got := len(rec.all()); got != 0 {
			t.Errorf("upstream saw %d requests, want 0", got)
		}
	})

	t.Run("a repeated round replaces its capture", func(t *testing.T) {
		rec := &recorder{}
		up := stubUpstream(rec, http.StatusOK, []byte(`{"choices":[{"message":{"content":"second"}}]}`))
		defer up.Close()

		r, root := startRelay(t, &Ledger{}, NewRegistry(time.Minute))
		registerRelay(t, root, "w", map[string]string{"openai": up.URL + "/v1"}, nil, nil)
		h := http.Header{}
		h.Set("Authorization", "Bearer task-w5-role-dev-round-9-rk-"+testToken)
		stagedURL := staged(root, "w", WireOpenAI, up.URL+"/v1")
		for i, body := range [][]byte{[]byte(`{"first":true}`), prompt} {
			if status, _ := postRelay(t, stagedURL+"/chat/completions", h, body); status != http.StatusOK {
				t.Fatalf("call %d status = %d, want the forward through", i+1, status)
			}
		}
		cr, ok := r.captures.Get("w5", "dev", "9")
		if !ok || cr.Prompt != string(prompt) || cr.Response != "second" {
			t.Errorf("capture = (%+v, %v), want the last call's record whole", cr, ok)
		}
	})
}
