package main

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ozzono/daedalus/internal/metricsrelay"
)

// previewEntries lays the window every preview test folds: one fully
// priced call (mA: 1M in, 2M out, 4M cache reads, 0.5M cache writes), one
// call on a model with no saved rates (mB: 3M in, 1M out), and one
// model-less failure that still spent 100k input tokens.
func previewEntries(now time.Time) []metricsrelay.Entry {
	return []metricsrelay.Entry{
		{At: now.Add(-2 * time.Minute), Worker: "w-a", Wire: metricsrelay.WireAnthropic, Model: "mA",
			InputTokens: 1_000_000, OutputTokens: 2_000_000, CacheReadTokens: 4_000_000,
			CacheWriteTokens: 500_000, Status: 200},
		{At: now.Add(-time.Minute), Worker: "w-b", Wire: metricsrelay.WireOpenAI, Model: "mB",
			InputTokens: 3_000_000, OutputTokens: 1_000_000, Status: 200},
		{At: now.Add(-30 * time.Second), Worker: "w-a", InputTokens: 100_000, Status: 401},
	}
}

// TestBuildPricePreviewFoldsTheWindow pins the modal's baseline fold: the
// token totals count every relayed call (the model-less failures too),
// the current total prices only entries whose model has saved rates and
// stays the dash when nothing in the window prices, and an empty window
// is the no-calls face with no what-if block.
func TestBuildPricePreviewFoldsTheWindow(t *testing.T) {
	now := time.Now()

	v := buildPricePreview(now, nil, map[string]modelPrice{"mA": {In: 3}}, "")
	if v.Window != "no relayed calls yet" || v.Calls != 0 || v.Current != "—" || v.WhatIf != nil {
		t.Errorf("empty preview = %+v, want the no-calls face with no what-if", v)
	}

	v = buildPricePreview(now, previewEntries(now), map[string]modelPrice{"mA": {In: 3, Out: 2, Cached: 1}}, "")
	if v.Window != "2m0s window" {
		t.Errorf("window = %q, want the ledger's own span", v.Window)
	}
	if v.Calls != 3 || v.In != 8_600_000 || v.Out != 3_000_000 ||
		v.CacheRead != 4_000_000 || v.CacheWrite != 500_000 {
		t.Errorf("totals = %+v, want every call folded (the input whole: 5.5M anthropic, 3M openai, 0.1M), the model-less failure included", v)
	}
	// mA only: (1M+0.5M)*$3 + 2M*$2 + 4M*$1 = $12.50 — mB and the
	// model-less failure price nowhere.
	if v.Current != "$12.5000" {
		t.Errorf("current = %q, want $12.5000", v.Current)
	}
	if v.WhatIf != nil {
		t.Errorf("an unnamed preview grew a what-if block: %+v", v.WhatIf)
	}

	// Rates on a model the window never called price nothing: the current
	// total stays the dash even though the what-if itself is well-defined.
	v = buildPricePreview(now, previewEntries(now), map[string]modelPrice{"mZ": {In: 1}}, "mZ")
	if v.Current != "—" || v.WhatIf == nil || v.WhatIf.Total != "$4.6000" || v.WhatIf.Delta != "—" {
		t.Errorf("unpriced window = %+v / %+v, want a dash current and delta beside a real total", v, v.WhatIf)
	}
}

// TestBuildPricePreviewWhatIf pins the named model's block: its saved
// rates render back in exact decimal form, the whole window reprices at
// them (calls the current total never priced included), the delta carries
// its sign against the current total, and a name with no saved rates
// renders the dash face with its note — never zeros dressed as a reprice.
func TestBuildPricePreviewWhatIf(t *testing.T) {
	now := time.Now()
	entries := previewEntries(now)
	prices := map[string]modelPrice{"mA": {In: 3, Out: 2, Cached: 1}, "mB": {In: 1, Out: 1, Cached: 1}}

	// mB's rates are cheaper across the window: the $16.50 current (12.5
	// from mA's calls, 4.0 from mB's own — both saved) reprices to $11.60,
	// so the delta reads the saving.
	w := buildPricePreview(now, entries, prices, "mB").WhatIf
	if w == nil || w.Name != "mB" || w.In != "1" || w.Out != "1" || w.Cached != "1" ||
		w.Total != "$11.6000" || w.Delta != "-$4.9000" || w.Note != "" {
		t.Errorf("what-if mB = %+v, want rates 1/1/1, total $11.6000, delta -$4.9000", w)
	}

	// The already-saved model's own rates are pricier against the same
	// $16.50 current: the whole window bills at them — 12.5 + 11 + 0.3.
	w = buildPricePreview(now, entries, prices, "mA").WhatIf
	if w == nil || w.Total != "$23.8000" || w.Delta != "$7.3000" {
		t.Errorf("what-if mA = %+v, want total $23.8000, delta $7.3000", w)
	}

	// A name the table never saved.
	w = buildPricePreview(now, entries, prices, "ghost").WhatIf
	if w == nil || w.In != "—" || w.Out != "—" || w.Cached != "—" ||
		w.Total != "—" || w.Delta != "—" || w.Note != "no saved rates for ghost" {
		t.Errorf("what-if ghost = %+v, want the dash face and the note", w)
	}
}

// TestUIModelsPreviewModal pins the what-if modal end to end: the Models
// page carries the dialog shell and its fetch button while the polled body
// fragment never does (a poll may not open or clobber the modal), every
// fetch is served — the fragment is fetched on demand, never polled, so no
// unchanged-body 204 may swallow a reopen — the named what-if rides the
// saved rates through the same snapshot the form's save writes, whitespace
// around the submitted name is trimmed, and nothing a preview fetch does
// is ever stored.
func TestUIModelsPreviewModal(t *testing.T) {
	t.Run("the modal rides the page, never the poll", func(t *testing.T) {
		h := newUIHarness(t)
		seedLedger(h, previewEntries(time.Now()))

		page := h.get(t, "/models")
		for _, want := range []string{
			`hx-get="/models/preview"`, `id="preview-dlg"`, `id="preview-body"`,
		} {
			if !strings.Contains(page, want) {
				t.Errorf("the models page lacks the modal piece %q:\n%s", want, page)
			}
		}
		body := h.get(t, "/models/body")
		for _, banned := range []string{`preview-dlg`, `preview-body`, `/models/preview`} {
			if strings.Contains(body, banned) {
				t.Errorf("the polled body carries modal plumbing %q:\n%s", banned, body)
			}
		}
	})

	t.Run("every fetch is served", func(t *testing.T) {
		h := newUIHarness(t)

		out := h.get(t, "/models/preview")
		if !strings.Contains(out, "no relayed calls yet") || !strings.Contains(out, "Preview only") {
			t.Errorf("the empty preview = %s, want the no-calls window and the nothing-stored note", out)
		}
		if strings.Contains(out, "What-if:") {
			t.Errorf("an unnamed preview rendered a what-if block:\n%s", out)
		}
		// Identical bytes must still answer 200 with a body: a 204 would
		// leave a reopened modal staring at its Loading placeholder.
		c1, b1 := h.poll(t, "/models/preview")
		c2, b2 := h.poll(t, "/models/preview")
		if c1 != http.StatusOK || c2 != http.StatusOK || b1 == "" || b1 != b2 {
			t.Errorf("repeat fetches = %d/%d bytes %d/%d, want 200 both times with the body served",
				c1, c2, len(b1), len(b2))
		}
	})

	t.Run("the named what-if rides the saved rates", func(t *testing.T) {
		h := newUIHarness(t)
		seedLedger(h, previewEntries(time.Now()))
		if code, loc, _ := postModels(t, h, url.Values{"in[mA]": {"3"}, "out[mA]": {"2"}, "cached[mA]": {"1"}}); code != http.StatusSeeOther {
			t.Fatalf("the seeded save answered %d to %q", code, loc)
		}

		// Padded to pin the trim — the form's input is the raw source.
		out := h.get(t, "/models/preview?model=%20mA%20")
		for _, want := range []string{
			`value="mA"`,
			"$12.5000", // the current total at the saved snapshot
			"$23.8000", // the whole window repriced at mA's rates
			"$11.3000", // the signed delta
			">8.60M<",  // the token tiers ride the tables' humanizer
			">500.0k<",
		} {
			if !strings.Contains(out, want) {
				t.Errorf("the what-if fragment lacks %q:\n%s", want, out)
			}
		}
		// The preview is read-only: the saved table in the db is exactly
		// what a later read sees.
		if p := h.prices.load(); len(p) != 1 || p["mA"] != (modelPrice{In: 3, Out: 2, Cached: 1}) {
			t.Errorf("the preview left the price table %+v, want the saved mA rates untouched", p)
		}
	})

	t.Run("a name with no saved rates renders the note", func(t *testing.T) {
		h := newUIHarness(t)
		seedLedger(h, previewEntries(time.Now()))

		// "unknown" is the failed-calls bucket the save gate can never
		// price — the modal's free-text input must not price it either.
		for _, name := range []string{"ghost", "unknown"} {
			out := h.get(t, "/models/preview?model="+url.QueryEscape(name))
			if !strings.Contains(out, "no saved rates for "+name) {
				t.Errorf("the %q preview lacks its note:\n%s", name, out)
			}
			if !strings.Contains(out, `What-if total</td><td class="mono text-right">—</td>`) {
				t.Errorf("the %q preview priced the unsavable name:\n%s", name, out)
			}
		}
	})
}
