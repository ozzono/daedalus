package shrink

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

// TestTaskMinifiesPhrases pins the substitution table's observable effect on
// plain prose: each entry collapses to its shorter form, the match's leading
// capital carries over, and the ambiguous family ("that is", "who is") is
// left alone — its contraction would read as nonsense.
func TestTaskMinifiesPhrases(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"in order to go", "to go"},
		{"utilize the parser", "use the parser"},
		{"do not skip it", "don't skip it"},
		{"It is important to note that X", "Note: X"},
		{"prior to the build", "before the build"},
		{"the majority of cases", "most cases"},
		{"It is fine", "It's fine"},
		{"it is fine", "it's fine"},
		{"that is the plan", "that is the plan"},
		{"who is on call", "who is on call"},
	} {
		if got := Task(c.in); got != c.want {
			t.Errorf("Task(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestTaskProtectsSpans pins the protected classes: inline code spans,
// path-like tokens, and flag-like tokens survive verbatim even when the
// substitution table's word-boundary match reaches into them ("cannot"
// inside "/var/cannot/x" or "--cannot-flag").
func TestTaskProtectsSpans(t *testing.T) {
	for _, in := range []string{
		"run `utilize it` now",
		"see /var/cannot/x now",
		"pass --cannot-flag twice",
	} {
		if got := Task(in); got != in {
			t.Errorf("Task(%q) = %q, want it untouched (protected span)", in, got)
		}
	}
}

// TestTaskNormalizesStructure pins pass 1's always-safe tier: "*"/"+"
// bullets become "-", "1)" ordinals become "1.", marker padding collapses
// to one space, trailing whitespace is stripped, blank-line runs collapse to
// a single blank line, and leading blank lines are dropped. Words are never
// touched: inner padding like "wide    list" survives.
func TestTaskNormalizesStructure(t *testing.T) {
	in := "\n\n- a\n* b\n+ c\n1) d\n2) e\n\n\n\nwide    list"
	want := "- a\n- b\n- c\n1. d\n2. e\n\nwide    list"
	if got := Task(in); got != want {
		t.Errorf("Task(%q) = %q, want %q", in, got, want)
	}
}

// TestTaskDropsBoilerplate pins pass 3's extraction: a restated-rationale
// line (keyword plus separator) is dropped whole, and the seams it leaves
// are tidied. The keep-whitelist vetoes the drop for a constraint word, a
// reply contract, or any protected content; a keyword without a separator
// is an ordinary sentence, not ceremony; and an input of boilerplate alone
// would shrink to nothing, so the empty-output veto forwards it unchanged.
func TestTaskDropsBoilerplate(t *testing.T) {
	in := "Fix the parser.\nBackground: it was written in 2019, and grew\nShip it."
	if got, want := Task(in), "Fix the parser.\nShip it."; got != want {
		t.Errorf("Task(%q) = %q, want %q", in, got, want)
	}

	for _, kept := range []string{
		"Background: the parser must run first, read this anyway",
		"Example: see `cmd/main.go`, run it",
		"For example: reply with one line",
		"Note that the parser is fast",
	} {
		in := "Fix the parser.\n" + kept + "\nShip it."
		if got := Task(in); !strings.Contains(got, kept) {
			t.Errorf("Task(%q) = %q, want the kept line %q to survive", in, got, kept)
		}
	}

	only := "Background: filler, more filler"
	if got := Task(only); got != only {
		t.Errorf("Task(%q) = %q, want it unchanged (the extraction would empty the input)", only, got)
	}
}

// TestTaskKeepsContractLines pins the contract discipline: a line whose
// wording is a reply/output contract is never rewritten by pass 2 (even
// when it contains table phrases) and never dropped by pass 3 (even when it
// is boilerplate-shaped) — and the diet keeps working around one, so a
// rewritten contract line cannot silently fail the whole input closed —
// and pass 1's trailing-whitespace strip on one is not a gate failure (the
// pinned copy is the trimmed text pass 1 actually produces).
func TestTaskKeepsContractLines(t *testing.T) {
	if got, in := Task("Reply with a summary. It is important to note that plans are final."),
		"Reply with a summary. It is important to note that plans are final."; got != in {
		t.Errorf("Task(%q) = %q, want the contract line un-rewritten", in, got)
	}

	in := "Output contract: reply with one raw JSON array. It is important to note that the plan is final.\ndo not stop here"
	if got, want := Task(in),
		"Output contract: reply with one raw JSON array. It is important to note that the plan is final.\ndon't stop here"; got != want {
		t.Errorf("Task(%q) = %q, want %q (contract line verbatim, prose around it still minified)", in, got, want)
	}

	in = "Background: reply with JSON only, nothing else"
	if got := Task(in); got != in {
		t.Errorf("Task(%q) = %q, want the contract line un-dropped despite its boilerplate shape", in, got)
	}

	in = "Output contract: reply with one raw JSON array.   \nkeep this line"
	want := "Output contract: reply with one raw JSON array.\nkeep this line"
	if got := Task(in); got != want {
		t.Errorf("Task(%q) = %q, want %q (contract line trimmed by pass 1, still verbatim)", in, got, want)
	}
}

// TestTaskGateFailsClosed pins the invariants gate: a pass whose result is
// not strictly fewer o200k_base tokens forwards the input unchanged — here
// the only table hit ("cannot" → "can't") counts the same after as before —
// as does an input that is empty or all whitespace.
func TestTaskGateFailsClosed(t *testing.T) {
	for _, in := range []string{"", "  \n\t ", "cannot x"} {
		if got := Task(in); got != in {
			t.Errorf("Task(%q) = %q, want the input unchanged (the gate claims nothing)", in, got)
		}
	}
}

// TestTaskIdempotent pins the diet's determinism contract: the output
// re-shrinks to itself, so re-gating an already-shrunken prompt (a
// continued run) cannot double-shrink it.
func TestTaskIdempotent(t *testing.T) {
	in := "In order to utilize the parser, do not skip the acceptance criteria.\n\n\n- item one  \n* item two\n\nBackground: this is why we need it, honestly\nOutput contract: reply with exactly one raw JSON array.\n```\ncannot  do_not  modify   me\n```\nIt is important to note that the majority of prior to cases are able to run."
	out := Task(in)
	if out == in {
		t.Fatalf("Task(%q) = the input, want a shrunk text for this input", in)
	}
	if again := Task(out); again != out {
		t.Errorf("Task(Task(in)) = %q, want the first output unchanged", again)
	}
}

// TestTaskProtectsFencedBlocks pins the fence walk through minification:
// everything inside a fenced block survives verbatim — markers, padding,
// and words the table would rewrite — while prose after a true closer is
// minified again. "do not" is the probe word because its minification
// strictly shrinks the token count, so a leaked line actually ships
// minified instead of being masked by the fail-closed gate.
func TestTaskProtectsFencedBlocks(t *testing.T) {
	for _, c := range []struct {
		name       string
		in         string
		wantResume bool // a true closer precedes trailing prose
	}{
		{"backtick block", "````go\ndo not touch inside\n```\nstill inside: do not move\n````\nafter: do not stop\n", true},
		{"unclosed protects to the end", "```\ndo not touch this\nand neither this", false},
		{"closer with an info string is content", "```\ncode do not touch\n``` go\nprose after: do not stop\n", false},
		{"tab-indented closer is content", "```\ncode do not touch\n\t```\nprose after: do not stop\n", false},
		{"four-space-indented closer is content", "```\ncode do not touch\n    ```\nprose after: do not stop\n", false},
		{"tilde block ignores a backtick line", "~~~\ndo not touch\n```\nstill inside\n~~~\nafter do not stop", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			out := Task(c.in)
			if !strings.Contains(out, "do not") {
				t.Errorf("Task(%q) = %q, want the fenced words untouched", c.in, out)
			}
			if c.wantResume && !strings.Contains(out, "don't") {
				t.Errorf("Task(%q) = %q, want minification to resume after the closing fence", c.in, out)
			}
			if !c.wantResume && strings.Contains(out, "don't") {
				t.Errorf("Task(%q) = %q, want nothing inside the fence minified", c.in, out)
			}
		})
	}
}

// TestCount pins the strict counter: empty text counts zero, a real
// mergeable-rank count is not the chars/4 estimate ("hello world" is two
// tokens, not ceil(11/4)), a special-token string riding in the text counts
// as its ordinary pieces — never interpreted as the special itself (one
// token) — and the count is stable call to call.
func TestCount(t *testing.T) {
	for _, c := range []struct {
		in   string
		want int
	}{
		{"", 0},
		{"hello world", 2},
		{"<|endoftext|>", 7},
	} {
		got, err := Count(c.in)
		if err != nil {
			t.Fatalf("Count(%q): %v", c.in, err)
		}
		if got != c.want {
			t.Errorf("Count(%q) = %d, want %d", c.in, got, c.want)
		}
		if again, err := Count(c.in); err != nil || again != got {
			t.Errorf("Count(%q) again = (%d, %v), want the same count", c.in, again, err)
		}
	}
}

// TestEstimate pins the biased-high chars/4 estimator: ceiling division,
// empty text included.
func TestEstimate(t *testing.T) {
	for _, c := range []struct {
		in   string
		want int
	}{
		{"", 0},
		{"abc", 1},
		{"abcd", 1},
		{"abcde", 2},
	} {
		if got := Estimate(c.in); got != c.want {
			t.Errorf("Estimate(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

// TestEmbeddedRanks pins the strict counter's offline integrity: the
// embedded ranks file is byte-identical to the canonical o200k_base file
// (the sha256 OpenAI's own tiktoken pins), the loader refuses any other
// encoding's file instead of falling back to the network, and the real file
// parses into the full rank table.
func TestEmbeddedRanks(t *testing.T) {
	sum := sha256.Sum256([]byte(o200kBaseRanks))
	if got := hex.EncodeToString(sum[:]); got != "446a9538cb6c348e3516120d7c08b09f57c36495e2acfffe59a5bf8b0cfb1a2d" {
		t.Errorf("embedded o200k_base.tiktoken sha256 = %s, want the canonical hash pinned in count.go", got)
	}

	ranks, err := (embeddedLoader{}).LoadTiktokenBpe("/somewhere/cl100k_base.tiktoken")
	if err == nil || ranks != nil {
		t.Errorf("LoadTiktokenBpe(cl100k) = (%d ranks, %v), want a refusal — only o200k_base is embedded", len(ranks), err)
	}

	ranks, err = (embeddedLoader{}).LoadTiktokenBpe("/somewhere/o200k_base.tiktoken")
	if err != nil {
		t.Fatalf("LoadTiktokenBpe(o200k_base): %v", err)
	}
	if len(ranks) < 199000 {
		t.Errorf("LoadTiktokenBpe(o200k_base) parsed %d ranks, want the full ~200k table", len(ranks))
	}
}
