// Package shrink implements the slim flow's input diet: three pure,
// deterministic passes over a run's task input — structural collapse,
// dictionary-driven prose minification, extractive trimming — applied in a
// fixed 1 → 2 → 3 order (structure first so the later passes see clean
// lines; extraction last so it scores the minimized text) and gated by the
// caveman invariants: the compressed text is accepted only if it counts
// strictly fewer o200k_base tokens than the original AND every
// output-contract line survives byte-verbatim; otherwise the original is
// forwarded unchanged and nothing is claimed (fail closed, the same
// pass-through discipline as the caveman engine's own gate — the approach
// is adopted, no caveman code is).
//
// The diet is not a measurement side channel: when the gated text is
// accepted, that text — never the original — is the task content the slim
// run embeds and renders into every round prompt.
package shrink

import (
	"regexp"
	"sort"
	"strings"
)

// Task shrinks a slim run's task input through the three passes and
// returns the result of the invariants gate: the compressed text when it
// is strictly smaller in o200k_base tokens and lost no output-contract
// line, else the input untouched. Pure and deterministic — the same input
// always yields the same output, and the output re-shrinks to itself.
func Task(input string) string {
	if strings.TrimSpace(input) == "" {
		return input
	}
	base := collapseStructural(input)
	// Contract lines are pinned against the structurally collapsed text —
	// pass 1's whitespace normalization is not a constraint change (the
	// prompt render trims around the embedded task anyway), and this is the
	// text passes 2 and 3 actually consume.
	contracts := contractLines(base)
	out := trimExtract(minifyProse(base))
	before, err := Count(input)
	if err != nil {
		return input // no trustworthy count → claim nothing
	}
	after, err := Count(out)
	if err != nil || after >= before ||
		strings.TrimSpace(out) == "" ||
		!contractsSurvive(contracts, out) {
		return input
	}
	return out
}

// --- the fence walk ---------------------------------------------------------

// fenceTracker walks a document's lines in order and reports which belong
// to fenced code blocks. A fence opens on a ≥3-backtick/tilde run (info
// string allowed) and closes only on a line that is nothing but a run of
// the same character at least as long as the opener's, indented at most
// three spaces and never a tab — the CommonMark closing-fence rule. The
// run-length rule is what keeps a ``` line nested inside a ```` block
// from false-closing it and handing the block's remainder to the passes
// as prose; requiring a bare closer (no info string) keeps a "``` see
// below" content line from false-closing in the other direction; the
// indent rule keeps a ≥4-space-indented run — literal content under
// CommonMark — from doing the same. Openers keep the lenient read:
// over-accepting an opener only protects more. An unclosed fence protects
// to the end of the input — under-protection is the one direction this
// walk must never lean.
type fenceTracker struct {
	ch   byte // the open fence's character, 0 when outside a fence
	open int  // the open fence's run length
}

// inFence reports whether ln belongs to a fenced block — the opening
// fence line, the closing fence line, and every line between — and
// advances the state. Call once per line, in document order.
func (ft *fenceTracker) inFence(ln string) bool {
	t := strings.TrimLeft(ln, " \t")
	if ft.ch == 0 {
		ch, n := opensFence(t)
		if ch == 0 {
			return false
		}
		ft.ch, ft.open = ch, n
		return true
	}
	// A deeper-indented or tab-indented closer-shaped line is content:
	// a tab advances past column 3, so every tab-indented run is outside
	// CommonMark's 0–3-space closing-fence budget.
	if !closerIndentOk(ln) {
		return true
	}
	if ch, n := bareFenceRun(t); ch == ft.ch && n >= ft.open {
		ft.ch = 0
	}
	return true
}

// closerIndentOk reports whether ln's indentation is inside CommonMark's
// closing-fence budget: at most three spaces, never a tab.
func closerIndentOk(ln string) bool {
	i := 0
	for i < len(ln) && ln[i] == ' ' {
		i++
	}
	return i < 4 && (i >= len(ln) || ln[i] != '\t')
}

// opensFence returns t's fence character and run length when t opens a
// fenced code block: a run of at least three ` or ~ characters, anything
// after it ("```go").
func opensFence(t string) (byte, int) {
	if len(t) < 3 || (t[0] != '`' && t[0] != '~') {
		return 0, 0
	}
	n := 0
	for n < len(t) && t[n] == t[0] {
		n++
	}
	if n < 3 {
		return 0, 0
	}
	return t[0], n
}

// bareFenceRun returns t's fence character and run length when t is
// nothing but a fence run — the only shape CommonMark accepts as a
// closer.
func bareFenceRun(t string) (byte, int) {
	ch, n := opensFence(t)
	if ch == 0 || strings.TrimSpace(t[n:]) != "" {
		return 0, 0
	}
	return ch, n
}

// --- pass 1: structural collapse -----------------------------------------

var (
	listMarkerRe = regexp.MustCompile(`^(\s*)[-*+][ \t]+`)
	numMarkerRe  = regexp.MustCompile(`^(\s*)(\d+)[.)][ \t]+`)
)

// collapseStructural is the always-safe tier: per-line trailing-whitespace
// strip, list-marker normalization (* and + bullets, "1)" ordinals → the
// canonical "- "/"1." forms), and blank-line runs collapsed to the single
// blank line that already separates markdown sections. Fenced blocks are
// protected whole — markers, padding, and blank lines inside a fence are
// code, not structure. Outside fences it never touches the words, so it
// cannot soften a constraint. (**bold** emphasis and tabs are deliberately
// left alone: emphasis is the author's, and the o200k savings would be
// noise.)
func collapseStructural(s string) string {
	lines := strings.Split(s, "\n")
	out := make([]string, 0, len(lines))
	prevBlank := true // also drops leading blank lines
	var ft fenceTracker
	for _, ln := range lines {
		if ft.inFence(ln) {
			out = append(out, ln)
			prevBlank = false
			continue
		}
		ln = normListMarker(strings.TrimRight(ln, " \t\r"))
		if ln == "" {
			if prevBlank {
				continue
			}
			prevBlank = true
		} else {
			prevBlank = false
		}
		out = append(out, ln)
	}
	return strings.Join(out, "\n")
}

// normListMarker rewrites one line's leading bullet or ordinal marker to
// its canonical form, collapsing the marker's padding to a single space.
func normListMarker(ln string) string {
	if m := listMarkerRe.FindStringSubmatch(ln); m != nil {
		return m[1] + "- " + strings.TrimLeft(ln[len(m[0]):], " \t")
	}
	if m := numMarkerRe.FindStringSubmatch(ln); m != nil {
		return m[1] + m[2] + ". " + strings.TrimLeft(ln[len(m[0]):], " \t")
	}
	return ln
}

// --- pass 2: dictionary-driven prose minification ------------------------

// phraseEntries is the substitution table: direction-preserving merges a
// misread can never invert (the shortened form means exactly the long
// form), so a target model loses nothing but ceremony. The contraction
// half is the unambiguous family only — "that is"/"who is" are excluded
// because their discourse-marker sense ("that is,") contracts into
// nonsense. ponytail: free article/auxiliary drops ("the repository" →
// "repository") are absent — each is a coin-flip on a 14B model's
// comprehension, which no invariant gate can check, so the tier lands at
// the low end of its estimated range instead of risking the prompt.
var phraseEntries = []struct{ from, to string }{
	{"it is important to note that", "note:"},
	{"it should be noted that", "note:"},
	{"due to the fact that", "because"},
	{"in spite of the fact that", "although"},
	{"despite the fact that", "although"},
	{"for the reason that", "because"},
	{"on the grounds that", "because"},
	{"in the event that", "if"},
	{"for the purpose of", "for"},
	{"with the exception of", "except"},
	{"until such time as", "until"},
	{"has the ability to", "can"},
	{"have the ability to", "can"},
	{"in addition to", "besides"},
	{"is able to", "can"},
	{"are able to", "can"},
	{"at this point in time", "now"},
	{"at the present time", "now"},
	{"a large number of", "many"},
	{"a small number of", "a few"},
	{"in the near future", "soon"},
	{"the majority of", "most"},
	{"makes use of", "uses"},
	{"make use of", "use"},
	{"each and every", "every"},
	{"period of time", "period"},
	{"subsequent to", "after"},
	{"a number of", "several"},
	{"prior to", "before"},
	{"by means of", "by"},
	{"whether or not", "whether"},
	{"in order to", "to"},
	{"utilize", "use"},
	{"utilizes", "uses"},
	{"commence", "start"},
	{"do not", "don't"},
	{"does not", "doesn't"},
	{"did not", "didn't"},
	{"cannot", "can't"},
	{"can not", "can't"},
	{"will not", "won't"},
	{"would not", "wouldn't"},
	{"should not", "shouldn't"},
	{"could not", "couldn't"},
	{"is not", "isn't"},
	{"are not", "aren't"},
	{"was not", "wasn't"},
	{"were not", "weren't"},
	{"has not", "hasn't"},
	{"have not", "haven't"},
	{"had not", "hadn't"},
	{"it is", "it's"},
	{"there is", "there's"},
	{"you are", "you're"},
	{"we are", "we're"},
	{"they are", "they're"},
}

var phraseMap = func() map[string]string {
	m := make(map[string]string, len(phraseEntries))
	for _, e := range phraseEntries {
		m[e.from] = e.to
	}
	return m
}()

// phraseRe matches any table entry, longest alternatives first so "in
// addition to" never leaves "to" behind inside "besides to", whole-word on
// both ends, case-insensitively (the match's own case is carried over).
var phraseRe = func() *regexp.Regexp {
	sorted := make([]string, len(phraseEntries))
	for i, e := range phraseEntries {
		sorted[i] = e.from
	}
	sort.Slice(sorted, func(i, j int) bool { return len(sorted[i]) > len(sorted[j]) })
	parts := make([]string, len(sorted))
	for i, from := range sorted {
		parts[i] = `\b(?:` + regexp.QuoteMeta(from) + `)\b`
	}
	return regexp.MustCompile(`(?i)` + strings.Join(parts, "|"))
}()

// tokenRe splits an unprotected stretch into whitespace-delimited tokens
// so the protected classes can be located by token.
var tokenRe = regexp.MustCompile(`[^\s]+`)

// pathLikeRe matches path- and URL-like tokens: anything carrying a slash,
// or a dotted extension tail ("config.yaml", "e.g.", "1.5" — the
// false positives cost nothing; they are tokens the table never matched).
var pathLikeRe = regexp.MustCompile(`/|[A-Za-z0-9][A-Za-z0-9_\-]*\.[A-Za-z0-9]{1,8}`)

// minifyProse applies the substitution table to prose outside protected
// spans. Fenced blocks are protected whole (fenceTracker's CommonMark
// walk), inline code spans byte-identical, and within ordinary text every
// path-like or flag-like token: the table's word-boundary matches could
// otherwise reach into "/var/cannot/x" or "--in-order-flag".
func minifyProse(s string) string {
	lines := strings.Split(s, "\n")
	var ft fenceTracker
	for i, ln := range lines {
		switch {
		case ft.inFence(ln):
			// inside a fence: verbatim
		case contractRe.MatchString(ln):
			// a contract line is never rewritten
		default:
			lines[i] = minifyLine(ln)
		}
	}
	return strings.Join(lines, "\n")
}

func minifyLine(ln string) string {
	var b strings.Builder
	for _, seg := range splitCodeSpans(ln) {
		if seg.protected {
			b.WriteString(seg.text)
			continue
		}
		b.WriteString(minifySegment(seg.text))
	}
	return b.String()
}

func minifySegment(s string) string {
	var b strings.Builder
	last := 0
	for _, iv := range protectedTokenIntervals(s) {
		b.WriteString(substitutePhrases(s[last:iv[0]]))
		b.WriteString(s[iv[0]:iv[1]])
		last = iv[1]
	}
	b.WriteString(substitutePhrases(s[last:]))
	return b.String()
}

// substitutePhrases applies the table to one unprotected stretch, carrying
// the match's leading capital over to the replacement.
func substitutePhrases(s string) string {
	return phraseRe.ReplaceAllStringFunc(s, func(m string) string {
		r, ok := phraseMap[strings.ToLower(m)]
		if !ok {
			return m
		}
		if m[0] >= 'A' && m[0] <= 'Z' {
			r = strings.ToUpper(r[:1]) + r[1:]
		}
		return r
	})
}

// protectedTokenIntervals returns the [start,end) spans of the
// path-like and flag-like tokens in s.
func protectedTokenIntervals(s string) [][2]int {
	var out [][2]int
	for _, loc := range tokenRe.FindAllStringIndex(s, -1) {
		tok := s[loc[0]:loc[1]]
		if strings.Contains(tok, "/") || pathLikeRe.MatchString(tok) ||
			(len(tok) > 1 && tok[0] == '-') {
			out = append(out, [2]int{loc[0], loc[1]})
		}
	}
	return out
}

type spanSegment struct {
	text      string
	protected bool
}

// splitCodeSpans cuts a line at inline code spans: each `…` span (matched
// backtick-run pairs, runs included) comes back protected, the prose
// between them unprotected. An unterminated opener protects the rest of
// the line — a dangling backtick is more likely markup than prose.
func splitCodeSpans(ln string) []spanSegment {
	var segs []spanSegment
	emit := func(text string, protected bool) {
		if text != "" {
			segs = append(segs, spanSegment{text, protected})
		}
	}
	pos := 0
	for i := 0; i < len(ln); {
		if ln[i] != '`' {
			i++
			continue
		}
		j := i
		for j < len(ln) && ln[j] == '`' {
			j++
		}
		runLen := j - i
		closed := -1
		for k := j; k < len(ln); {
			if ln[k] != '`' {
				k++
				continue
			}
			m := k
			for m < len(ln) && ln[m] == '`' {
				m++
			}
			if m-k == runLen {
				closed = m
				break
			}
			k = m
		}
		if closed < 0 {
			emit(ln[pos:], false)
			emit(ln[i:], true)
			return segs
		}
		emit(ln[pos:i], false)
		emit(ln[i:closed], true)
		pos = closed
		i = closed
	}
	emit(ln[pos:], false)
	return segs
}

// --- pass 3: extractive trimming ------------------------------------------

// boilerplateRe matches the openers of the restated-rationale class —
// background, rationale, motivation, history, examples-of — after any list
// or heading marker, and requires a separator (colon, comma, dash) so an
// ordinary sentence that merely starts with one of the words ("Example
// implementations live in…") never matches. Anything dropped is dropped
// whole-line: the repo's markdown rule makes one paragraph one line, so a
// line is the extractable unit.
var boilerplateRe = regexp.MustCompile(`(?i)^[ \t]*(?:[-*+][ \t]+|\d+[.)][ \t]+|#+[ \t]+)*(background|rationale|motivation|history|for example|e\.g\.|as an example|example|note that|fyi)[ \t]*(:|,|—|–)`)

// keepMarkerRe vetoes a boilerplate match: a rationale line that carries a
// constraint word is a constraint, not ceremony.
var keepMarkerRe = regexp.MustCompile(`(?i)\b(must|never|required|acceptance|criteria)\b`)

// trimExtract drops the restated-rationale lines pass 2 can only minify.
// The keep-whitelist is what makes the riskiest tier safe: a line survives
// if it states a constraint (keepMarkerRe), carries a reply contract
// (contractRe), or holds any protected content — code spans, paths, flags,
// the command lines and file lists a worker round cannot lose — and lines
// inside fenced blocks survive regardless.
func trimExtract(s string) string {
	lines := strings.Split(s, "\n")
	out := lines[:0]
	var ft fenceTracker
	for _, ln := range lines {
		if !ft.inFence(ln) && isBoilerplate(ln) {
			continue
		}
		out = append(out, ln)
	}
	// A dropped line leaves its surrounding blanks doubled; the collapse
	// pass is idempotent, so re-running it here tidies the seams it made.
	return collapseStructural(strings.Join(out, "\n"))
}

func isBoilerplate(ln string) bool {
	if !boilerplateRe.MatchString(ln) ||
		keepMarkerRe.MatchString(ln) ||
		contractRe.MatchString(ln) {
		return false
	}
	for _, seg := range splitCodeSpans(ln) {
		if seg.protected || len(protectedTokenIntervals(seg.text)) > 0 {
			return false
		}
	}
	return true
}

// --- the contract lines ----------------------------------------------------

// contractRe matches the lines whose wording is a reply/output contract —
// the sentences a downstream parser or the loop's verdict depends on
// ("Output contract — …", "reply with exactly one raw JSON array",
// "transcribe faithfully"). The shrink never rewrites (pass 2 skips them)
// and never drops (pass 3 vetoes them) a contract line, and the gate then
// verifies survival verbatim, so compression can move ceremony but never a
// constraint.
var contractRe = regexp.MustCompile(`(?i)` + strings.Join([]string{
	`output contract`,
	`reply with`,
	`respond with`,
	`your reply`,
	`entire reply`,
	`final line`,
	`raw json`,
	`no prose`,
	`do not emit`,
	`faithful`,
	`verbatim`,
}, "|"))

// contractLines lists the trimmed text of every contract line in s.
func contractLines(s string) []string {
	var out []string
	for _, ln := range strings.Split(s, "\n") {
		if contractRe.MatchString(ln) {
			out = append(out, strings.TrimSpace(ln))
		}
	}
	return out
}

// contractsSurvive reports whether every contract line appears byte-verbatim
// in out. Belt and suspenders: the passes protect these lines by
// construction, the gate is what claims it.
func contractsSurvive(contracts []string, out string) bool {
	for _, c := range contracts {
		if !strings.Contains(out, c) {
			return false
		}
	}
	return true
}
