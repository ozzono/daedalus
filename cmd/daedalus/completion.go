// Shell completion: the hidden `daedalus __complete` command answers "what
// can come next" for the words typed so far — one candidate per line and
// nothing else on the path — and `daedalus completion bash|zsh` prints the
// small script that feeds the shell's tab press to that probe. The candidate
// sources are the dispatch's own tables (commandHelp for commands,
// workerActions for worker subcommands, initProfiles for init's profiles,
// flagTable for flags), completion's own shells, and — for the positions
// that take a workflow id — one bounded Temporal visibility query, so a
// command, action, flag, or live id is completed without touching this
// file.
package main

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	workflowservice "go.temporal.io/api/workflowservice/v1"

	"github.com/ozzono/daedalus/internal/config"
)

// completionShells are the shells `daedalus completion` writes an installer
// for — also the probe's candidates at the position after "completion".
var completionShells = []string{"bash", "zsh"}

// liveWorkflowIDs and recentWorkflowIDs are the completion's workflow-id
// sources: only a running run can be woken up, appended to, chained behind,
// or attached to, while continue/log/wipe address closed sessions too and
// take the recent set. They take the config path typed so far ("" for the
// default resolution) — the offered ids must answer the queue the
// invocation would act on, not whichever config the cwd resolves to.
// Package vars so the suite can stub them (the daemonDir pattern) — the
// probe never dials a real Temporal.
var (
	liveWorkflowIDs   = func(configPath string) []string { return listCompletionIDs(true, configPath) }
	recentWorkflowIDs = func(configPath string) []string { return listCompletionIDs(false, configPath) }
)

// completionIDFreshness bounds the recent id set — candidates age out
// instead of accumulating unbounded.
const completionIDFreshness = 12 * time.Hour

// completionIDTimeout is the visibility query's budget: a tab press must
// never wait past it on Temporal itself. The client dial in front of the
// query is not under this bound — the SDK connects eagerly (about five
// seconds worst case against an unreachable-by-drop host; a refused
// connection, the common localhost case, fails fast) — so the probe's
// worst case is the dial plus this budget. A dead Temporal answers empty
// either way (the completion scripts fall back to shell file completion
// on empty).
const completionIDTimeout = 3 * time.Second

// listCompletionIDs lists the deployment's workflow ids from Temporal
// visibility — the established first-page shape (report, tasklog,
// recapture), newest first — running-only, or started within the freshness
// bound. The queue comes from the named config (configPath, or the default
// resolution when empty). A config-resolution or query failure returns
// empty, never an error. An id that could not ride safely inside a
// visibility query — the same plain-id guard listWorkflowRuns applies —
// is never offered, and the same guard on the queue answers empty;
// repeated ids (a continued session's executions) are offered once.
func listCompletionIDs(runningOnly bool, configPath string) []string {
	given := configPath
	if given == "" {
		given = defaultConfigPath
	}
	path, err := resolveConfigPath(given)
	if err != nil {
		return nil
	}
	cfg, err := config.Load(path)
	if err != nil {
		return nil
	}
	queue := cfg.Temporal.TaskQueue
	if strings.ContainsAny(queue, "'\\") {
		return nil
	}
	query := fmt.Sprintf("TaskQueue = '%s'", queue)
	if runningOnly {
		query += " AND ExecutionStatus = 'Running'"
	} else {
		query += fmt.Sprintf(" AND StartTime >= '%s'", time.Now().Add(-completionIDFreshness).UTC().Format(time.RFC3339))
	}
	ctx, cancel := context.WithTimeout(context.Background(), completionIDTimeout)
	defer cancel()
	c, err := newClient(cfg)
	if err != nil {
		return nil
	}
	defer c.Close()
	resp, err := c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: "default",
		PageSize:  100,
		Query:     query,
	})
	if err != nil {
		return nil
	}
	seen := make(map[string]bool, len(resp.GetExecutions()))
	var ids []string
	for _, e := range resp.GetExecutions() {
		id := e.GetExecution().GetWorkflowId()
		if seen[id] || strings.ContainsAny(id, "'\\") {
			continue
		}
		seen[id] = true
		ids = append(ids, id)
	}
	return ids
}

// complete returns the completion candidates for the word being completed.
// words is the command line so far with the partial word last ("" when only
// tab was pressed). Depth follows the CLI's own grammar: top-level commands;
// worker actions; worker restart targets (the record-driven `all`, --all,
// and every worker on record); flag names at any position; and the
// workflow-id positions — a bounded visibility query per tab press
// (listCompletionIDs), running ids where only a running run can be
// addressed, recent ones where a closed session still means something,
// degrading to an empty reply on a dead Temporal. Paths and -f/--file
// values stay served by the shell's default file completion.
func complete(words []string) []string {
	var typed []string
	toComplete := ""
	if len(words) > 0 {
		typed, toComplete = words[:len(words)-1], words[len(words)-1]
	}
	// The id sources resolve the config the typed line itself selects —
	// -c/--config wherever it sits — so the offered ids answer the queue
	// the invocation would act on.
	cfgPath := typedConfigPath(typed)
	// A value-taking flag as the previous word is consuming this word: its
	// own candidates when it has one — -a and -dep offer live ids, run
	// being the only command that parses them — else none (file completion
	// covers -f/--file's value). -f in the restart scope is boolean force:
	// it consumes nothing, so the line falls through to the positional
	// machinery below.
	if n := len(typed); n > 0 {
		prev := typed[n-1]
		if s, ok := flagTable[prev]; ok && s.value && !(prev == "-f" && n-1 >= restartForceFrom(typed)) {
			if s.candidates != nil && commandWord(typed) == "run" {
				return prefixed(s.candidates(cfgPath), toComplete)
			}
			return nil
		}
	}
	// The command is the first word that is not a flag; global flags are
	// accepted anywhere, so they are skipped wherever they sit (the
	// "--name=value" form too — a lone "-" word with an "=" in it).
	pos := 0
	for pos < len(typed) {
		if s, ok := flagTable[typed[pos]]; ok {
			pos++
			if s.value && pos < len(typed) {
				pos++
			}
			continue
		}
		if strings.HasPrefix(typed[pos], "-") && strings.Contains(typed[pos], "=") {
			pos++
			continue
		}
		break
	}
	if strings.HasPrefix(toComplete, "-") {
		// The restart-target position's only offered flag is --all: the
		// record-driven restarts (`restart all`, `restart <worker>`)
		// reject the global flags outright, so offering them there would
		// complete an invocation parseFlags hard-errors on. (The scope's
		// -f/--force parse there too but stay unoffered — the pinned
		// surface keeps this position's answer at --all.)
		if pos+2 == len(typed) && typed[pos] == "worker" && typed[pos+1] == "restart" {
			return prefixed([]string{"--all"}, toComplete)
		}
		return flagCandidates(typed, toComplete)
	}
	if pos >= len(typed) {
		return prefixed(commandCandidates(), toComplete)
	}
	if typed[pos] == "worker" {
		// Global flags parse from anywhere, so only the positional tail
		// reaches the action/target positions (same skip as init below):
		// `worker -c cfg.yaml restart <tab>` is a valid restart, not a
		// saturated one. The tail keeps its `worker` head through
		// stripRestartForce — that helper anchors the force scope on the
		// command position — and the head is dropped after, with the
		// scope's -f/--force gone too: both force spellings leave the
		// restart target reachable, the words parseFlags would consume
		// as force never saturating the position.
		rest := positionalWords(stripRestartForce(typed[pos:])[1:])
		switch {
		case len(rest) == 0:
			return prefixed(workerActions, toComplete)
		case len(rest) == 1 && rest[0] == "restart":
			// The restart targets: the record-driven "all" spelling
			// plus every worker on record, running or not.
			targets := []string{"all"}
			if names, err := recordedWorkers(); err == nil {
				targets = append(targets, names...)
			}
			return prefixed(targets, toComplete)
		case len(rest) == 1 && rest[0] == "wakeup":
			// The workflow id: only a running run can be woken.
			return prefixed(liveWorkflowIDs(cfgPath), toComplete)
		}
	}
	switch typed[pos] {
	case "guide", "attach":
		// The workflow id, running-only: guidance lands on a live round
		// and attach reconnects to a live run (a finished one reports its
		// outcome, but a running one is what reattaching means). A second
		// positional completes nothing.
		rest := positionalWords(typed[pos+1:])
		if len(rest) == 0 {
			return prefixed(liveWorkflowIDs(cfgPath), toComplete)
		}
	case "continue", "log", "wipe":
		// The workflow id from the recent set: all three address closed
		// sessions as readily as running ones, and the freshness bound
		// keeps the listing from aging into noise.
		rest := positionalWords(typed[pos+1:])
		if len(rest) == 0 {
			return prefixed(recentWorkflowIDs(cfgPath), toComplete)
		}
	case "init":
		// The profile arguments. They compose, so a line already carrying
		// one profile still offers the other. Global flags parse from
		// anywhere, so only the positional tail counts toward the
		// saturation: a flag word (or the value a value-taking flag
		// consumes) is never a profile argument — the same skip the
		// command scan at the top does.
		rest := positionalWords(typed[pos+1:])
		if len(rest) < len(initProfiles) {
			var left []string
			for _, p := range initProfiles {
				if !slices.Contains(rest, p) {
					left = append(left, p)
				}
			}
			return prefixed(left, toComplete)
		}
	case "metrics", "dash":
		// The action argument; the actions take no further positionals
		// (list's flags complete no differently than worker's — not at
		// all). Both spellings complete identically — metrics is the
		// dash command's hidden compatibility alias.
		rest := positionalWords(typed[pos+1:])
		if len(rest) == 0 {
			return prefixed(metricsActions, toComplete)
		}
	case "completion":
		// Exactly one shell argument — a second is a usage failure.
		if pos+1 == len(typed) {
			return prefixed(completionShells, toComplete)
		}
	}
	return nil
}

// commandWord returns the subcommand word of a typed line — the position
// commandPos finds — or "" when none: the scope check behind the
// flag-value id sources, which only `run` parses (-a, -dep).
func commandWord(typed []string) string {
	if p := commandPos(typed); p >= 0 {
		return typed[p]
	}
	return ""
}

// typedConfigPath returns the -c/--config value typed so far — separate
// word or --config= spelling, either — or "" when none. First occurrence
// wins; a dangling -c at the line's end never reaches here (the
// value-position check answers nil first).
func typedConfigPath(typed []string) string {
	for i, w := range typed {
		if name, val, hasEq := strings.Cut(w, "="); hasEq {
			if s, ok := flagTable[name]; ok && s.name == "config" {
				return val
			}
			continue
		}
		if s, ok := flagTable[w]; ok && s.name == "config" && s.value && i+1 < len(typed) {
			return typed[i+1]
		}
	}
	return ""
}

// commandCandidates lists the completable top-level commands: commandHelp
// holds a detailed help screen for every dispatch command and doubles as
// the completion's command list (the __complete probe stays hidden — it is
// the one command with neither). The metrics key is the dash command's
// hidden compatibility alias — accepted everywhere, offered nowhere.
func commandCandidates() []string {
	names := make([]string, 0, len(commandHelp))
	for name := range commandHelp {
		if name == "metrics" {
			continue
		}
		names = append(names, name)
	}
	return names
}

// positionalWords filters a typed tail down to its positional words: flag
// words drop out, a value-taking flag also drops the word it consumes, and
// the "--name=value" form is one word. parseFlags strips the global flags
// from anywhere, so none of them can be an argument of the command.
func positionalWords(words []string) []string {
	var out []string
	for i := 0; i < len(words); i++ {
		if s, ok := flagTable[words[i]]; ok {
			if s.value {
				i++
			}
			continue
		}
		if strings.HasPrefix(words[i], "-") && strings.Contains(words[i], "=") {
			continue
		}
		out = append(out, words[i])
	}
	return out
}

// flagCandidates lists every flag spelling matching the prefix, each once:
// a flag already used — by either of its spellings — is suppressed, so a
// repeated flag is never offered twice.
func flagCandidates(typed []string, prefix string) []string {
	used := map[string]bool{}
	for _, w := range typed {
		if s, ok := flagTable[w]; ok {
			used[s.name] = true
		}
	}
	var out []string
	for spelling, s := range flagTable {
		if !used[s.name] && strings.HasPrefix(spelling, prefix) {
			out = append(out, spelling)
		}
	}
	sort.Strings(out)
	return out
}

// prefixed filters candidates to those starting with the word being
// completed, sorted for a deterministic listing.
func prefixed(candidates []string, prefix string) []string {
	var out []string
	for _, c := range candidates {
		if strings.HasPrefix(c, prefix) {
			out = append(out, c)
		}
	}
	sort.Strings(out)
	return out
}

// completionScript returns the tab-completion installer for the named shell
// (bash or zsh): on every tab it calls the __complete probe with the words
// typed so far and offers the reply, falling back to the shell's file
// completion when the probe answers nothing.
func completionScript(shell string) string {
	switch shell {
	case "bash":
		return `# bash completion for daedalus — install with: eval "$(daedalus completion bash)"
__daedalus_complete() {
    local IFS=$'\n'
    COMPREPLY=($(daedalus __complete "${COMP_WORDS[@]:1}" 2>/dev/null))
}
complete -o default -F __daedalus_complete daedalus
`
	case "zsh":
		return `# zsh completion for daedalus — install with: eval "$(daedalus completion zsh)"
__daedalus_complete() {
    local -a candidates
    candidates=(${(f)"$(daedalus __complete "${words[@]:1}" 2>/dev/null)"})
    if ((${#candidates})); then
        compadd -a candidates
    else
        _files
    fi
}
compdef __daedalus_complete daedalus
`
	}
	return ""
}
