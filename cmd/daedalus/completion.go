// Shell completion: the hidden `daedalus __complete` command answers "what
// can come next" for the words typed so far — one candidate per line and
// nothing else on the path — and `daedalus completion bash|zsh` prints the
// small script that feeds the shell's tab press to that probe. The candidate
// sources are the dispatch's own tables (commandHelp for commands,
// workerActions for worker subcommands, flagTable for flags), so a command,
// action, or flag added once is completed without touching this file.
package main

import (
	"sort"
	"strings"
)

// complete returns the completion candidates for the word being completed.
// words is the command line so far with the partial word last ("" when only
// tab was pressed). Depth follows the CLI's own grammar: top-level commands;
// worker actions; worker restart targets (the record-driven `all`, --all,
// and every worker on record); flag names at any position. Run ids,
// workflow names, and paths are deliberately not completed — -f/--file and
// paths are served by the shell's default file completion, and id
// enumeration would put a query on an interactive keystroke.
func complete(words []string) []string {
	var typed []string
	toComplete := ""
	if len(words) > 0 {
		typed, toComplete = words[:len(words)-1], words[len(words)-1]
	}
	// A value-taking flag as the previous word is consuming this word:
	// no candidates (file completion covers -f/--file's value).
	if n := len(typed); n > 0 {
		if s, ok := flagTable[typed[n-1]]; ok && s.value {
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
		// The restart-target position's only accepted flag is --all: the
		// record-driven restarts (`restart all`, `restart <worker>`)
		// reject the global flags outright, so offering them there would
		// complete an invocation parseFlags hard-errors on.
		if pos+2 == len(typed) && typed[pos] == "worker" && typed[pos+1] == "restart" {
			return prefixed([]string{"--all"}, toComplete)
		}
		return flagCandidates(typed, toComplete)
	}
	if pos >= len(typed) {
		return prefixed(commandCandidates(), toComplete)
	}
	if typed[pos] == "worker" {
		rest := typed[pos+1:]
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
		}
	}
	return nil
}

// commandCandidates lists the completable top-level commands: commandHelp
// holds a detailed help screen for every dispatch command and doubles as
// the completion's command list (the __complete probe stays hidden — it is
// the one command with neither).
func commandCandidates() []string {
	names := make([]string, 0, len(commandHelp))
	for name := range commandHelp {
		names = append(names, name)
	}
	return names
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
