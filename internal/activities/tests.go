package activities

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"gopkg.in/yaml.v3"

	"github.com/ozzono/daedalus/internal/config"
)

// TestResult reports the outcome of a native test run. A failing suite is
// reported via Passed=false (not a system error) so the workflow can feed the
// logs back to the agent. Logs are complete — no truncation anywhere in the
// app; complete output is the contract for every agent-facing channel, so a
// failing gate step's identity is always present in what a tests-fix round
// delivers. Command records the entrypoint that ran — declared, detected, or
// AI-discovered — for visibility in history.
type TestResult struct {
	Passed  bool
	Logs    string
	Command string
	// Coverage is the Go statement coverage percentage recorded for this
	// run ("74.2"), when the run asked for coverage and a total was
	// computable; empty otherwise. The workflow relays it (and the delta
	// against the previous round) to the reviewer.
	Coverage string
}

// RunNativeTestsActivity resolves and runs the repository's own test suite
// in one activity on the main task queue. Superseded for new runs by
// ResolveTestCommandActivity (discovery stays on the main queue) plus
// RunTestSuiteActivity (the suite itself executes on the dedicated test
// queue, TestTaskQueue); it remains registered so histories and callers
// of the combined form keep working.
func RunNativeTestsActivity(ctx context.Context, worktreePath, agent string) (TestResult, error) {
	argv, err := nativeTestCommand(ctx, worktreePath, agent)
	if err != nil {
		return TestResult{}, err
	}
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir = worktreePath
	setProcessGroup(cmd)
	defer killGroup(cmd)

	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	res := TestResult{Command: strings.Join(argv, " ")}
	if err := cmd.Start(); err != nil {
		return TestResult{}, fmt.Errorf("run tests (%s): %w", res.Command, err)
	}
	err = waitCommand(ctx, cmd)
	if err != nil && !isWaitDelay(err) {
		if _, ok := errors.AsType[*exec.ExitError](err); !ok {
			return TestResult{}, fmt.Errorf("run tests (%s): %w", res.Command, err)
		}
		res.Passed, res.Logs = false, out.String()
		return res, nil
	}
	res.Passed, res.Logs = true, out.String()
	return res, nil
}

// TestTaskQueue is the dedicated task queue the native test suite executes
// on, served by the test worker (a dumb, agent-free command runner — no
// ai-jail, no agent CLI, no provider env). One shared queue serves every
// main worker on the Temporal server: suite commands carry absolute
// worktree paths, so any test worker on the same host can run them. Across
// hosts the queue is effectively per-host — a test task routed to another
// host's test worker fails on a `cd` to a path that does not exist there.
// Main workers refuse to configure this name as their own queue (config
// ReservedTestTaskQueue), so the two poller populations cannot collide.
const TestTaskQueue = config.ReservedTestTaskQueue

// testConcurrency reads the worker-level cap on concurrent test-suite
// executions, exported at worker startup from config max_concurrent_tests
// (DAEDALUS_MAX_CONCURRENT_TESTS). Unset, malformed, or non-positive values
// fall back to the config default.
func testConcurrency() int {
	if v := os.Getenv("DAEDALUS_MAX_CONCURRENT_TESTS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return config.DefaultMaxConcurrentTests
}

// testLimiterSem is the process-wide semaphore bounding concurrent test
// suites, mirroring agentLimiter for CPU-bound host work instead of
// provider-bound agent rounds.
var (
	testLimiterOnce sync.Once
	testLimiterSem  chan struct{}
)

func testLimiter() chan struct{} {
	testLimiterOnce.Do(func() {
		testLimiterSem = make(chan struct{}, testConcurrency())
	})
	return testLimiterSem
}

// TestRunInput tells the test worker what to run: a command input plus the
// path to run it in — the requester (the workflow) supplies the worktree
// its host created. The test worker is agent-free by design, so nothing
// else crosses this boundary.
type TestRunInput struct {
	WorktreePath string
	// Command is the full shell command executing the suite.
	Command string
	// Cover, when set, asks for Go statement coverage: a command invoking
	// `go test` gains -covermode/-coverprofile and the total lands in
	// TestResult.Coverage. ponytail: recognized by the literal substring
	// "go test" — other languages' coverage needs the same slot built for
	// their runners before a flow asks for it.
	Cover bool
}

// shellQuote single-quotes s for sh, escaping embedded single quotes.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// RunTestSuiteActivity executes a test suite command on the test task
// queue. It is a dumb command runner: the command runs from ~ with a
// `cd <worktree>` prefix, its combined output comes back complete and
// verbatim, and it heartbeats throughout so a long suite shows as alive
// rather than hung (and never trips anything but the workflow's
// tests_timeout — the suite's runtime answers to that ceiling alone).
// Concurrent suites are bounded by max_concurrent_tests; further suites
// queue on the semaphore, heartbeating while they wait. A non-zero exit is
// a test failure (Passed=false) with the output captured so far; any other
// error (e.g. the shell itself missing) is a system error.
func RunTestSuiteActivity(ctx context.Context, input TestRunInput) (TestResult, error) {
	if strings.TrimSpace(input.Command) == "" {
		return TestResult{}, fmt.Errorf("empty test command for %s", input.WorktreePath)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return TestResult{}, fmt.Errorf("resolve home directory: %w", err)
	}
	lim := testLimiter()
	hbDone := make(chan struct{})
	defer close(hbDone)
	go heartbeatLoop(ctx, hbDone)
	select {
	case lim <- struct{}{}:
		defer func() { <-lim }()
	case <-ctx.Done():
		return TestResult{}, fmt.Errorf("test suite slot: %w", ctx.Err())
	}
	// Coverage is opt-in and Go-only: the profile lives outside the
	// worktree (a file inside it would pollute the run's diff), and the
	// flags are injected right after the `go test` literal, with the path
	// passed by environment — interpolating it into the composed line
	// would have to survive the nested `sh -c` quoting, which no
	// shell-quoted form does when TMPDIR carries spaces. ponytail:
	// suites whose entrypoint does not name `go test` run without
	// coverage — Coverage comes back empty rather than failing the run.
	command := input.Command
	profile := ""
	if input.Cover && strings.Contains(command, "go test") {
		tmp, err := os.MkdirTemp("", "daedalus-cover-")
		if err != nil {
			return TestResult{}, fmt.Errorf("coverage profile directory: %w", err)
		}
		defer os.RemoveAll(tmp)
		profile = filepath.Join(tmp, "cover.out")
		command = strings.Replace(command, "go test",
			`go test -covermode=atomic -coverprofile="$DAEDALUS_COVER_PROFILE"`, 1)
	}
	cmd := exec.CommandContext(ctx, "sh", "-c",
		"cd "+shellQuote(input.WorktreePath)+" && "+command)
	if profile != "" {
		cmd.Env = append(os.Environ(), "DAEDALUS_COVER_PROFILE="+profile)
	}
	cmd.Dir = home
	setProcessGroup(cmd)
	defer killGroup(cmd)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	res := TestResult{Command: command}
	start := time.Now()
	logger := activityLogger(ctx)
	if err := cmd.Start(); err != nil {
		return TestResult{}, fmt.Errorf("run tests (%s): %w", res.Command, err)
	}
	logger.Info("Test suite started", "Command", command, "Worktree", input.WorktreePath)
	// waitCommand (not Run — that would Start the already-started command
	// again and fail with exec: already started): same shutdown-drain shape
	// as runJailedRound, so a suite cut off by a worker restart comes back
	// as a red round with its captured logs, never a silent exit.
	err = waitCommand(ctx, cmd)
	logger.Info("Test suite finished", "Duration", time.Since(start).Round(time.Second))
	// Task-log block: the full combined output — the full log is the
	// point.
	appendTaskLog(ctx, fmt.Sprintf("test suite exited after %s: %s",
		time.Since(start).Round(time.Second), command), out.String())
	if err != nil && !isWaitDelay(err) {
		if _, ok := errors.AsType[*exec.ExitError](err); !ok {
			return TestResult{}, fmt.Errorf("run tests (%s): %w: %s",
				res.Command, err, out.String())
		}
		// A failing suite is a red round, not a system error — and the
		// output captured before the failure is the point: the fix loop
		// digests it.
		res.Passed, res.Logs = false, out.String()
		res.Coverage = goTotalCoverage(ctx, profile)
		return res, nil
	}
	res.Passed, res.Logs = true, out.String()
	res.Coverage = goTotalCoverage(ctx, profile)
	return res, nil
}

// goTotalCoverage reads the total statement coverage from a Go coverage
// profile (`go tool cover -func`'s final "total:" line), returned as the
// bare percentage string ("74.2"). Best-effort: anything unexpected — a
// missing profile, a tool failure, an unparsable line — comes back empty
// rather than failing a suite that ran.
func goTotalCoverage(ctx context.Context, profile string) string {
	if profile == "" {
		return ""
	}
	out, err := exec.CommandContext(ctx, "go", "tool", "cover", "-func="+profile).Output()
	if err != nil {
		return ""
	}
	for line := range strings.SplitSeq(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "total:" {
			continue
		}
		pct := strings.TrimSuffix(fields[len(fields)-1], "%")
		if _, err := strconv.ParseFloat(pct, 64); err == nil {
			return pct
		}
	}
	return ""
}

// ResolveTestCommandActivity resolves the worktree's test-suite entrypoint
// — a `.daedalus.yaml` declaration first, then static detection, then an
// AI discovery round for repos nothing recognizes (see nativeTestCommand) —
// and returns the command string for RunTestSuiteActivity. It stays on the
// main task queue because the AI-discovery fallback is a jailed agent round
// needing the worker's provider environment, unlike the suite execution
// itself. agent is the run's -cli/--cli override, honored by the discovery
// round alone; empty falls back to the worker's DAEDALUS_AGENT.
func ResolveTestCommandActivity(ctx context.Context, worktreePath, agent string) (string, error) {
	argv, err := nativeTestCommand(ctx, worktreePath, agent)
	if err != nil {
		return "", err
	}
	// Quote element-wise. nativeTestCommand wraps declared and discovered
	// commands as ["sh", "-c", cmd], and a bare join would let the outer
	// shell (RunTestSuiteActivity's `sh -c "cd … && …"`) split the inner
	// command string: `sh -c go test ./...` runs bare `go`. Quoted, the
	// inner `sh -c 'go test ./...'` is the command nativeTestCommand meant.
	quoted := make([]string, len(argv))
	for i, a := range argv {
		quoted[i] = shellQuote(a)
	}
	return strings.Join(quoted, " "), nil
}

// nativeTestCommand resolves how to run the worktree's own test suite:
//  1. a `tests:` declaration in the repo's .daedalus.yaml — the repo owner's
//     explicit word, always winning;
//  2. static detection: marker files (go.mod, package.json with a test
//     script, pytest config) and Makefile test-ui/test-api targets;
//  3. an AI discovery round — a short jailed agent run that answers with
//     the command — for repositories none of the above recognize.
//
// agent is the run's -cli/--cli override, honored by the discovery round
// alone; empty falls back to the worker's DAEDALUS_AGENT.
func nativeTestCommand(ctx context.Context, worktreePath, agent string) ([]string, error) {
	if declared, ok := declaredTestCommand(worktreePath); ok {
		return []string{"sh", "-c", declared}, nil
	}
	if argv, ok := detectedTestCommand(worktreePath); ok {
		return argv, nil
	}
	return discoverTestCommand(ctx, worktreePath, agent)
}

// declaredTestCommand reads the repo-owned test declaration.
func declaredTestCommand(worktreePath string) (string, bool) {
	data, err := os.ReadFile(filepath.Join(worktreePath, ".daedalus.yaml"))
	if err != nil {
		return "", false
	}
	var decl struct {
		Tests string `yaml:"tests"`
	}
	if err := yaml.Unmarshal(data, &decl); err != nil {
		return "", false
	}
	if s := strings.TrimSpace(decl.Tests); s != "" {
		return s, true
	}
	return "", false
}

// detectedTestCommand recognizes the common test entrypoints from marker
// files. Order matters only in that go.mod wins over a Makefile — a Go repo
// that also has make targets usually wraps the same suite.
func detectedTestCommand(worktreePath string) ([]string, bool) {
	if _, err := os.Stat(filepath.Join(worktreePath, "go.mod")); err == nil {
		return []string{"go", "test", "./..."}, true
	}
	if pkg, err := os.ReadFile(filepath.Join(worktreePath, "package.json")); err == nil {
		var p struct {
			Scripts struct {
				Test string `json:"test"`
			} `json:"scripts"`
		}
		if json.Unmarshal(pkg, &p) == nil && strings.TrimSpace(p.Scripts.Test) != "" {
			return []string{"npm", "test"}, true
		}
	}
	for _, marker := range []string{"pyproject.toml", "pytest.ini", "setup.cfg"} {
		if _, err := os.Stat(filepath.Join(worktreePath, marker)); err == nil {
			return []string{"pytest", "-q"}, true
		}
	}
	if mk, err := os.ReadFile(filepath.Join(worktreePath, "Makefile")); err == nil {
		var targets []string
		for _, target := range []string{"test-ui", "test-api"} {
			if makefileHasTarget(string(mk), target) {
				targets = append(targets, target)
			}
		}
		if len(targets) > 0 {
			return append([]string{"make"}, targets...), true
		}
	}
	return nil, false
}

// makefileHasTarget reports whether the Makefile declares target as a rule
// ("target:" starting a line).
func makefileHasTarget(mk, target string) bool {
	for line := range strings.SplitSeq(mk, "\n") {
		if strings.HasPrefix(line, target+":") {
			return true
		}
	}
	return false
}

// ErrNoSuite is the sentinel discovery returns when the repository has no
// test suite at all: the AI round answered NONE, meaning nothing can be red
// — a greenfield repo is not a red baseline. The preflight gate treats it
// as a vacuous pass; flows that need a suite to gate on surface it as the
// run error it is there.
var ErrNoSuite = errors.New("no test suite exists in this repository")

// discoverTestCommand asks a short jailed agent run for the repository's
// test entrypoint — the general, AI-led fallback. agent is the run's
// -cli/--cli override, empty for the worker's default.
func discoverTestCommand(ctx context.Context, worktreePath, agent string) ([]string, error) {
	res, err := runJailed(ctx, agent, worktreePath,
		"Inspect this repository and determine the exact shell command that runs its full test suite. "+
			"The suite may be make test, go test ./..., flutter test, npm test, pytest, cargo test, mvn test, "+
			"or any other language's standard runner — inspect build files and CI config to determine it. "+
			"Reply with EXACTLY one line: either that command, or the single word NONE if the repository "+
			"has no test suite. No explanation, no code fences.")
	if err != nil {
		return nil, fmt.Errorf("discover test command: %w", err)
	}
	// The round spawns through jailedAgentCLI, which resolves the worker's
	// DAEDALUS_AGENT default — the parse must dispatch on the same
	// resolved CLI, not the raw override (empty means the default, and a
	// pi-default worker would otherwise be parsed as claude and feed a
	// JSON session header to firstCommandLine as the "test command").
	selected, _, _ := jailedAgentCLI(agent)
	_, text, _, _ := parseRoundOutput(selected, res.Stdout)
	if text == "" {
		// Plain-text CLI (opencode, aider) or a parse miss: the raw stdout
		// is the reply.
		text = res.Stdout
	}
	cmd := firstCommandLine(text)
	if cmd == "" {
		return nil, fmt.Errorf("no test command found for %s — declare one in .daedalus.yaml (tests: <command>)", worktreePath)
	}
	// The NONE verdict travels as one word, but models routinely dress it
	// ("NONE.", "NONE — no test suite"): match the first word with
	// sentence punctuation trimmed, so a dressed NONE never becomes the
	// suite command. No real test runner is named "none", so the
	// liberality is safe.
	none := cmd
	if i := strings.IndexAny(none, " \t"); i >= 0 {
		none = none[:i]
	}
	if strings.EqualFold(strings.Trim(none, ".,;:!?\"'`"), "NONE") {
		return nil, ErrNoSuite
	}
	return []string{"sh", "-c", cmd}, nil
}

// firstCommandLine extracts a single-line command from an agent reply: the
// first command-shaped, non-fence line, stripped of backticks. A rejected
// candidate does not end the scan — CLI chrome (separator rules, banners)
// precedes the model's reply in raw stdout, so the scan continues past it;
// the reply is more often later than the banner, never earlier.
func firstCommandLine(text string) string {
	for line := range strings.SplitSeq(text, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "```") {
			continue
		}
		line = strings.TrimSpace(strings.Trim(line, "`"))
		if line != "" && len(line) <= 500 && commandShaped(line) {
			return line
		}
	}
	return ""
}

// commandShaped reports whether a candidate line can plausibly be a shell
// command: it starts with a name, a path (./gradlew, ~/bin/run-tests,
// /usr/bin/make), or an expansion ($VAR, $(…), {…}, a quoted word), and is
// not a decorative rule built from one repeated non-alphanumeric character
// (──────, ======, ******). It exists so CLI banner chrome never becomes
// a suite command. ponytail: chrome that is itself command-shaped (aider's
// "Aider v0.86.2" banner) still passes — full discrimination needs the
// CLI's reply channel, not stdout shape heuristics.
func commandShaped(line string) bool {
	first, _ := utf8.DecodeRuneInString(line)
	if unicode.IsLetter(first) || unicode.IsDigit(first) {
		return true
	}
	for _, r := range line {
		if r != first {
			// Not a decorative run — but only a shell-plausible starter
			// (path, expansion, quoting) may still be a command; any other
			// leading punctuation is chrome (bullets, rules, quotes-prose).
			return strings.ContainsRune("./~$({[\"'", first)
		}
	}
	// Every rune identical: decorative rule.
	return false
}
