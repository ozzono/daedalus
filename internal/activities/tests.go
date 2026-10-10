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
// logs back to the agent. The complete output always reaches the run's task
// log on the worker host before anything is bounded; Logs carries it complete
// when it fits the transport, otherwise the tail plus a marker naming the
// total size and the task-log path (the no-truncation contract governs
// daedalus's channels — Temporal caps its payloads regardless of what we
// send, and an over-limit payload fails its own upload, TMPRL1103).
// Command records the entrypoint that ran — declared, detected, or
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
	// DumpPath is the worktree-relative path of the file holding the
	// suite's complete combined output (test_output dumping enabled and
	// the write succeeded); empty otherwise — dumping off or the
	// best-effort write failed, in which case the task log alone holds the
	// complete record, exactly as before. Inside the run's worktree, so a
	// jailed tester or reviewer can read it.
	DumpPath string
}

// RunNativeTestsActivity resolves and runs the repository's own test suite
// in one activity on the main task queue. Superseded for new runs by
// ResolveTestCommandActivity (discovery stays on the main queue) plus
// RunTestSuiteActivity (the suite itself executes on the deployment's
// suite queue); it remains registered so histories and callers
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
	// Task-log block before any bounding — same contract as
	// RunTestSuiteActivity: the complete output reaches the record first.
	logPath := appendTaskLog(ctx, fmt.Sprintf("native test run finished: %s", res.Command), out.String())
	if err != nil && !isWaitDelay(err) {
		if _, ok := errors.AsType[*exec.ExitError](err); !ok {
			return TestResult{}, fmt.Errorf("run tests (%s): %w", res.Command, err)
		}
		res.Passed, res.Logs = false, boundedOutput(logPath, out.String())
		return res, nil
	}
	res.Passed, res.Logs = true, boundedOutput(logPath, out.String())
	return res, nil
}

// Suite activities execute on a deployment's suite queue: the Temporal-wide
// shared ReservedTestTaskQueue for a shared_test_queue-true config, the
// derived config.TestQueueFor(taskQueue) for an opted-out one (schedule
// site: the workflows package, from the run's PipelineInput; pollers:
// cmd/daedalus worker startup, from the same config — one config drives
// both ends, so a deployment never schedules on a queue its own workers
// ignore). Suite commands carry absolute worktree paths, so any worker on
// the same host can run them; across hosts the queue is effectively
// per-host — a suite routed to another host's worker fails on a `cd` to a
// path that does not exist there.

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
	// OutputDir is the worktree-relative folder the suite's complete
	// combined output is dumped under (config test_output, resolved by the
	// scheduling deployment and carried through the pipeline input — never
	// worker env, since a shared test queue can land the suite on a
	// foreign deployment's worker whose env would name the wrong dir).
	// Empty means dumping is off; nothing is written and the result is
	// byte-identical to before. It rides the same config→pipeline-input
	// route as shared_test_queue (one config drives the scheduling end).
	OutputDir string
}

// shellQuote single-quotes s for sh, escaping embedded single quotes.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// RunTestSuiteActivity executes a test suite command on the test task
// queue. It is a dumb command runner: the command runs from ~ with a
// `cd <worktree>` prefix, and it heartbeats throughout so a long suite
// shows as alive rather than hung (and never trips anything but the
// workflow's tests_timeout — the suite's runtime answers to that ceiling
// alone). The combined output lands complete in the run's task log; what
// returns in the result or error carries it bounded when it exceeds the
// transport limit (see boundedOutput). Concurrent suites are bounded by
// max_concurrent_tests; further suites queue on the semaphore, heartbeating
// while they wait. A non-zero exit is a test failure (Passed=false) with
// the output captured so far; any other error (e.g. the shell itself
// missing) is a system error.
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
	// point. The path feeds the transport-bound copies below.
	logPath := appendTaskLog(ctx, fmt.Sprintf("test suite exited after %s: %s",
		time.Since(start).Round(time.Second), command), out.String())
	// Suite-output dump (test_output enabled): the complete record also
	// lands in the worktree, where the jailed tester and reviewer can read
	// it. Best-effort — a failed dump leaves DumpPath empty and the task
	// log holding the complete record, and never changes the verdict.
	fullLogPath := logPath
	if input.OutputDir != "" {
		if dumpPath := dumpSuiteOutput(ctx, input.WorktreePath, input.OutputDir, out.String()); dumpPath != "" {
			res.DumpPath = dumpPath
			fullLogPath = dumpPath
		}
		// Post-dump mirror (test_output.mirror), the same best-effort
		// contract as the dump itself: a mirror failure leaves the task
		// log and the worktree dump holding the complete record and never
		// changes the verdict.
		mirrorToHost(ctx, input.WorktreePath, input.OutputDir, os.Getenv(config.TestOutputMirrorEnv))
	}
	if err != nil && !isWaitDelay(err) {
		if _, ok := errors.AsType[*exec.ExitError](err); !ok {
			return TestResult{}, fmt.Errorf("run tests (%s): %w: %s",
				res.Command, err, boundedOutput(fullLogPath, out.String()))
		}
		// A failing suite is a red round, not a system error — and the
		// output captured before the failure is the point: the fix loop
		// digests it.
		res.Passed, res.Logs = false, boundedOutput(fullLogPath, out.String())
		res.Coverage = goTotalCoverage(ctx, profile)
		return res, nil
	}
	res.Passed, res.Logs = true, boundedOutput(fullLogPath, out.String())
	res.Coverage = goTotalCoverage(ctx, profile)
	return res, nil
}

// dumpSuiteOutput writes the suite's complete combined output to
// <worktreePath>/<dir>/<timestamp>.log and returns the worktree-relative
// path; "" when the write failed (best-effort — see RunTestSuiteActivity).
// The timestamp is second-precision; a file already at that name is never
// overwritten — the run lands in "-2", "-3", … (O_EXCL create, so two
// suites ending the same second cannot clobber each other). The dir is
// kept out of `git status` (excludeSuiteDirFromStatus) so an
// agent-staged diff cannot pick the log up.
func dumpSuiteOutput(ctx context.Context, worktreePath, dir, out string) string {
	logger := activityLogger(ctx)
	absDir := filepath.Join(worktreePath, dir)
	if err := os.MkdirAll(absDir, 0o755); err != nil {
		logger.Warn("Test-output dump dir could not be created", "Dir", absDir, "Error", err)
		return ""
	}
	base := time.Now().Format("20060102-150405")
	name := base + ".log"
	for i := 2; ; i++ {
		f, err := os.OpenFile(filepath.Join(absDir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if os.IsExist(err) {
			name = fmt.Sprintf("%s-%d.log", base, i)
			continue
		}
		if err != nil {
			logger.Warn("Test-output dump file could not be created", "Path", filepath.Join(absDir, name), "Error", err)
			return ""
		}
		if _, err := f.WriteString(out); err != nil {
			f.Close()
			logger.Warn("Test-output dump write failed", "Path", filepath.Join(absDir, name), "Error", err)
			return ""
		}
		if err := f.Close(); err != nil {
			logger.Warn("Test-output dump write failed", "Path", filepath.Join(absDir, name), "Error", err)
			return ""
		}
		excludeSuiteDirFromStatus(ctx, worktreePath, dir)
		return filepath.Join(dir, name)
	}
}

// excludeSuiteDirFromStatus best-effort appends dir to the repository's
// .git/info/exclude, so the dump never surfaces as an untracked file a
// stage-everything agent round could commit into the deliverable diff. The
// real exclude path is resolved through git — a linked worktree's `.git`
// is a file pointing at its gitdir, and info/exclude lives in the common
// dir — and any failure is a logged no-op: a leftover untracked log is a
// review-visible artifact, never a failed suite. ponytail: info/exclude
// matches gitignore-style, so a dir bearing `*?[` metachars may not match
// its own line, and one starting with `!` or `#` lands as a negation or
// comment — un-ignoring (or ignoring nothing) across every worktree of
// that repository — because validateWorktreeRelDir admits any non-path
// bytes verbatim. The operator-trusted dir makes residual risk
// review-visible, per the same ceiling as the task's branch-hygiene
// contract.
func excludeSuiteDirFromStatus(ctx context.Context, worktreePath, dir string) {
	out, err := exec.CommandContext(ctx, "git", "-C", worktreePath,
		"rev-parse", "--git-path", "info/exclude").Output()
	if err != nil {
		return
	}
	path := strings.TrimSpace(string(out))
	if path == "" {
		return
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(worktreePath, path)
	}
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		if line == dir {
			return
		}
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	prefix := ""
	if len(data) > 0 && data[len(data)-1] != '\n' {
		prefix = "\n"
	}
	f.WriteString(prefix + dir + "\n")
}

// testOutputTailLimit bounds suite output that travels through Temporal
// payloads (activity results and errors — both are payloads under the same
// server limits; an over-limit error fails its own upload with TMPRL1103,
// 2026-09-27 nested-config incident). Sized well under any plausible server
// blob-error limit (historically 2 MB).
const testOutputTailLimit = 512 << 10 // 512 KiB

// boundedOutput passes out through byte-identical when it fits the
// transport limit; larger output becomes a marker line — total size, the
// fact of the cut, and the path holding the complete record — followed by
// the final testOutputTailLimit bytes (a tail: the end of a run is where
// failures live). fullPath may be empty (no full record exists); the
// marker then names the cut only. It is the worktree dump path when a
// dump was written (readable by a jailed agent), else the task-log path.
// The complete output always reaches the task log before anything is
// bounded, so the no-truncation contract holds — only what fits
// Temporal's payload cap changes.
func boundedOutput(fullPath, out string) string {
	if len(out) <= testOutputTailLimit {
		return out
	}
	loc := ""
	if fullPath != "" {
		loc = "; full output: " + fullPath
	}
	marker := fmt.Sprintf("[daedalus: suite output exceeds the transport limit — %d bytes total; last %d bytes follow%s]\n",
		len(out), testOutputTailLimit, loc)
	return marker + out[len(out)-testOutputTailLimit:]
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
//  2. static detection: a plain Makefile test: rule, then the language
//     marker files (go.mod, package.json with a test script, pytest
//     config), then Makefile test-ui/test-api targets;
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
// files. A plain Makefile test: rule wins over the single-language defaults
// — a repo that declares its own test composition means to run it, not the
// bare default its primary language would suggest.
func detectedTestCommand(worktreePath string) ([]string, bool) {
	if mk, err := os.ReadFile(filepath.Join(worktreePath, "Makefile")); err == nil && makefileHasTarget(string(mk), "test") {
		return []string{"make", "test"}, true
	}
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
// ("target:" starting a line). Variable assignments sharing the prefix do
// not match — plain "target:=v", POSIX "target::=v", and target-specific
// assignments like "target: X = 3" ("?=", "+=", "!=", also after an
// override/export keyword). The rejection is a literal "=" scan: in that
// position make does read a bare "target: foo=bar" as an assignment, not a
// rule (GNU-make-probed). The scan is literal, so an "=" inside a $(…)
// prereq reference is rejected too — make actually parses those as rules
// (probed: "test: $(shell echo a=b)" → "No rule to make target 'a=b',
// needed by 'test'") — but their expanded prereqs are unbuildable in
// practice, so the rejection is a safe fall-through to the next detection
// step, never a false rule.
func makefileHasTarget(mk, target string) bool {
	for line := range strings.SplitSeq(mk, "\n") {
		if strings.HasPrefix(line, target+":") {
			if strings.Contains(line[len(target)+1:], "=") {
				continue // variable assignment, not a rule
			}
			return true
		}
	}
	return false
}

// ErrNoSuite is the sentinel discovery returns when the repository has no
// test suite at all: the AI round answered NONE, or every discovery
// candidate failed the existence probe (a fabricated runner is treated as
// suite-less, never as red), meaning nothing can be red — a greenfield repo
// is not a red baseline. The preflight gate treats it as a vacuous pass;
// flows that need a suite to gate on surface it as the run error it is
// there.
var ErrNoSuite = errors.New("no test suite exists in this repository")

// discoverTestCommand asks a short jailed agent run for the repository's
// test entrypoint — the general, AI-led fallback. agent is the run's
// -cli/--cli override, empty for the worker's default.
//
// A model — small ones especially — treats a list of example commands as
// the answer menu and parrots the first entry on a suite-less repo
// (wa-termo 2026-09-26: `make test` answered for a repo with no Makefile),
// parking the run as an opaque red baseline indistinguishable from a real
// one. The prompt therefore carries a verification contract instead of
// example commands, and every candidate is probed before it is trusted
// (probeTestCommand): a failed probe buys one re-discovery round, then the
// verdict degrades to NONE — a repo whose declared runner cannot be
// verified to exist is suite-less (ErrNoSuite, vacuous gate pass), never
// red. A passing probe never swallows a real red baseline: suite execution
// still decides at the gate.
func discoverTestCommand(ctx context.Context, worktreePath, agent string) ([]string, error) {
	for attempt := 0; attempt < 2; attempt++ {
		// Snapshot the reply channel before each round: chat.history.md lives in
		// the repo-writable worktree, so a repo-planted file must not forge the
		// reply (backlog/bugs/aider-history-plant-forge). Only bytes appended
		// after the snapshot are accepted below; mid-round tampering degrades
		// to model trust, the same level as the stdout fallback. The snapshot
		// is per-round: the history accumulates across a probe retry too.
		histPath := filepath.Join(worktreePath, ".daedalus-aider", "chat.history.md")
		var histLen int64
		if st, err := os.Stat(histPath); err == nil {
			histLen = st.Size()
		}
		res, err := runJailed(ctx, agent, worktreePath,
			"Inspect this repository and determine the exact shell command that runs its full test suite. "+
				"Before answering, verify the entrypoint actually exists in this repository — inspect the build "+
				"files (Makefile, package.json, go.mod, pyproject.toml, Cargo.toml, pom.xml, build.gradle) and "+
				"the CI config for a test target or script. If no test suite exists, or you cannot verify one, "+
				"reply the single word NONE. Never guess and never copy an example: a command you cannot verify "+
				"is worse than NONE. Reply with EXACTLY one line: either that command, or the single word NONE. "+
				"No explanation, no code fences.")
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
		if text == "" && selected == "aider" {
			// Aider's raw stdout is all banner chrome, and its command-shaped
			// lines ("Aider v0.86.2") sit above the reply and win the shape
			// scan — turning a correct NONE reply into `sh -c 'Aider v0.86.2'`
			// (wa-termo 2026-09-26). Take the reply from aider's own
			// chat-history file instead, which records it chrome-free — but
			// only from the bytes this round appended (see the snapshot above),
			// so a repo-planted history cannot forge the reply.
			if hist, err := os.ReadFile(histPath); err == nil && int64(len(hist)) > histLen {
				text = lastAiderReply(string(hist[histLen:]))
			}
		}
		if text == "" {
			// Plain-text CLI without a history file (opencode, or an older
			// aider run) or a parse miss: the raw stdout is the reply.
			// ponytail: opencode keeps the stdout fallback — no reply channel
			// is wired for it, so letter-initial chrome could still shadow its
			// reply here.
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
		if !probeTestCommand(ctx, worktreePath, cmd) {
			// The declared runner or target does not exist — fabrication,
			// not a red suite. One re-discovery round, then NONE.
			activityLogger(ctx).Info("Discovery probe failed",
				"Attempt", attempt+1, "Command", cmd, "Worktree", worktreePath)
			continue
		}
		return []string{"sh", "-c", cmd}, nil
	}
	// Both rounds answered with a runner that does not exist: the
	// declaration was fabricated, and the repo is treated as suite-less —
	// never gated on a command nothing can run.
	return nil, ErrNoSuite
}

// probeTestCommand verifies a discovery candidate against the repository
// cheaply: a `make …` candidate is dry-run probed (`make -n` inserted
// before its original arguments, so -C dir, -f file, variable assignments,
// and multi-target forms resolve the same rules the real run would), every
// other candidate by resolving its runner from the worktree the way a
// shell would (`command -v` for bare names, an executability check for
// paths). False means the declared runner or target does not exist — a
// discovery error (fabrication), not a suite failure, so the caller
// retries the round rather than gating on it.
//
// The probe does run repo-controlled code once: `make -n` expands
// parse-time `$(shell …)` calls in the Makefile, and a make candidate
// whose runner is itself a repo-owned file named make executes that file
// directly — so a planted repo can execute shell even for a candidate
// that is then rejected (probe-fail → NONE runs no suite). ponytail: the
// probe runs unjailed on the host, mirroring RunTestSuiteActivity's
// unjailed `sh -c` suite execution — the only delta is that
// rejected-candidate window, which is why the probe is not jail-wrapped.
// Expansion-led candidates ("$RUN_ALL", "$(make test)") probe as
// nonexistent and degrade to the safe-side retry, never to a wrong gate.
func probeTestCommand(ctx context.Context, worktreePath, cmd string) bool {
	fields := strings.Fields(cmd)
	if len(fields) == 0 {
		return false
	}
	if filepath.Base(fields[0]) == "make" {
		probe := exec.CommandContext(ctx, fields[0], append([]string{"-n"}, fields[1:]...)...)
		probe.Dir = worktreePath
		return probe.Run() == nil
	}
	return commandExists(ctx, worktreePath, fields[0])
}

// commandExists resolves runner from dir the way a shell would: a PATH
// lookup for bare names, an executability check for paths — with a
// leading ~ expanded against $HOME first, since the suite's own `sh -c`
// performs tilde expansion and a literal probe would declare a valid
// ~/bin/run-tests suite-less; the mode bit is still required explicitly
// (dash's `command -v` accepts a non-executable explicit path, rc=0), so
// a runner that lost its +x is a real "Permission denied" baseline and
// fails here.
func commandExists(ctx context.Context, dir, runner string) bool {
	script := `command -v "$1"`
	if strings.ContainsRune(runner, '/') {
		script = `case "$1" in "~"*) p="$HOME${1#?}";; *) p="$1";; esac; [ -x "$p" ]`
	}
	probe := exec.CommandContext(ctx, "sh", "-c", script, "sh", runner)
	probe.Dir = dir
	return probe.Run() == nil
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

// lastAiderReply extracts the model's most recent reply from aider's
// chat-history markdown: the text after the last `#### <prompt>` heading,
// with the `> …` chrome lines (banner echo, the `Tokens:` tally) dropped.
// The file accumulates one heading + reply pair per round, so scanning to
// the last heading yields the latest round's answer. Format per the
// aider-probe sample of 2026-09-25 (v0.86.2); an unparsable file yields "".
func lastAiderReply(history string) string {
	var reply []string
	inReply := false
	for line := range strings.SplitSeq(history, "\n") {
		if strings.HasPrefix(line, "#### ") {
			reply, inReply = nil, true
			continue
		}
		if !inReply || strings.HasPrefix(line, ">") {
			continue
		}
		reply = append(reply, line)
	}
	return strings.TrimSpace(strings.Join(reply, "\n"))
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
