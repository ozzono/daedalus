package runstart

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/workflows"
)

// gitRun runs one git command in dir with the repo-override env scrubbed
// (the same hygiene the production checks apply) and fails the test on a
// non-zero exit.
func gitRun(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	cmd.Env = EnvWithoutGitRepoOverrides(os.Environ())
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v in %s: %v: %s", args, dir, err, out)
	}
	return string(out)
}

// gitRepo builds a committed throwaway repository on branch named by
// -b and returns its path and that branch's name.
func gitRepo(t *testing.T, branch string) string {
	t.Helper()
	dir := t.TempDir()
	gitRun(t, dir, "init", "-b", branch)
	gitRun(t, dir, "config", "user.email", "t@example.com")
	gitRun(t, dir, "config", "user.name", "t")
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitRun(t, dir, "add", ".")
	gitRun(t, dir, "commit", "-m", "init")
	return dir
}

// TestIssueIDGate pins the submit-path charset: the token set passes
// (including the empty id, which the repo/issue required checks own) and
// everything a Temporal visibility filter literal cannot carry is named in
// the refusal — the gate runs before any side effect, so a bad id must
// never cost a repo check or a daemon dial.
func TestIssueIDGate(t *testing.T) {
	ok := []string{"iss1", "ISS-501", "a.b_c-d", ""}
	for _, id := range ok {
		if err := IssueIDGate(id); err != nil {
			t.Errorf("IssueIDGate(%q) = %v, want nil", id, err)
		}
	}
	for _, id := range []string{"has space", "tick'", "semi;", "sl/ash"} {
		err := IssueIDGate(id)
		if err == nil {
			t.Errorf("IssueIDGate(%q) = nil, want a refusal", id)
			continue
		}
		if !strings.Contains(err.Error(), "not allowed") {
			t.Errorf("IssueIDGate(%q) = %v, want the charset diagnostic", id, err)
		}
	}
}

// TestEnvWithoutGitRepoOverrides pins the scrub: exactly the four variables
// that redirect git's repository discovery are dropped, everything else —
// order included — survives.
func TestEnvWithoutGitRepoOverrides(t *testing.T) {
	in := []string{
		"PATH=/bin",
		"GIT_DIR=/elsewhere/.git",
		"GIT_WORK_TREE=/elsewhere",
		"HOME=/home/t",
		"GIT_INDEX_FILE=/elsewhere/.git/index",
		"GIT_CEILING_DIRECTORIES=/home/t",
	}
	got := EnvWithoutGitRepoOverrides(in)
	want := []string{"PATH=/bin", "HOME=/home/t"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("EnvWithoutGitRepoOverrides() = %v, want %v", got, want)
	}
}

// TestSharedTestQueue pins the always-concrete contract: the input field is
// never nil, so the run records the starting config's routing instead of
// falling back at schedule time.
func TestSharedTestQueue(t *testing.T) {
	if got := SharedTestQueue(config.Config{}); got == nil || !*got {
		t.Errorf("SharedTestQueue(default) = %v, want non-nil true", got)
	}
	off := false
	if got := SharedTestQueue(config.Config{SharedTestQueue: &off}); got == nil || *got {
		t.Errorf("SharedTestQueue(false) = %v, want non-nil false", got)
	}
}

// TestResolveRepoPath pins the process-boundary normalization: the path
// leaves absolute (the worker daemon's cwd is wherever it was started), a
// non-directory and a non-repository are refused with diagnostics naming
// the input, and an exported GIT_DIR/GIT_CEILING_DIRECTORIES cannot steer
// the check away from the directory on disk.
func TestResolveRepoPath(t *testing.T) {
	repo := gitRepo(t, "main")

	t.Run("a relative path resolves against the process cwd", func(t *testing.T) {
		t.Chdir(repo)
		got, err := ResolveRepoPath(".")
		if err != nil {
			t.Fatalf("ResolveRepoPath(.): %v", err)
		}
		if got != repo {
			t.Errorf("ResolveRepoPath(.) = %q, want %q", got, repo)
		}
	})

	t.Run("a missing directory is refused", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "nope")
		_, err := ResolveRepoPath(missing)
		if err == nil || !strings.Contains(err.Error(), "not a directory") {
			t.Errorf("ResolveRepoPath(%s) = %v, want the not-a-directory refusal", missing, err)
		}
	})

	t.Run("a non-repository directory is refused", func(t *testing.T) {
		plain := t.TempDir()
		_, err := ResolveRepoPath(plain)
		if err == nil || !strings.Contains(err.Error(), "not a git repository") {
			t.Errorf("ResolveRepoPath(%s) = %v, want the not-a-repository refusal", plain, err)
		}
	})

	t.Run("an exported git override cannot redirect the check", func(t *testing.T) {
		t.Setenv("GIT_DIR", "/nonexistent/.git")
		t.Setenv("GIT_CEILING_DIRECTORIES", repo)
		got, err := ResolveRepoPath(repo)
		if err != nil {
			t.Fatalf("ResolveRepoPath(%s) with GIT_DIR set: %v", repo, err)
		}
		if got != repo {
			t.Errorf("ResolveRepoPath(%s) = %q", repo, got)
		}
	})
}

// TestRepoStatus pins the dirty-tree probe both clean-repo gates read:
// staged and untracked changes alike count (the repo state branches and
// worktrees derive from), a clean tree reports nothing, and an unreadable
// repo is an error, not a silent pass.
func TestRepoStatus(t *testing.T) {
	repo := gitRepo(t, "main")

	dirty, status, err := RepoStatus(repo)
	if err != nil || dirty || status != "" {
		t.Errorf("RepoStatus(clean) = %v, %q, %v; want false, \"\", nil", dirty, status, err)
	}

	if err := os.WriteFile(filepath.Join(repo, "untracked.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	dirty, status, err = RepoStatus(repo)
	if err != nil || !dirty {
		t.Fatalf("RepoStatus(untracked) = %v, %q, %v; want dirty", dirty, status, err)
	}
	if !strings.Contains(status, "untracked.txt") {
		t.Errorf("RepoStatus(untracked) status = %q, want it to name the file", status)
	}

	if err := os.WriteFile(filepath.Join(repo, "staged.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitRun(t, repo, "add", "staged.txt")
	dirty, _, err = RepoStatus(repo)
	if err != nil || !dirty {
		t.Errorf("RepoStatus(staged) = %v, %v; want dirty", dirty, err)
	}

	if _, _, err := RepoStatus(t.TempDir()); err == nil {
		t.Error("RepoStatus(non-repo) = nil error, want one")
	}
}

// TestValidate pins the side-effect-free submit gates: the issue-id
// charset, the agent-vs-wire conflict on the effective agent (override
// first, config default otherwise), and the registry lookup with its
// available-flows diagnostic.
func TestValidate(t *testing.T) {
	base := config.Config{Agent: "claude"}
	openaiOnly := config.Config{Agent: "claude", OpenAI: config.OpenAIConfig{URL: "http://o"}}

	cases := []struct {
		name    string
		cfg     config.Config
		req     Request
		wantErr string
	}{
		{"a plain valid submission", base, Request{WorkflowName: "feature-dev", IssueID: "iss1"}, ""},
		{"a bad issue id fails before everything", base, Request{WorkflowName: "feature-dev", IssueID: "a b"}, "not allowed"},
		{"an unknown workflow names the registry", base, Request{WorkflowName: "nope", IssueID: "iss1"}, `unknown workflow "nope"`},
		{"the available flows are listed", base, Request{WorkflowName: "nope", IssueID: "iss1"}, "feature-dev"},
		{"an agent the config cannot serve is refused", openaiOnly, Request{WorkflowName: "feature-dev", IssueID: "iss1", Agent: "claude"}, "cannot be served"},
		{"the config's default agent is checked when no override is set", openaiOnly, Request{WorkflowName: "feature-dev", IssueID: "iss1"}, "cannot be served"},
		{"an override that fixes the conflict passes", openaiOnly, Request{WorkflowName: "feature-dev", IssueID: "iss1", Agent: "pi"}, ""},
	}
	for _, c := range cases {
		err := Validate(c.cfg, c.req)
		if c.wantErr == "" {
			if err != nil {
				t.Errorf("%s: Validate() = %v, want nil", c.name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), c.wantErr) {
			t.Errorf("%s: Validate() = %v, want containing %q", c.name, err, c.wantErr)
		}
	}
}

// fakeFrontend is an in-process stand-in for the Temporal frontend Start
// dispatches against: it captures StartWorkflowExecution requests, serves
// canned DescribeWorkflowExecution answers by workflow id, and serves
// canned histories by workflow id (the SDK polls history with an empty run
// id when a caller names the workflow alone, so run-id keying cannot serve
// the dependency-outcome read). Anything else fails the RPC.
type fakeFrontend struct {
	workflowservice.UnimplementedWorkflowServiceServer

	mu        sync.Mutex
	starts    []*workflowservice.StartWorkflowExecutionRequest
	describes map[string]*workflowservice.DescribeWorkflowExecutionResponse
	histories map[string][]*historypb.HistoryEvent
	listRows  []*workflowpb.WorkflowExecutionInfo
	listErr   error
	describeN int
	// describeFailure, when set, is answered for every describe — the
	// server-is-there-but-failing face a dead port cannot produce (Dial
	// fails first there).
	describeFailure error
	servedHist      map[string]int
}

func (f *fakeFrontend) GetSystemInfo(_ context.Context, _ *workflowservice.GetSystemInfoRequest) (*workflowservice.GetSystemInfoResponse, error) {
	return &workflowservice.GetSystemInfoResponse{Capabilities: &workflowservice.GetSystemInfoResponse_Capabilities{}}, nil
}

func (f *fakeFrontend) StartWorkflowExecution(_ context.Context, req *workflowservice.StartWorkflowExecutionRequest) (*workflowservice.StartWorkflowExecutionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.starts = append(f.starts, req)
	return &workflowservice.StartWorkflowExecutionResponse{RunId: "run-1"}, nil
}

func (f *fakeFrontend) DescribeWorkflowExecution(_ context.Context, req *workflowservice.DescribeWorkflowExecutionRequest) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.describeN++
	if f.describeFailure != nil {
		return nil, f.describeFailure
	}
	resp, ok := f.describes[req.GetExecution().GetWorkflowId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "workflow "+req.GetExecution().GetWorkflowId()+" not found")
	}
	return resp, nil
}

func (f *fakeFrontend) GetWorkflowExecutionHistory(_ context.Context, req *workflowservice.GetWorkflowExecutionHistoryRequest) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := req.GetExecution().GetWorkflowId()
	events, ok := f.histories[id]
	if !ok {
		return nil, status.Error(codes.NotFound, "no history for "+id)
	}
	if f.servedHist == nil {
		f.servedHist = map[string]int{}
	}
	f.servedHist[id]++
	return &workflowservice.GetWorkflowExecutionHistoryResponse{
		History: &historypb.History{Events: events},
	}, nil
}

func (f *fakeFrontend) ListWorkflowExecutions(_ context.Context, req *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return &workflowservice.ListWorkflowExecutionsResponse{Executions: f.listRows}, nil
}

// startFake serves fake on a loopback port and returns the config pointing
// at it; Dial connects eagerly, so the server listens before any caller
// runs.
func startFake(t *testing.T, fake *fakeFrontend) config.Config {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("bind the fake frontend: %v", err)
	}
	srv := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return config.Config{Temporal: config.TemporalConfig{Host: lis.Addr().String(), TaskQueue: "startq"}}
}

// dispatchInput decodes the PipelineInput a captured start carried.
func dispatchInput(t *testing.T, req *workflowservice.StartWorkflowExecutionRequest) workflows.PipelineInput {
	t.Helper()
	var in workflows.PipelineInput
	ps := req.GetInput().GetPayloads()
	if len(ps) != 1 {
		t.Fatalf("start carried %d input payloads, want 1", len(ps))
	}
	if err := converter.GetDefaultDataConverter().FromPayload(ps[0], &in); err != nil {
		t.Fatalf("decode the captured input: %v", err)
	}
	return in
}

// TestStartDispatch pins the seam's dispatch: the workflow id derivation
// (the default flow keeps its legacy unscoped id, any other flow carries
// its name), the input the workflow receives (absolute repo path, the
// flow, the routing, the override and prefix), the reuse policy that lets
// a succeeded issue re-run, and the ordering of the pre-dispatch refusals —
// a validation failure or a dirty tree must reach the server as zero
// starts. It also pins the dependency chain's submit half: the absent-id
// usage error surfaces as ErrDependencyNotFound, and the release-with-
// fallback default resolves the invocation branch into the input at submit.
func TestStartDispatch(t *testing.T) {
	valid := func(repo string) Request {
		return Request{
			WorkflowName: "feature-dev",
			RepoPath:     repo,
			IssueID:      "iss1",
			Prompt:       "do it",
			Folders:      []string{"docs"},
			BranchPrefix: "px/",
		}
	}

	t.Run("the default flow keeps its legacy unscoped id and carries the input", func(t *testing.T) {
		repo := gitRepo(t, "main")
		fake := &fakeFrontend{}
		cfg := startFake(t, fake)

		run, err := Start(cfg, valid(repo))
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if run.WorkflowID != "startq-iss1" || run.RunID != "run-1" {
			t.Errorf("Start = %+v, want workflow startq-iss1 / run run-1", run)
		}
		if len(fake.starts) != 1 {
			t.Fatalf("the fake served %d starts, want 1", len(fake.starts))
		}
		req := fake.starts[0]
		if req.GetWorkflowId() != "startq-iss1" {
			t.Errorf("workflow id = %q, want startq-iss1", req.GetWorkflowId())
		}
		if got := req.GetTaskQueue().GetName(); got != "startq" {
			t.Errorf("task queue = %q, want startq", got)
		}
		if want := workflows.WorkflowTypeName(workflows.FeatureDevWorkflow); req.GetWorkflowType().GetName() != want {
			t.Errorf("workflow type = %q, want %q", req.GetWorkflowType().GetName(), want)
		}
		if req.GetWorkflowIdReusePolicy() != enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE {
			t.Errorf("reuse policy = %v, want ALLOW_DUPLICATE (a succeeded issue re-runs)", req.GetWorkflowIdReusePolicy())
		}
		in := dispatchInput(t, req)
		if in.RepoPath != repo {
			t.Errorf("input RepoPath = %q, want the absolute %q", in.RepoPath, repo)
		}
		if in.IssueID != "iss1" || in.Prompt != "do it" || in.Flow != "feature-dev" ||
			in.BranchPrefix != "px/" || strings.Join(in.Folders, ",") != "docs" {
			t.Errorf("input = %+v, want the submission's own fields", in)
		}
		if in.SharedTestQueue == nil || *in.SharedTestQueue != cfg.SharesTestQueue() {
			t.Errorf("input SharedTestQueue = %v, want the config's routing (%v), never nil", in.SharedTestQueue, cfg.SharesTestQueue())
		}
		if in.SkipPreflight {
			t.Errorf("input SkipPreflight = true on a plain submission, want the default false (the gate keeps its park)")
		}
	})

	t.Run("the preflight opt-out rides the dispatch input", func(t *testing.T) {
		repo := gitRepo(t, "main")
		fake := &fakeFrontend{}
		cfg := startFake(t, fake)

		req := valid(repo)
		req.SkipPreflight = true
		if _, err := Start(cfg, req); err != nil {
			t.Fatalf("Start: %v", err)
		}
		if in := dispatchInput(t, fake.starts[0]); !in.SkipPreflight {
			t.Errorf("input SkipPreflight = false, want the submission's opt-out carried into the workflow input")
		}
	})

	t.Run("a non-default flow scopes the id with its name", func(t *testing.T) {
		repo := gitRepo(t, "main")
		fake := &fakeFrontend{}
		cfg := startFake(t, fake)

		req := valid(repo)
		req.WorkflowName = "slim"
		if _, err := Start(cfg, req); err != nil {
			t.Fatalf("Start: %v", err)
		}
		if got := fake.starts[0].GetWorkflowId(); got != "startq-slim-iss1" {
			t.Errorf("workflow id = %q, want startq-slim-iss1", got)
		}
		if in := dispatchInput(t, fake.starts[0]); in.Flow != "slim" {
			t.Errorf("input Flow = %q, want slim", in.Flow)
		}
	})

	t.Run("a dirty repo is refused before anything is dispatched", func(t *testing.T) {
		repo := gitRepo(t, "main")
		if err := os.WriteFile(filepath.Join(repo, "wip.txt"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		fake := &fakeFrontend{}
		cfg := startFake(t, fake)

		_, err := Start(cfg, valid(repo))
		if err == nil || !strings.Contains(err.Error(), "uncommitted changes") {
			t.Fatalf("Start(dirty repo) = %v, want the clean-repo refusal", err)
		}
		if len(fake.starts) != 0 {
			t.Errorf("the dirty refusal dispatched %d starts, want 0", len(fake.starts))
		}

		// The CLI's affirmed confirm rides DirtyOK across the seam: the
		// same dirty tree dispatches when the operator already answered.
		req := valid(repo)
		req.DirtyOK = true
		if _, err := Start(cfg, req); err != nil {
			t.Errorf("Start(dirty repo, DirtyOK) = %v, want the dispatch to proceed", err)
		}
		if len(fake.starts) != 1 {
			t.Errorf("the affirmed dirty submission dispatched %d starts, want 1", len(fake.starts))
		}
	})

	t.Run("a validation failure never dials the server", func(t *testing.T) {
		repo := gitRepo(t, "main")
		fake := &fakeFrontend{}
		cfg := startFake(t, fake)

		req := valid(repo)
		req.IssueID = "a b"
		if _, err := Start(cfg, req); err == nil {
			t.Fatal("Start(bad issue id) = nil error, want a refusal")
		}
		if len(fake.starts) != 0 {
			t.Errorf("the invalid submission dispatched %d starts, want 0", len(fake.starts))
		}
	})

	t.Run("an absent dependency is the usage error", func(t *testing.T) {
		repo := gitRepo(t, "main")
		fake := &fakeFrontend{}
		cfg := startFake(t, fake)

		req := valid(repo)
		req.Depends = "startq-missing"
		_, err := Start(cfg, req)
		if !errors.Is(err, ErrDependencyNotFound) {
			t.Fatalf("Start(missing dep) = %v, want ErrDependencyNotFound", err)
		}
		if len(fake.starts) != 0 {
			t.Errorf("the broken chain dispatched %d starts, want 0", len(fake.starts))
		}
	})

	t.Run("the release fallback resolves the invocation branch at submit", func(t *testing.T) {
		repo := gitRepo(t, "main")
		fake := &fakeFrontend{describes: map[string]*workflowservice.DescribeWorkflowExecutionResponse{
			"startq-dep": describeRunning("startq-dep", "startq"),
		}}
		cfg := startFake(t, fake)

		req := valid(repo)
		req.Depends = "startq-dep"
		if _, err := Start(cfg, req); err != nil {
			t.Fatalf("Start(dep): %v", err)
		}
		in := dispatchInput(t, fake.starts[0])
		if in.Dependency.FallbackBranch != "main" {
			t.Errorf("input fallback branch = %q, want the invocation branch main", in.Dependency.FallbackBranch)
		}
	})

	t.Run("an explicit fallback branch is verified before dispatch", func(t *testing.T) {
		repo := gitRepo(t, "main")
		fake := &fakeFrontend{}
		cfg := startFake(t, fake)
		cfg.Dependency.FallbackBranch = "no-such-branch"

		req := valid(repo)
		req.Depends = "startq-other"
		fake.describes = map[string]*workflowservice.DescribeWorkflowExecutionResponse{
			"startq-other": describeRunning("startq-other", "startq"),
		}
		_, err := Start(cfg, req)
		if err == nil || !strings.Contains(err.Error(), "no such branch") {
			t.Fatalf("Start(bad fallback branch) = %v, want the verification refusal", err)
		}
		if len(fake.starts) != 0 {
			t.Errorf("the unverifiable chain dispatched %d starts, want 0", len(fake.starts))
		}
	})

	t.Run("an in-repo grant rides as scope, never a mount", func(t *testing.T) {
		repo := gitRepo(t, "main")
		// An empty untracked directory keeps the clean-repo gate silent.
		if err := os.MkdirAll(filepath.Join(repo, "specs"), 0o755); err != nil {
			t.Fatal(err)
		}
		fake := &fakeFrontend{}
		cfg := startFake(t, fake)

		req := valid(repo)
		req.WorkflowName = "investigate" // a flow with an allowed-path policy
		req.Folders = []string{filepath.Join(repo, "specs")}
		if _, err := Start(cfg, req); err != nil {
			t.Fatalf("Start: %v", err)
		}
		in := dispatchInput(t, fake.starts[0])
		if len(in.Folders) != 0 {
			t.Errorf("input Folders = %v, want none — mounting the host copy would bypass the worktree commit model", in.Folders)
		}
		if want := []string{"*.md", "docs/", "specs/"}; !slices.Equal(in.AllowedPaths, want) {
			t.Errorf("input AllowedPaths = %v, want the flow's policy extended by the grant's prefix %v", in.AllowedPaths, want)
		}
	})

	t.Run("a repo-root grant is refused before anything is dispatched", func(t *testing.T) {
		repo := gitRepo(t, "main")
		fake := &fakeFrontend{}
		cfg := startFake(t, fake)

		req := valid(repo)
		req.Folders = []string{repo}
		_, err := Start(cfg, req)
		if err == nil || !strings.Contains(err.Error(), "is the repo root") {
			t.Fatalf("Start(repo-root grant) = %v, want the repo-root refusal", err)
		}
		if len(fake.starts) != 0 {
			t.Errorf("the repo-root refusal dispatched %d starts, want 0", len(fake.starts))
		}
	})

	t.Run("a -f brief dropped in the repo root names the brief in the refusal", func(t *testing.T) {
		repo := gitRepo(t, "main")
		fake := &fakeFrontend{}
		cfg := startFake(t, fake)

		req := valid(repo)
		req.TaskFile = filepath.Join(repo, "brief.md") // the CLI's auto-grant of its folder is the repo root
		req.Folders = []string{repo}
		_, err := Start(cfg, req)
		if err == nil || !strings.Contains(err.Error(), req.TaskFile) || !strings.Contains(err.Error(), "outside the repo") || !strings.Contains(err.Error(), "subfolder") {
			t.Fatalf("Start(brief at repo root) = %v, want the refusal naming the brief and both placements", err)
		}
		if len(fake.starts) != 0 {
			t.Errorf("the brief-origin refusal dispatched %d starts, want 0", len(fake.starts))
		}
	})
}

// TestScopeFolderGrants pins the submit-time split of folder grants against
// the repo root: an in-repo grant is a scope grant — it extends a flow's
// allowed-path policy by the grant's repo-relative directory prefix and
// never mounts (the worktree already carries the files, and a mount would
// let the agent edit the host repo past the worktree commit model); a grant
// outside the repo keeps the mount and extends nothing; a grant equal to
// the repo root is refused — an explicit --folder spelling with the plain
// message, a -f brief sitting in the repo root with one naming the brief
// and the placements that work (the taskFile argument only ever rewords
// this refusal); nested, duplicate, and trailing-slash
// spellings are left as-is; a policy-free flow is never seeded (an empty
// allowed set means unrestricted, and seeding it would strand every write
// outside the grant); and the caller's policy slice is never mutated in
// place — it aliases the shared Registry spec.
func TestScopeFolderGrants(t *testing.T) {
	repo := t.TempDir()
	policy := []string{"*.md", "docs/"}

	t.Run("an in-repo grant extends the policy and mounts nothing", func(t *testing.T) {
		mounts, extended, err := scopeFolderGrants(repo, "", []string{filepath.Join(repo, "specs")}, policy)
		if err != nil {
			t.Fatalf("scopeFolderGrants: %v", err)
		}
		if mounts != nil {
			t.Errorf("mounts = %v, want none — the worktree carries the grant's files", mounts)
		}
		if want := []string{"*.md", "docs/", "specs/"}; !slices.Equal(extended, want) {
			t.Errorf("extended = %v, want %v", extended, want)
		}
		if want := []string{"*.md", "docs/"}; !slices.Equal(policy, want) {
			t.Errorf("the caller's policy was mutated in place: %v, want %v", policy, want)
		}
	})

	t.Run("a trailing-slash spelling normalizes to the same prefix", func(t *testing.T) {
		_, extended, err := scopeFolderGrants(repo, "", []string{filepath.Join(repo, "specs") + "/"}, policy)
		if err != nil {
			t.Fatalf("scopeFolderGrants: %v", err)
		}
		if want := []string{"*.md", "docs/", "specs/"}; !slices.Equal(extended, want) {
			t.Errorf("extended = %v, want %v (no doubled or uncleaned prefix)", extended, want)
		}
	})

	t.Run("a grant outside the repo keeps the mount and extends nothing", func(t *testing.T) {
		outside := filepath.Join(t.TempDir(), "shared")
		mounts, extended, err := scopeFolderGrants(repo, "", []string{outside}, policy)
		if err != nil {
			t.Fatalf("scopeFolderGrants: %v", err)
		}
		if !slices.Equal(mounts, []string{outside}) {
			t.Errorf("mounts = %v, want [%s]", mounts, outside)
		}
		if !slices.Equal(extended, policy) {
			t.Errorf("extended = %v, want the flow's policy unchanged %v", extended, policy)
		}
	})

	t.Run("mixed grants split by location in submission order", func(t *testing.T) {
		outside := filepath.Join(t.TempDir(), "notes")
		mounts, extended, err := scopeFolderGrants(repo, "", []string{outside, filepath.Join(repo, "specs")}, policy)
		if err != nil {
			t.Fatalf("scopeFolderGrants: %v", err)
		}
		if !slices.Equal(mounts, []string{outside}) {
			t.Errorf("mounts = %v, want only the out-of-repo grant %s", mounts, outside)
		}
		if want := []string{"*.md", "docs/", "specs/"}; !slices.Equal(extended, want) {
			t.Errorf("extended = %v, want %v", extended, want)
		}
	})

	t.Run("the repo root is refused", func(t *testing.T) {
		for _, spelling := range []string{repo, repo + "/"} {
			mounts, extended, err := scopeFolderGrants(repo, "", []string{spelling}, policy)
			if err == nil || !strings.Contains(err.Error(), "is the repo root") || !strings.Contains(err.Error(), "grant a folder inside it instead") {
				t.Errorf("scopeFolderGrants(%s) err = %v, want the explicit root refusal", spelling, err)
			}
			if mounts != nil || extended != nil {
				t.Errorf("scopeFolderGrants(%s) = %v, %v, want nothing back on a refusal", spelling, mounts, extended)
			}
		}
	})

	t.Run("a -f brief in the repo root names itself and both placements", func(t *testing.T) {
		brief := filepath.Join(repo, "brief.md")
		for _, spelling := range []string{repo, repo + "/"} {
			mounts, extended, err := scopeFolderGrants(repo, brief, []string{spelling}, policy)
			for _, want := range []string{"-f " + brief, "its folder " + repo + " is the repo root", "outside the repo", "subfolder"} {
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Errorf("scopeFolderGrants(%s) err = %v, want the brief-origin refusal naming %q", spelling, err, want)
				}
			}
			if mounts != nil || extended != nil {
				t.Errorf("scopeFolderGrants(%s) = %v, %v, want nothing back on a refusal", spelling, mounts, extended)
			}
		}
	})

	t.Run("a brief away from the root keeps the explicit refusal plain", func(t *testing.T) {
		brief := filepath.Join(repo, "specs", "brief.md")
		_, _, err := scopeFolderGrants(repo, brief, []string{repo}, policy)
		if err == nil || !strings.Contains(err.Error(), "grant a folder inside it instead") || strings.Contains(err.Error(), "-f ") {
			t.Errorf("scopeFolderGrants(brief in a subfolder) err = %v, want the plain explicit refusal — only a brief at the root rewords it", err)
		}
	})

	t.Run("a brief outside the repo mounts its folder like any grant", func(t *testing.T) {
		outside := filepath.Join(t.TempDir(), "shared")
		brief := filepath.Join(outside, "brief.md")
		mounts, extended, err := scopeFolderGrants(repo, brief, []string{outside}, policy)
		if err != nil {
			t.Fatalf("scopeFolderGrants: %v", err)
		}
		if !slices.Equal(mounts, []string{outside}) {
			t.Errorf("mounts = %v, want [%s] — the placement the brief-origin refusal names first", mounts, outside)
		}
		if !slices.Equal(extended, policy) {
			t.Errorf("extended = %v, want the flow's policy unchanged %v", extended, policy)
		}
	})

	t.Run("a brief in a repo subfolder extends the scope like any grant", func(t *testing.T) {
		brief := filepath.Join(repo, "notes", "brief.md")
		mounts, extended, err := scopeFolderGrants(repo, brief, []string{filepath.Join(repo, "notes")}, policy)
		if err != nil {
			t.Fatalf("scopeFolderGrants: %v", err)
		}
		if mounts != nil {
			t.Errorf("mounts = %v, want none — the subfolder grant rides as scope", mounts)
		}
		if want := []string{"*.md", "docs/", "notes/"}; !slices.Equal(extended, want) {
			t.Errorf("extended = %v, want %v — the placement the brief-origin refusal names second", extended, want)
		}
	})

	t.Run("nested and duplicate grants are left as-is", func(t *testing.T) {
		mounts, extended, err := scopeFolderGrants(repo, "", []string{
			filepath.Join(repo, "specs"),
			filepath.Join(repo, "specs", "deep"),
			filepath.Join(repo, "specs"),
		}, policy)
		if err != nil {
			t.Fatalf("scopeFolderGrants: %v", err)
		}
		if mounts != nil {
			t.Errorf("mounts = %v, want none", mounts)
		}
		if want := []string{"*.md", "docs/", "specs/", "specs/deep/", "specs/"}; !slices.Equal(extended, want) {
			t.Errorf("extended = %v, want the prefixes kept verbatim %v — they cannot shadow, only re-match", extended, want)
		}
	})

	t.Run("a policy-free flow is never seeded", func(t *testing.T) {
		mounts, extended, err := scopeFolderGrants(repo, "", []string{filepath.Join(repo, "specs")}, nil)
		if err != nil {
			t.Fatalf("scopeFolderGrants: %v", err)
		}
		if mounts != nil {
			t.Errorf("mounts = %v, want none", mounts)
		}
		if extended != nil {
			t.Errorf("extended = %v, want nil — an empty allowed set means unrestricted, and seeding it would strand every write outside the grant", extended)
		}
	})

	t.Run("no grants is a no-op", func(t *testing.T) {
		mounts, extended, err := scopeFolderGrants(repo, "", nil, policy)
		if err != nil {
			t.Fatalf("scopeFolderGrants: %v", err)
		}
		if mounts != nil || !slices.Equal(extended, policy) {
			t.Errorf("scopeFolderGrants with no grants = %v, %v, want nil mounts and the policy unchanged", mounts, extended)
		}
	})

	t.Run("a relative grant resolves against the process cwd", func(t *testing.T) {
		t.Chdir(repo)
		mounts, extended, err := scopeFolderGrants(repo, "", []string{"specs"}, policy)
		if err != nil {
			t.Fatalf("scopeFolderGrants: %v", err)
		}
		if mounts != nil {
			t.Errorf("mounts = %v, want none — the relative grant resolves inside the repo", mounts)
		}
		if want := []string{"*.md", "docs/", "specs/"}; !slices.Equal(extended, want) {
			t.Errorf("extended = %v, want %v", extended, want)
		}
	})

	t.Run("a parent of the repo is a mount, not a refusal", func(t *testing.T) {
		parent := filepath.Dir(repo)
		mounts, _, err := scopeFolderGrants(repo, "", []string{parent}, policy)
		if err != nil {
			t.Fatalf("scopeFolderGrants: %v", err)
		}
		if !slices.Equal(mounts, []string{parent}) {
			t.Errorf("mounts = %v, want [%s] — the .. boundary is a mount like any outside path", mounts, parent)
		}
	})

	t.Run("a traversal spelling that resolves inside is a scope grant", func(t *testing.T) {
		winding := filepath.Join(repo, "..", filepath.Base(repo), "specs")
		mounts, extended, err := scopeFolderGrants(repo, "", []string{winding}, policy)
		if err != nil {
			t.Fatalf("scopeFolderGrants: %v", err)
		}
		if mounts != nil {
			t.Errorf("mounts = %v, want none — the cleaned path is inside the repo", mounts)
		}
		if want := []string{"*.md", "docs/", "specs/"}; !slices.Equal(extended, want) {
			t.Errorf("extended = %v, want %v", extended, want)
		}
	})
}

// describeRunning builds a Describe answer naming id live on queue.
func describeRunning(id, queue string) *workflowservice.DescribeWorkflowExecutionResponse {
	return &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: id + "-run"},
			TaskQueue: queue,
			Status:    enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
			StartTime: timestamppb.New(time.Now().Add(-time.Hour)),
		},
	}
}

// TestPreflightDependency pins the submit-time dependency gate's decision
// table: a self-reference and a genuinely absent id are usage errors (the
// latter — and only that — as ErrDependencyNotFound); a foreign-queue id is
// refused; anything in flight (running, continued, paused) passes; an
// approved completion passes; a terminal stop without approval is the
// broken-chain refusal unless the config is in release posture; a parked
// completion is broken in gate posture even though Temporal calls it
// Completed; and a server failure that is not absence comes back plainly,
// never dressed as the usage error.
func TestPreflightDependency(t *testing.T) {
	parkedResult := workflows.ParkedResult("reviewer halted the run")

	// greenCloseEvent builds the close event of a workflow completing with
	// out as its result — the outcome read resolves through it.
	greenCloseEvent := func(t *testing.T, out string) *historypb.HistoryEvent {
		t.Helper()
		payload, err := converter.GetDefaultDataConverter().ToPayload(out)
		if err != nil {
			t.Fatalf("encode result: %v", err)
		}
		return &historypb.HistoryEvent{
			EventId:   2,
			EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
				WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
					Result: &commonpb.Payloads{Payloads: []*commonpb.Payload{payload}},
				},
			},
		}
	}
	describe := func(id, queue string, st enums.WorkflowExecutionStatus) *workflowservice.DescribeWorkflowExecutionResponse {
		return &workflowservice.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
				Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: id + "-run"},
				TaskQueue: queue,
				Status:    st,
			},
		}
	}

	cases := []struct {
		name      string
		dep       string
		depQueue  string
		status    enums.WorkflowExecutionStatus
		result    string
		release   bool // cfg.Dependency.FallbackBranch preset → release posture
		wantErr   string
		wantUsage bool // ErrDependencyNotFound expected
	}{
		{"a running dependency passes", "startq-dep", "startq", enums.WORKFLOW_EXECUTION_STATUS_RUNNING, "", false, "", false},
		{"a continued dependency passes", "startq-dep", "startq", enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, "", false, "", false},
		{"a paused dependency passes", "startq-dep", "startq", enums.WORKFLOW_EXECUTION_STATUS_PAUSED, "", false, "", false},
		{"an approved completion passes", "startq-dep", "startq", enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, "main", false, "", false},
		{"a parked completion is a broken chain in gate posture", "startq-dep", "startq", enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, parkedResult, false, "completed parked", false},
		{"a parked completion releases in release posture", "startq-dep", "startq", enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, parkedResult, true, "", false},
		{"a failed dependency is a broken chain in gate posture", "startq-dep", "startq", enums.WORKFLOW_EXECUTION_STATUS_FAILED, "", false, "Failed", false},
		{"a failed dependency releases in release posture", "startq-dep", "startq", enums.WORKFLOW_EXECUTION_STATUS_FAILED, "", true, "", false},
		{"a canceled dependency is a broken chain in gate posture", "startq-dep", "startq", enums.WORKFLOW_EXECUTION_STATUS_CANCELED, "", false, "Canceled", false},
		{"a foreign-queue dependency is refused", "startq-dep", "otherq", enums.WORKFLOW_EXECUTION_STATUS_RUNNING, "", true, "task queue", false},
		{"an absent dependency is the usage error", "startq-dep", "", 0, "", false, "not found", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fake := &fakeFrontend{describes: map[string]*workflowservice.DescribeWorkflowExecutionResponse{}}
			if c.depQueue != "" || c.status != 0 {
				fake.describes["startq-dep"] = describe("startq-dep", c.depQueue, c.status)
			}
			if c.result != "" {
				fake.histories = map[string][]*historypb.HistoryEvent{
					"startq-dep": {greenCloseEvent(t, c.result)},
				}
			}
			cfg := startFake(t, fake)
			if c.release {
				cfg.Dependency.FallbackBranch = "main"
			}

			err := PreflightDependency(mustClient(t, cfg), cfg, "startq-new", "startq-dep")
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("PreflightDependency() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("PreflightDependency() = %v, want containing %q", err, c.wantErr)
			}
			if c.wantUsage != errors.Is(err, ErrDependencyNotFound) {
				t.Errorf("PreflightDependency() = %v, ErrDependencyNotFound = %v, want %v", err, errors.Is(err, ErrDependencyNotFound), c.wantUsage)
			}
		})
	}

	t.Run("a run cannot depend on itself", func(t *testing.T) {
		fake := &fakeFrontend{}
		cfg := startFake(t, fake)
		err := PreflightDependency(mustClient(t, cfg), cfg, "startq-self", "startq-self")
		if err == nil || !strings.Contains(err.Error(), "own id") {
			t.Fatalf("PreflightDependency(self) = %v, want the self-reference refusal", err)
		}
		if fake.describeN != 0 {
			t.Errorf("the self-refusal issued %d describes, want 0", fake.describeN)
		}
	})

	t.Run("a non-absence describe failure is not the usage error", func(t *testing.T) {
		// Only a genuinely absent id wraps ErrDependencyNotFound; a server
		// that cannot answer (unavailable, throttled) must surface
		// plainly — the check-`daedalus-list` advice needs the server
		// that just failed.
		fake := &fakeFrontend{describeFailure: status.Error(codes.Unavailable, "frontend down")}
		cfg := startFake(t, fake)
		err := PreflightDependency(mustClient(t, cfg), cfg, "startq-new", "startq-dep")
		if err == nil {
			t.Fatal("PreflightDependency(failing server) = nil, want an error")
		}
		if errors.Is(err, ErrDependencyNotFound) {
			t.Errorf("PreflightDependency(failing server) = %v — a dead server is not an absent id", err)
		}
		if !strings.Contains(err.Error(), "describe dependency") {
			t.Errorf("PreflightDependency(failing server) = %v, want the plain describe diagnostic", err)
		}
	})
}

func mustClient(t *testing.T, cfg config.Config) client.Client {
	t.Helper()
	c, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

// TestInvocationBranch pins the fallback default: the branch the run is
// submitted from, with a detached HEAD refused — it names no branch, and
// the error names the config key that fixes it.
func TestInvocationBranch(t *testing.T) {
	repo := gitRepo(t, "main")
	branch, err := InvocationBranch(repo)
	if err != nil || branch != "main" {
		t.Errorf("InvocationBranch(repo) = %q, %v; want main", branch, err)
	}

	gitRun(t, repo, "checkout", "--detach", "HEAD")
	_, err = InvocationBranch(repo)
	if err == nil || !strings.Contains(err.Error(), "detached HEAD") {
		t.Errorf("InvocationBranch(detached) = %v, want the detached-HEAD refusal", err)
	}
}

// TestVerifyBranchExists pins the branch-verification: an existing branch
// passes, a missing name is refused, and a tag by the same name is not a
// branch — the check pins refs/heads/ because a tag or SHA is not a
// worktree base.
func TestVerifyBranchExists(t *testing.T) {
	repo := gitRepo(t, "main")
	gitRun(t, repo, "branch", "feat")

	if err := VerifyBranchExists(repo, "feat"); err != nil {
		t.Errorf("VerifyBranchExists(feat) = %v, want nil", err)
	}
	err := VerifyBranchExists(repo, "nope")
	if err == nil || !strings.Contains(err.Error(), "no such branch") {
		t.Errorf("VerifyBranchExists(nope) = %v, want the no-such-branch refusal", err)
	}
	gitRun(t, repo, "tag", "v1")
	if err := VerifyBranchExists(repo, "v1"); err == nil {
		t.Error("VerifyBranchExists(tag v1) = nil, want a refusal — a tag is not a branch")
	}
}

// startedEventWithFolders builds the history's started event carrying in as
// the pipeline input.
func startedEventWithFolders(t *testing.T, in workflows.PipelineInput) *historypb.HistoryEvent {
	t.Helper()
	payload, err := converter.GetDefaultDataConverter().ToPayload(in)
	if err != nil {
		t.Fatalf("encode pipeline input: %v", err)
	}
	return &historypb.HistoryEvent{
		EventId:   1,
		EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{payload}},
			},
		},
	}
}

// TestWarnFolderOverlap pins the shared-grant alert: an overlap with a
// running pipeline's folders is a warning naming the folder and the other
// run; disjoint grants stay silent; an enumeration failure degrades to
// silence, never an error; and a nil writer is simply no alert.
func TestWarnFolderOverlap(t *testing.T) {
	newFake := func(t *testing.T) (config.Config, *fakeFrontend) {
		fake := &fakeFrontend{
			listRows: []*workflowpb.WorkflowExecutionInfo{{
				Execution: &commonpb.WorkflowExecution{WorkflowId: "startq-other", RunId: "run-other"},
			}},
			histories: map[string][]*historypb.HistoryEvent{
				"startq-other": {startedEventWithFolders(t, workflows.PipelineInput{Folders: []string{"docs"}})},
			},
		}
		return startFake(t, fake), fake
	}

	t.Run("an overlap warns naming the folder and the other run", func(t *testing.T) {
		cfg, _ := newFake(t)
		var buf strings.Builder
		warnFolderOverlap(mustClient(t, cfg), cfg, []string{"docs", "other"}, &buf)
		out := buf.String()
		if !strings.Contains(out, "docs") || !strings.Contains(out, "startq-other") {
			t.Errorf("overlap warning = %q, want it to name the folder and the running pipeline", out)
		}
	})

	t.Run("disjoint grants stay silent", func(t *testing.T) {
		cfg, _ := newFake(t)
		var buf strings.Builder
		warnFolderOverlap(mustClient(t, cfg), cfg, []string{"solo"}, &buf)
		if buf.Len() != 0 {
			t.Errorf("disjoint grants warned %q, want silence", buf.String())
		}
	})

	t.Run("a failed enumeration degrades to silence", func(t *testing.T) {
		cfg, fake := newFake(t)
		fake.listErr = errors.New("visibility down")
		var buf strings.Builder
		warnFolderOverlap(mustClient(t, cfg), cfg, []string{"docs"}, &buf)
		if buf.Len() != 0 {
			t.Errorf("a failed walk warned %q, want silence", buf.String())
		}
	})

	t.Run("a nil writer is no alert and no panic", func(t *testing.T) {
		cfg, _ := newFake(t)
		warnFolderOverlap(mustClient(t, cfg), cfg, []string{"docs"}, nil)
	})
}
