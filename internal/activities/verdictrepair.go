package activities

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/ozzono/daedalus/internal/config"
)

// repairPromotionThreshold is the confidence a classification of APPROVED
// must reach before a marker-less round promotes to approval; below it —
// and for every other verdict — the round continues as changes-requested,
// annotated with the most-likely verdict and confidence. Mutation-checked
// in both directions: a classification at the threshold must promote, one
// below it must not.
const repairPromotionThreshold = 0.9

// repairTimeout bounds one decision-model classify call. A slower endpoint
// fails the repair and the round keeps the pre-repair marker-less behavior:
// the fail-open is the layer's contract, and the strike budget — not a
// retry loop — is its retry policy.
const repairTimeout = 60 * time.Second

// repairReviewVerdict is the NoVerdict repair: it asks the configured
// decision model (verdict_repair.url / model, key optional) to classify the
// reviewer's marker-less output into one of the four markers, annotates the
// result with the most-likely verdict and confidence, and — only for a
// high-confidence APPROVED — promotes the round to approval: the workflow's
// review() decides approval before its verdictless strike accounting, so a
// promoted round ends the loop exactly as a parsed approval would (streaks
// reset, no strike, no strike-budget park discarding the promotion).
// Everything else is fail-open: an unconfigured, budget-starved,
// unreachable, timed-out, or garbage-answering endpoint returns the result
// untouched, byte-identical to the pre-repair behavior (changes-requested,
// whole output as comments, counted by the workflow's verdictless strike
// budget exactly as before — the repair outcome never touches strike
// accounting; only the workflow counts strikes).
func repairReviewVerdict(ctx context.Context, result ReviewResult) ReviewResult {
	logger := activityLogger(ctx)
	if !repairConfigured() {
		// The layer is off (verdict_repair unset): marker-less rounds keep
		// the pre-repair behavior, silently — an off toggle is not a
		// failure worth a log line on every marker-less round.
		return result
	}
	verdict, confidence, ok := classifyReviewVerdict(ctx, result.Comments)
	if !ok {
		logger.Warn("Verdict repair could not classify the marker-less review; keeping it as changes-requested",
			"Verdict", "", "Confidence", 0.0, "Repaired", false)
		return result
	}
	result.LikelyVerdict = verdict
	result.LikelyConfidence = confidence
	repaired := verdict == "APPROVED" && confidence >= repairPromotionThreshold
	if repaired {
		result.Approved = true
	}
	logger.Info("Verdict repair classified the marker-less review",
		"Verdict", verdict, "Confidence", confidence, "Repaired", repaired)
	return result
}

// repairConfigured reports whether the decision endpoint is configured —
// both the URL and the model name must be set (the key is optional). Off
// means the repair layer never runs.
func repairConfigured() bool {
	return os.Getenv(config.VerdictRepairURLEnv) != "" && os.Getenv(config.VerdictRepairModelEnv) != ""
}

// classifyReviewVerdict dials the decision endpoint with one classify-only
// request — a plain typed HTTP client in the activity path: no tools, no
// session, no jail, no streaming; only the model name differs from the
// chat-completions traffic the same relay serves. It returns the
// most-likely marker, the classifier's confidence in it, and ok — false on
// any unconfigured, budget-starved, unreachable, timed-out, or unparseable
// outcome (the caller's fail-open). Never retried.
func classifyReviewVerdict(ctx context.Context, review string) (verdict string, confidence float64, ok bool) {
	endpoint, model := os.Getenv(config.VerdictRepairURLEnv), os.Getenv(config.VerdictRepairModelEnv)
	if endpoint == "" || model == "" {
		return "", 0, false
	}
	// The call rides the review activity's remaining StartToClose budget —
	// the same budget the jailed reviewer round just consumed. A round that
	// brushed that ceiling leaves less than the call needs, and dialing
	// anyway would fail the whole activity at StartToClose, destroying the
	// round (comments, session, annotation) instead of degrading to the
	// pre-repair behavior. Skip unless the full call fits — the same
	// headroom discipline as the fallback retry, and the documented
	// fail-open for a slow endpoint.
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < repairTimeout {
		return "", 0, false
	}
	// The prompt names the choice set in prose; verdictMarkers below is the
	// machine-checked copy of that list — any marker change touches both.
	prompt := fmt.Sprintf(`You are a strict classifier. Below is a code review's output whose final verdict marker line is missing. Decide which of these four verdicts the reviewer most likely intended:

- APPROVED: the review accepts the change as-is.
- CHANGES_REQUESTED: the review asks for changes or lists findings to address.
- NEEDS_MAINTAINER: the review halts for maintainer input (the task cannot be completed as stated).
- REBUILD: the review asks for an implementation change rather than a test change.

Reply with ONLY a JSON object, no other text:
{"verdict": "<one of APPROVED, CHANGES_REQUESTED, NEEDS_MAINTAINER, REBUILD>", "confidence": <a number between 0 and 1>}

Review output:

%s`, review)
	reqBody, err := json.Marshal(map[string]any{
		"model":    model,
		"messages": []map[string]string{{"role": "user", "content": prompt}},
		"stream":   false,
	})
	if err != nil { // unreachable: map of marshalable values
		return "", 0, false
	}
	callCtx, cancel := context.WithTimeout(ctx, repairTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(callCtx, http.MethodPost,
		strings.TrimSuffix(endpoint, "/")+"/chat/completions", bytes.NewReader(reqBody))
	if err != nil {
		return "", 0, false
	}
	req.Header.Set("Content-Type", "application/json")
	if key := os.Getenv(config.VerdictRepairKeyEnv); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", 0, false
	}
	var completion struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&completion); err != nil {
		return "", 0, false
	}
	if len(completion.Choices) == 0 {
		return "", 0, false
	}
	var answer struct {
		Verdict    string  `json:"verdict"`
		Confidence float64 `json:"confidence"`
	}
	// Small local models fence their JSON about half the time — the same
	// tolerance the tool relay's parser applies to the same model class.
	if err := json.Unmarshal([]byte(stripJSONFence(completion.Choices[0].Message.Content)), &answer); err != nil {
		return "", 0, false
	}
	verdict = strings.ToUpper(strings.TrimSpace(answer.Verdict))
	if !slices.Contains(verdictMarkers, verdict) {
		return "", 0, false
	}
	// A confidence outside [0,1] — a different scale ("90"), a misread
	// prompt — is garbage like any other malformed answer: fail open rather
	// than let a rescaled number impersonate certainty.
	if answer.Confidence < 0 || answer.Confidence > 1 {
		return "", 0, false
	}
	return verdict, answer.Confidence, true
}

// stripJSONFence trims content and, when it opens with a code fence, strips
// the fence and its info string. Shapes the strip cannot make a single
// object (prose plus a block, two blocks) fail the downstream unmarshal and
// fail the repair open.
func stripJSONFence(content string) string {
	s := strings.TrimSpace(content)
	if !strings.HasPrefix(s, "```") {
		return s
	}
	s = strings.TrimPrefix(s, "```")
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[i+1:]
	} else {
		return s
	}
	if i := strings.LastIndex(s, "```"); i >= 0 {
		s = s[:i]
	}
	return strings.TrimSpace(s)
}
