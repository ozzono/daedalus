package activities

import (
	"bytes"
	"context"
	"encoding/json"
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

// systemoneQuestion is one named entry of the decision request's questions
// field map. A choice question names its answer set in criteria — option
// key to description, 2–26 entries (the endpoint rejects anything else).
type systemoneQuestion struct {
	Type         string            `json:"type"`
	Instructions string            `json:"instructions"`
	Criteria     map[string]string `json:"criteria"`
}

// systemoneRequest is the decision endpoint's whole request body: the model
// name, one state carried as data (never instructions), and a named
// question map — no positional fields, no generation parameters (the
// answer is scored by the pointer head, not generated).
type systemoneRequest struct {
	Model     string                       `json:"model"`
	State     string                       `json:"state"`
	Questions map[string]systemoneQuestion `json:"questions"`
}

// systemoneAnswer is one question's answer: the chosen criteria key with
// the full score distribution and the confidence in the choice.
type systemoneAnswer struct {
	Type          string             `json:"type"`
	Choice        string             `json:"choice"`
	Probabilities map[string]float64 `json:"probabilities"`
	Confidence    float64            `json:"confidence"`
}

// verdictQuestionKey is the named questions-map key of the review verdict
// question — the same name on the request and the answers lookup.
const verdictQuestionKey = "verdict"

// verdictDescriptions anchor each marker's semantics in the decision
// question's criteria. Keyed by marker and iterated under verdictMarkers,
// so the choice set cannot drift from the set the verdict parser
// exact-matches — a marker added to verdictMarkers without an entry here
// fails the classify open like any other malformed answer, never an empty
// criterion: add both together.
var verdictDescriptions = map[string]string{
	"APPROVED":          "the review accepts the change as-is",
	"CHANGES_REQUESTED": "the review asks for changes or lists findings to address",
	"NEEDS_MAINTAINER":  "the review halts for maintainer input (the task cannot be completed as stated)",
	"REBUILD":           "the review asks for an implementation change rather than a test change",
}

// classifyReviewVerdict dials the decision endpoint with one classify-only
// request — the decision model's native systemone contract
// (POST <url>/systemone; the OpenAI-compat chat paths reject
// decision-capable models, so no chat-completions fallback exists): state
// carries the marker-less review as data, and the named "verdict" choice
// question offers the four markers as criteria, making the answer the
// pointer head's scored choice rather than generated text. A plain typed
// HTTP client in the activity path: no tools, no session, no jail, no
// streaming. It returns the most-likely marker, the classifier's
// confidence in it, and ok — false on any unconfigured, budget-starved,
// unreachable, timed-out, or unparseable outcome (the caller's fail-open).
// Never retried.
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
	// The criteria are the marker set itself — verdictMarkers is the single
	// source, so the choice keys cannot drift from the parser's set.
	criteria := make(map[string]string, len(verdictMarkers))
	for _, marker := range verdictMarkers {
		desc, known := verdictDescriptions[marker]
		if !known {
			return "", 0, false
		}
		criteria[marker] = desc
	}
	reqBody, err := json.Marshal(systemoneRequest{
		Model: model,
		State: review,
		Questions: map[string]systemoneQuestion{
			verdictQuestionKey: {
				Type:         "choice",
				Instructions: "Which final review verdict does this review round support?",
				Criteria:     criteria,
			},
		},
	})
	if err != nil { // unreachable: struct of marshalable values
		return "", 0, false
	}
	callCtx, cancel := context.WithTimeout(ctx, repairTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(callCtx, http.MethodPost,
		strings.TrimSuffix(endpoint, "/")+"/systemone", bytes.NewReader(reqBody))
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
		Answers map[string]systemoneAnswer `json:"answers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&completion); err != nil {
		return "", 0, false
	}
	answer, answered := completion.Answers[verdictQuestionKey]
	if !answered || !slices.Contains(verdictMarkers, answer.Choice) {
		return "", 0, false
	}
	// A confidence outside [0,1] — a different scale, a misread field — is
	// garbage like any other malformed answer: fail open rather than let a
	// rescaled number impersonate certainty.
	if answer.Confidence < 0 || answer.Confidence > 1 {
		return "", 0, false
	}
	return answer.Choice, answer.Confidence, true
}
