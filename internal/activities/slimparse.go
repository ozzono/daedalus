package activities

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ozzono/daedalus/internal/config"
)

// slimParseSalvageTimeout bounds one parsing-AI transcription call. The
// call transcribes a whole stranded plan into a subtask queue — thousands
// of completion tokens on exactly the small self-hosted hardware the slim
// flow targets — so the bound gives a slow seat room without letting a
// wedged one hold the run: a slower seat fails the salvage and the kill
// parks as before. One call per run, never retried.
const slimParseSalvageTimeout = 10 * time.Minute

// SlimParseSalvageInput carries the complete transcription prompt —
// template.SlimParse over the captured stranded prose, built workflow-side:
// the workflow owns the payload contract and validates the reply with its
// own parse machinery, so this activity is the dial alone.
type SlimParseSalvageInput struct {
	Prompt string
}

// SlimParseSalvageOutput is the parsing seat's reply. OK reports that the
// seat answered with non-empty text; Text is that raw reply, unvalidated —
// the workflow's parse machinery decides whether it is a usable subtask
// queue.
type SlimParseSalvageOutput struct {
	OK   bool
	Text string
}

// SlimParseSalvageActivity dials the slim parse seat — the parser model
// (slim.parser_model, exported as DAEDALUS_SLIM_PARSER_MODEL) at the
// openai channel's upstream (OPENAI_API_BASE / OPENAI_BASE_URL, the same
// endpoint the jailed rounds' staged base URLs name) — with one
// non-streaming chat completion and returns its reply. The layer is
// fail-open by contract: an unconfigured seat, an unreachable endpoint, a
// non-200, a decode failure, or an empty answer all return OK=false with a
// nil error, so the caller parks exactly as the unarmed path would have —
// the parse AI may only recover a doomed round, never wedge the run — and
// the workflow's single-attempt retry policy plus this shape keep the call
// at exactly one per run (the verdict-repair layer's posture).
func SlimParseSalvageActivity(ctx context.Context, in SlimParseSalvageInput) (SlimParseSalvageOutput, error) {
	model := os.Getenv(config.SlimParserModelEnv)
	base := os.Getenv("OPENAI_API_BASE")
	if base == "" {
		base = os.Getenv("OPENAI_BASE_URL")
	}
	if in.Prompt == "" || model == "" || base == "" {
		return SlimParseSalvageOutput{}, nil
	}
	reqBody, err := json.Marshal(map[string]any{
		"model": model,
		"messages": []map[string]string{
			{"role": "user", "content": in.Prompt},
		},
		"stream": false,
	})
	if err != nil { // unreachable: map of marshalable values
		return SlimParseSalvageOutput{}, nil
	}
	callCtx, cancel := context.WithTimeout(ctx, slimParseSalvageTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(callCtx, http.MethodPost,
		strings.TrimSuffix(base, "/")+"/chat/completions", bytes.NewReader(reqBody))
	if err != nil {
		return SlimParseSalvageOutput{}, nil
	}
	req.Header.Set("Content-Type", "application/json")
	if key := os.Getenv("OPENAI_API_KEY"); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		activityLogger(ctx).Warn("Slim parse salvage: parsing-AI call failed", "Error", err)
		return SlimParseSalvageOutput{}, nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		activityLogger(ctx).Warn("Slim parse salvage: parsing-AI endpoint answered non-200", "Status", resp.StatusCode)
		return SlimParseSalvageOutput{}, nil
	}
	var completion struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&completion); err != nil {
		activityLogger(ctx).Warn("Slim parse salvage: parsing-AI reply is not a chat completion", "Error", err)
		return SlimParseSalvageOutput{}, nil
	}
	if len(completion.Choices) == 0 || completion.Choices[0].Message.Content == "" {
		activityLogger(ctx).Warn("Slim parse salvage: parsing-AI reply carried no content")
		return SlimParseSalvageOutput{}, nil
	}
	return SlimParseSalvageOutput{OK: true, Text: completion.Choices[0].Message.Content}, nil
}
