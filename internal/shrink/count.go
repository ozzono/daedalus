package shrink

import (
	_ "embed"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"sync"

	tiktoken "github.com/pkoukk/tiktoken-go"
)

// o200kBaseRanks is the canonical o200k_base mergeable-ranks file — sha256
// 446a9538cb6c348e3516120d7c08b09f57c36495e2acfffe59a5bf8b0cfb1a2d, the
// hash OpenAI's own tiktoken pins for
// https://openaipublic.blob.core.windows.net/encodings/o200k_base.tiktoken —
// embedded so the strict counter never downloads at runtime. The file is
// counted with the same ranks the slim flow's target models are sized
// against; it is a lossless meter only: nothing here removes content, the
// shrink pass below does, and this count is how its "strictly smaller"
// invariant is verified.
//
//go:embed o200k_base.tiktoken
var o200kBaseRanks string

// embeddedLoader replaces tiktoken-go's default loader (an HTTP fetch per
// ranks file) with the embedded copy. Only o200k_base is embedded, so a
// request for any other encoding fails instead of silently falling back to
// the network.
type embeddedLoader struct{}

func (embeddedLoader) LoadTiktokenBpe(tiktokenBpeFile string) (map[string]int, error) {
	if !strings.HasSuffix(tiktokenBpeFile, "o200k_base.tiktoken") {
		return nil, fmt.Errorf("shrink: no embedded ranks for %q (only o200k_base)", tiktokenBpeFile)
	}
	ranks := make(map[string]int, 200000)
	for i, line := range strings.Split(o200kBaseRanks, "\n") {
		if line == "" {
			continue
		}
		parts := strings.Split(line, " ")
		if len(parts) != 2 {
			return nil, fmt.Errorf("shrink: o200k_base ranks line %d: want \"<base64> <rank>\"", i+1)
		}
		token, err := base64.StdEncoding.DecodeString(parts[0])
		if err != nil {
			return nil, fmt.Errorf("shrink: o200k_base ranks line %d: %w", i+1, err)
		}
		rank, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, fmt.Errorf("shrink: o200k_base ranks line %d: %w", i+1, err)
		}
		ranks[string(token)] = rank
	}
	return ranks, nil
}

// The encoder is built once per process: the BPE merge table over ~200k
// ranks costs more than any single count, and the loader swap is package
// state in tiktoken-go.
var (
	encOnce sync.Once
	enc     *tiktoken.Tiktoken
	encErr  error
)

func encoder() (*tiktoken.Tiktoken, error) {
	encOnce.Do(func() {
		tiktoken.SetBpeLoader(embeddedLoader{})
		enc, encErr = tiktoken.GetEncoding("o200k_base")
	})
	return enc, encErr
}

// Count returns the strict o200k_base token count of s — a real
// mergeable-rank count, not an estimate, so "strictly smaller" in the
// shrink gate means strictly fewer real tokens. Special-token strings
// riding in the text are counted as ordinary content, never interpreted.
func Count(s string) (int, error) {
	e, err := encoder()
	if err != nil {
		return 0, fmt.Errorf("shrink: build o200k_base counter: %w", err)
	}
	return len(e.Encode(s, nil, nil)), nil
}

// Estimate is the biased-high chars/4 heuristic — ceil(len/4) — mirroring
// the estimator class pi's own prompt sizing uses to compute the wire
// budget. It reads high on ordinary English prose (o200k averages ~4
// chars/token, so ceiling plus punctuation-heavy text round upward), which
// is the point: the input gate takes the max of this and Count, so it
// cannot under-count against the arithmetic that actually governs the
// model's max_tokens.
func Estimate(s string) int {
	return (len(s) + 3) / 4
}
