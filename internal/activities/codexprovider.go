package activities

import (
	"fmt"
	"strings"
)

// codexProviderID is the custom codex provider daedalus selects for jailed
// rounds served by the config's openai section. The whole entry is staged
// through per-invocation `-c` overrides — codex layers them over its host
// config.toml (which the jail maps in, see codexJailMounts) per launch, so
// unlike pi's staging no host file is ever rewritten.
const codexProviderID = "daedalus"

// stageCodexProvider bridges the config's openai section into codex for one
// jailed round, derived from the round's provider env like pi's staging —
// so openai-type failover rounds re-derive for free (see fallbackEnv; an
// anthropic-type fallback has no codex channel at all — codex speaks only
// OpenAI wire formats, see jailedAgentCLI). The overrides ride argv:
// model_providers.<id>.base_url is the endpoint, env_key names the env var
// codex reads the key from at request time (so the key value itself never
// lands in argv or on disk — omitted entirely for a keyless backend, which
// then gets no auth header), wire_api pins codex's only wire, -c
// model_provider selects the staged entry, and -m selects the model. The
// bridge exists because codex reads no OPENAI_* env var natively — without
// it the section would be silently inert and the round would dial whatever
// the host's ~/.codex/config.toml says. With none of the openai vars set,
// nothing is staged and no flag is added — codex follows its own host
// config untouched. With a key or model but no URL, or a URL but no model,
// the round fails before launch: the section cannot be bridged
// half-specified, and a silently-unbridged round would misdirect spend
// (the same posture as stagePiProvider: an ambient key/model without a URL
// fails the codex round before launch, exactly like pi — there is no inert
// case).
// ponytail: the -c override shapes are probe-derived from codex-cli
// 0.160.0's documented config surface (2026-10-05) but never exercised
// against a live round — this sandbox has no codex binary; wire_api is
// pinned to "responses" per the same probe notes (chat completions
// reportedly removed from 2026 builds, unprobed here), so a self-hosted
// backend serving only chat completions cannot serve codex — that is
// codex's own constraint, surfaced at the first round. The openai
// section's sampler and timeout exports likewise have no codex route (a
// model_providers entry carries no request params, and the timeout env var
// has no verified codex reader), so the backend's own defaults stand, per
// the agnostic rule.
func stageCodexProvider(env []string) ([]string, error) {
	url := envLookup(env, "OPENAI_BASE_URL")
	if url == "" {
		url = envLookup(env, "OPENAI_API_BASE")
	}
	key := envLookup(env, "OPENAI_API_KEY")
	model := envLookup(env, "OPENAI_MODEL")
	if url == "" && key == "" && model == "" {
		return nil, nil
	}
	if url == "" {
		return nil, fmt.Errorf("codex round: OPENAI_API_KEY/OPENAI_MODEL exported without a base URL — codex reads no provider env var natively, so the section cannot be bridged and the round would dial the host's own codex config; set openai.url (and openai.model), or unset the ambient OPENAI_* exports")
	}
	if model == "" {
		return nil, fmt.Errorf("codex round: openai.url exported without openai.model — codex would pick its built-in default model against the bridged endpoint; set openai.model")
	}
	// Values are TOML basic strings — tomlQuote escapes them, so a URL is
	// never mistaken for anything else by codex's -c override parser, and
	// never terminates the override early.
	provider := "model_providers." + codexProviderID
	args := []string{
		"-c", provider + `.base_url=` + tomlQuote(url),
		"-c", provider + `.wire_api="responses"`,
	}
	if key != "" {
		args = append(args, "-c", provider+`.env_key="OPENAI_API_KEY"`)
	}
	return append(args,
		"-c", `model_provider="`+codexProviderID+`"`,
		"-m", model), nil
}

// tomlQuote renders s as one TOML basic string: backslash and quote
// escaped, the named control characters by their short forms, every other
// control character as \uXXXX — TOML forbids raw control characters (and a
// raw quote or backslash changes the string's shape) inside basic strings,
// and both config load routes accept an openai.url carrying any of them.
func tomlQuote(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\b':
			b.WriteString(`\b`)
		case '\t':
			b.WriteString(`\t`)
		case '\n':
			b.WriteString(`\n`)
		case '\f':
			b.WriteString(`\f`)
		case '\r':
			b.WriteString(`\r`)
		default:
			if r < 0x20 || r == 0x7f {
				fmt.Fprintf(&b, `\u%04X`, r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}
