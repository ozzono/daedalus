// Package version exposes the CLI's version from the committed VERSION file
// — the same file CI tags and bumps after each green master merge.
package version

import (
	_ "embed"
	"strings"
)

//go:embed VERSION
var raw string

// String returns the version (e.g. "0.1.0"), without the trailing newline.
func String() string {
	return strings.TrimSpace(raw)
}
