// Package version exposes the CLI's version. Releases are identified by git
// tags (CI tags each green master merge with the next version); a binary
// knows its version through an ldflags-injected value (see `make build`) or
// Go's build info, and plain local builds report "(devel)".
package version

import (
	"runtime/debug"
	"strings"
)

// ldflagsVersion is set at link time:
// -ldflags "-X daedalus/internal/version.ldflagsVersion=v1.2.3"
var ldflagsVersion string

// String returns the CLI's version: the ldflags-injected value when set,
// otherwise the module version Go recorded at build time, falling back to
// "(devel)" for local builds.
func String() string {
	if v := strings.TrimSpace(ldflagsVersion); v != "" {
		return v
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		if v := bi.Main.Version; v != "" && v != "(devel)" {
			return v
		}
	}
	return "(devel)"
}
