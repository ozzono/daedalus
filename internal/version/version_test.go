package version

import (
	"regexp"
	"testing"
)

// semverShape is the same X.Y.Z form CI enforces before tagging a release
// (no leading zeros — CI's patch arithmetic reads "08" as an invalid octal).
var semverShape = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

// TestString pins the version contract: the committed VERSION file ends in a
// newline, so a dropped trim (or a malformed version) fails this check.
func TestString(t *testing.T) {
	if v := String(); !semverShape.MatchString(v) {
		t.Errorf("String() = %q, want X.Y.Z with no surrounding whitespace", v)
	}
}
