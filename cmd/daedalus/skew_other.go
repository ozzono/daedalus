//go:build !linux

package main

// exeReplaced is the non-linux face of the skew check: the zero-link-count
// deleted-file signal is the linux predicate; other hosts skip the check
// silently (dash-hardening Q2 — every real deployment host is linux).
func exeReplaced(string) bool {
	return false
}
