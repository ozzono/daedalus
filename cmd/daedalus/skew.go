// The dash's stale-binary guard (dash-hardening Q1/Q2): a self-daemonized
// process keeps executing the image it was exec'd from, so a `go install`
// that replaces ~/go/bin/daedalus leaves the running old binary serving —
// two days stale in the incident that filed this — with nothing owning the
// lifecycle to swap it. The detector asks the kernel whether the inode this
// process still runs is still linked into the filesystem: once the on-disk
// path names a new file (or none), the unlinked inode answers stat with a
// zero link count — the exact condition, no mtime heuristic to drift. The
// probe must go through /proc/self/exe, the only handle left to the
// unlinked inode: statting the plain path resolves to whatever lives there
// now (the replacement, link count 1) and would read healthy in exactly
// the incident this guards.
package main

// binarySkewed reports whether this process's executable inode was unlinked
// since it started. Any stat failure (procfs unavailable, exotic host)
// degrades to not-skewed — the banner informs, it must never fire on
// uncertainty.
func binarySkewed() bool {
	return exeReplaced("/proc/self/exe")
}
