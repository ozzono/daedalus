//go:build linux

package main

import (
	"os"
	"syscall"
)

// exeReplaced reports whether the file at path carries the kernel's
// deleted-file signal — a link count of zero, what stat answers for an
// inode the filesystem no longer links while a process still runs its
// image. The caller supplies the handle (binarySkewed passes
// /proc/self/exe, the running image's own inode); a plain-path predicate
// (no /proc read inline), so the pins can feed it /proc/self/fd handles of
// unlinked files or crafted paths.
func exeReplaced(path string) bool {
	st, err := os.Stat(path)
	if err != nil {
		return false
	}
	sys, ok := st.Sys().(*syscall.Stat_t)
	return ok && sys.Nlink == 0
}
