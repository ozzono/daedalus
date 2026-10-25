package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
)

// TestExeReplaced pins the deleted-file signal the skew banner reads: a
// still-linked file answers not-replaced, an unlinked-but-open file — the
// exact inode condition /proc/self/exe answers once a `go install` swaps
// the running image — answers replaced, and an unstattable path degrades to
// not-replaced (the banner informs, it must never fire on uncertainty).
func TestExeReplaced(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the zero-link-count predicate is the linux face; other hosts skip the check")
	}

	live := filepath.Join(t.TempDir(), "live")
	if err := os.WriteFile(live, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if exeReplaced(live) {
		t.Errorf("exeReplaced(linked file) = true, want false")
	}

	if exeReplaced(filepath.Join(t.TempDir(), "gone")) {
		t.Errorf("exeReplaced(missing path) = true, want the not-replaced degrade")
	}

	// Hold an open handle, unlink, and reach the inode through /proc — the
	// only handle a daemon has left to its replaced image.
	ghost, err := os.Create(filepath.Join(t.TempDir(), "ghost"))
	if err != nil {
		t.Fatal(err)
	}
	defer ghost.Close()
	if err := os.Remove(ghost.Name()); err != nil {
		t.Fatal(err)
	}
	if !exeReplaced("/proc/self/fd/" + strconv.Itoa(int(ghost.Fd()))) {
		t.Errorf("exeReplaced(unlinked inode) = false, want the zero-link-count signal")
	}
}

// TestBinarySkewedHealthyHost pins the guard's quiet face: a live process
// whose executable is still linked must serve banner-free.
func TestBinarySkewedHealthyHost(t *testing.T) {
	if binarySkewed() {
		t.Error("binarySkewed() = true on a healthy host, want false")
	}
}
