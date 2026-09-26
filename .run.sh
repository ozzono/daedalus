#!/bin/bash
# Hermetic sandboxed test runner: mirrors proxy.golang.org on disk with curl
# (Go's own TLS fetch is blocked here), then runs go test.
set -u
cd /Users/hugo/Projects/daedalus

ROOT=$(mktemp -d)
MIRROR=/Users/hugo/Projects/daedalus/.mirror
mkdir -p "$MIRROR"

export GOCACHE="$PWD/.gocache"
export GOMODCACHE="$PWD/.gomodcache"
export GOPROXY="file://$MIRROR"
export GOSUMDB=off
export GOFLAGS=-mod=mod

fetch() {
  local mod="$1" ver="$2"
  local enc d base f
  enc=$(printf '%s' "$mod" | perl -pe 's/([A-Z])/!\L$1/g')
  d="$MIRROR/$enc/@v"
  base="https://proxy.golang.org/$enc/@v"
  mkdir -p "$d"
  for f in "$ver.mod" "$ver.zip"; do
    if [ ! -s "$d/$f" ]; then
      if ! curl -sf --max-time 90 -o "$d/$f.part" "$base/$f"; then
        rm -f "$d/$f.part"
        echo "FETCH_FAIL $mod $ver $f" >&2
        return 1
      fi
      mv "$d/$f.part" "$d/$f"
    fi
  done
  return 0
}

# Missing-module errors look like "mod@vX: reading file://...: no such file".
# Anchor the token to the ": reading file" suffix so cache-path prefixes
# (e.g. .gomodcache/mod@vX/pkg/file.go:12:3: other@vY: reading file...) can
# never be mistaken for the missing module, and only trust module@version
# tokens that start with a known prefix.
missing() {
  grep -oE '(github\.com|google\.golang\.org|golang\.org|gopkg\.in|go\.temporal\.io|buf\.build|gonum\.org)[a-z0-9._/-]*@v[0-9][0-9A-Za-z.+-]*: reading file' \
    | sed -E 's/: reading file$//' | sort -u
}

for i in $(seq 1 80); do
  ERR=$(go test -buildvcs=false ./... 2>&1)
  RC=$?
  [ $RC -eq 0 ] && { printf '%s\n' "$ERR" | grep -E '^(ok|FAIL|---|\?)'; echo "=== TESTS OK ==="; exit 0; }
  MISSES=$(printf '%s' "$ERR" | missing)
  if [ -n "$MISSES" ]; then
    fail=0
    while IFS= read -r m; do
      echo "round $i: $m" >&2
      fetch "${m%@*}" "${m##*@}" || fail=1
    done <<< "$MISSES"
    [ $fail -eq 0 ] && continue
  fi
  echo "STUCK (missing='$MISSES'):" >&2
  printf '%s\n' "$ERR" | grep -v '^go: downloading' | head -40 >&2
  exit 1
done
echo "LOOP_EXHAUSTED" >&2
exit 1
