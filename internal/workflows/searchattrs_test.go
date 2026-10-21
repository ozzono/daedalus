package workflows

import (
	"embed"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// searchAttrSources is this package's own production source, embedded at
// build: the upsert-site scan anchors on it, immune to both the process
// cwd and -trimpath (a runtime.Caller path is module-relative under
// trimpath and a cwd-relative read breaks outside the package dir).
//
//go:embed pipeline.go run.go
var searchAttrSources embed.FS

// searchAttrTypes is the declared attribute table — the source identifier
// (what an upsert site keys its map with), the constant's string value
// (the name registered on the namespace), and the Temporal type each
// attribute's values carry: Keyword for the string ids, Datetime for the
// workflow.Now timestamps — the type the namespace registration must
// create and the type the list rendering decodes back.
var searchAttrTypes = []struct {
	name string
	attr string
	typ  string
}{
	{"DaedalusStatusAttr", DaedalusStatusAttr, "Keyword"},
	{"LastActivityAttr", LastActivityAttr, "Datetime"},
	{"DaedalusDependsOnAttr", DaedalusDependsOnAttr, "Keyword"},
	{"DaedalusStartedAtAttr", DaedalusStartedAtAttr, "Datetime"},
}

// repoRoot walks up from the package dir to the module root — the Makefile
// lives there.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the package — where is the repo root?")
		}
		dir = parent
	}
}

// TestSearchAttributeNamespaceLockstep pins the lockstep between the search
// attributes the pipeline upserts — the declared constants — and the
// names/types registered on the namespace (the `make custom-columns`
// operator script today; the startup registration once it exists). The
// wedge this guards: a run upserting an attribute the namespace lacks
// fails every workflow task with BadSearchAttributes and retries forever —
// a live run had to be repaired by a hand-run `temporal operator
// search-attribute create`. A renamed constant, a drifted create line, a
// mistyped registration, or a new upsert site keying a raw string must
// fail here rather than a run.
func TestSearchAttributeNamespaceLockstep(t *testing.T) {
	// The namespace registration: every declared attribute has a create
	// line with the declared type, and every create line names a declared
	// attribute — no orphaned registration, no drifted name, no drifted
	// type.
	mk, err := os.ReadFile(filepath.Join(repoRoot(t), "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	created := map[string]string{}
	for line := range strings.SplitSeq(string(mk), "\n") {
		code, _, _ := strings.Cut(line, "#")
		if !strings.Contains(code, "search-attribute create") {
			continue
		}
		name, typ, ok := searchAttrCreateArgs(code)
		if !ok {
			t.Errorf("Makefile create line %q carries no --name/--type pair", strings.TrimSpace(line))
			continue
		}
		created[name] = typ
	}
	declared := map[string]string{} // constant value -> Temporal type
	idents := map[string]bool{}     // source identifier -> present
	for _, a := range searchAttrTypes {
		declared[a.attr] = a.typ
		idents[a.name] = true
	}
	for _, a := range searchAttrTypes {
		gotTyp, ok := created[a.attr]
		if !ok {
			t.Errorf("declared attribute %q (%s) has no namespace create line — a run upserting it wedges on BadSearchAttributes", a.attr, a.name)
			continue
		}
		if gotTyp != a.typ {
			t.Errorf("attribute %q is registered as %s, want %s — the upserted values would not decode as indexed", a.attr, gotTyp, a.typ)
		}
	}
	for name := range created {
		if _, ok := declared[name]; !ok {
			t.Errorf("the namespace registers %q, which no declared constant carries — the registration ran away from the table", name)
		}
	}

	// The upsert sites: every UpsertSearchAttributes call in the package
	// keys its map with a declared constant — a raw string names an
	// attribute this lockstep (and so the namespace registration) cannot
	// see, and upserting it wedges exactly like an unregistered name.
	for _, src := range []string{"pipeline.go", "run.go"} {
		b, err := searchAttrSources.ReadFile(src)
		if err != nil {
			t.Fatalf("read embedded %s: %v", src, err)
		}
		source := string(b)
		for pos := 0; ; {
			i := strings.Index(source[pos:], "UpsertSearchAttributes(")
			if i < 0 {
				break
			}
			abs := pos + i
			key, ok := upsertSearchAttrKey(source[abs:])
			if !ok {
				t.Errorf("%s: unreadable upsert map key at %q — teach this pin the new site's shape", src, strings.TrimSpace(firstLine(source[abs:])))
				break
			}
			if !idents[key] {
				t.Errorf("%s upserts %q, which no declared constant carries — key the site with a search-attribute constant", src, key)
			}
			pos = abs + len("UpsertSearchAttributes(")
		}
	}
}

// firstLine returns everything up to the first newline — for diagnostics.
func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// searchAttrCreateArgs pulls the --name and --type flag values off one
// `temporal operator search-attribute create` command line.
func searchAttrCreateArgs(cmd string) (name, typ string, ok bool) {
	fields := strings.Fields(cmd)
	for i, f := range fields {
		switch f {
		case "--name":
			if i+1 < len(fields) {
				name = fields[i+1]
			}
		case "--type":
			if i+1 < len(fields) {
				typ = fields[i+1]
			}
		}
	}
	return name, typ, name != "" && typ != ""
}

// upsertSearchAttrKey extracts the map key of an UpsertSearchAttributes
// call — the identifier before the call's first colon (the first map
// entry's separator). Sites span lines (`…map[string]interface{}{\n\tConst:
// value…}`), so the scan anchors on the colon and walks back over
// identifier characters; a raw-string or computed key has no identifier
// there and fails the pin loudly rather than skipping.
func upsertSearchAttrKey(call string) (key string, ok bool) {
	colon := strings.Index(call, ":")
	if colon < 0 {
		return "", false
	}
	i := colon
	for i > 0 && searchAttrIdentByte(call[i-1]) {
		i--
	}
	return call[i:colon], i < colon
}

// searchAttrIdentByte reports whether b can appear in a Go identifier.
func searchAttrIdentByte(b byte) bool {
	return b == '_' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9'
}
