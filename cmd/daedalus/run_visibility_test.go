package main

import (
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

// TestDur pins the runs-table duration rendering: negative elapsed (clock
// skew between server and client) clamps to zero, whole seconds render
// under an hour, and the hours/days tiers keep one decimal.
func TestDur(t *testing.T) {
	for _, c := range []struct {
		in   time.Duration
		want string
	}{
		{-5 * time.Minute, "0s"},
		{0, "0s"},
		{42 * time.Second, "42s"},
		{90 * time.Second, "1m30s"},
		{59 * time.Minute, "59m0s"},
		{time.Hour, "1.0h"},
		{90 * time.Minute, "1.5h"},
		{24 * time.Hour, "1.0d"},
		{36 * time.Hour, "1.5d"},
	} {
		if got := dur(c.in); got != c.want {
			t.Errorf("dur(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestDecodeRunVisibility pins the run-visibility decode behind the runs
// table: the workflow-upserted pair decodes with the default converter, and
// undecodable or absent values — old runs never backfilled, history is
// immutable — degrade to zero values per field instead of failing the table.
func TestDecodeRunVisibility(t *testing.T) {
	dc := converter.GetDefaultDataConverter()
	last := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)

	statusPayload, err := dc.ToPayload("parked")
	if err != nil {
		t.Fatal(err)
	}
	lastPayload, err := dc.ToPayload(last)
	if err != nil {
		t.Fatal(err)
	}
	// A JSON object under the json/plain encoding: metadata checks out,
	// unmarshalling into a string (or a time) does not.
	broken := func() *commonpb.Payload {
		return &commonpb.Payload{
			Metadata: map[string][]byte{"encoding": []byte("json/plain")},
			Data:     []byte(`{"not":"a string"}`),
		}
	}

	t.Run("both attributes decode", func(t *testing.T) {
		vis := decodeRunVisibility(&commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
			"DaedalusStatus": statusPayload,
			"LastActivityAt": lastPayload,
		}}, dc)
		if vis.Status != "parked" || !vis.LastAt.Equal(last) {
			t.Errorf("decodeRunVisibility = %+v, want status parked at %v", vis, last)
		}
	})

	t.Run("absent attributes degrade to zero values", func(t *testing.T) {
		empty := &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{}}
		for _, sa := range []*commonpb.SearchAttributes{nil, empty} {
			if vis := decodeRunVisibility(sa, dc); vis != (runVisibility{}) {
				t.Errorf("decodeRunVisibility(%v) = %+v, want the zero value", sa, vis)
			}
		}
	})

	t.Run("one undecodable field degrades alone", func(t *testing.T) {
		vis := decodeRunVisibility(&commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
			"DaedalusStatus": broken(),
			"LastActivityAt": lastPayload,
		}}, dc)
		if vis.Status != "" || !vis.LastAt.Equal(last) {
			t.Errorf("decodeRunVisibility = %+v, want an empty status but the decoded time %v", vis, last)
		}
	})

	t.Run("the dependency attribute decodes alone", func(t *testing.T) {
		depPayload, err := dc.ToPayload("wf-1")
		if err != nil {
			t.Fatal(err)
		}
		vis := decodeRunVisibility(&commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
			"DaedalusDependsOn": depPayload,
		}}, dc)
		if vis.DependsOn != "wf-1" || vis.Status != "" || !vis.LastAt.IsZero() {
			t.Errorf("decodeRunVisibility = %+v, want only DependsOn set to %q", vis, "wf-1")
		}
	})
}
