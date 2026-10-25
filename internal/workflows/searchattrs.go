package workflows

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	enums "go.temporal.io/api/enums/v1"
	operatorservice "go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/client"
)

// searchAttributes is the registration table: every search attribute the
// pipeline upserts (the visibilityChangeID and dependencyWakeupChangeID
// sites), keyed by the declared constants — a renamed constant fails to
// compile here instead of wedging runs — and valued with the Temporal type
// each upserted value carries. The pairs are the same ones the `make
// custom-columns` operator script creates (the lockstep pin
// TestSearchAttributeNamespaceLockstep guards that script against the
// constants); DaedalusDependsOn is Keyword, not KeywordList, because its
// upsert carries one dependency workflow id, the shape production use
// settled.
var searchAttributes = map[string]enums.IndexedValueType{
	DaedalusStatusAttr:    enums.INDEXED_VALUE_TYPE_KEYWORD,
	LastActivityAttr:      enums.INDEXED_VALUE_TYPE_DATETIME,
	DaedalusDependsOnAttr: enums.INDEXED_VALUE_TYPE_KEYWORD,
	DaedalusStartedAtAttr: enums.INDEXED_VALUE_TYPE_DATETIME,
}

// searchAttributeNamespace is the namespace every registration targets:
// the one the `make custom-columns` script creates on, and the one
// runstart.NewClient joins (its Dial sets no namespace, so this deployment
// is default-namespace by construction).
const searchAttributeNamespace = "default"

// searchAttributeRegisterTimeout bounds the two operator calls at worker
// boot. The client dial just proved connectivity, so the bound only keeps
// a stalled frontend from hanging an unkillable boot — the worker exits
// nonzero either way.
const searchAttributeRegisterTimeout = 30 * time.Second

// RegisterSearchAttributes creates any of the pipeline's search attributes
// missing from the namespace, so a worker boot leaves the runs it serves
// schedulable with no operator step — the startup registration the
// lockstep pin's comment anticipates. The query comes first: only names
// the listing lacks are created, never an assumed type over an existing
// one. A name already registered with the declared type is a quiet no-op,
// so every boot after the first is one list call; two workers booting
// simultaneously onto a bare namespace can both see the attributes
// missing, and the loser of that create race fails its boot with
// AlreadyExists — its supervisor (or operator) restarts it into the
// now-registered namespace. A name registered with a different type fails
// the startup naming both types: a search attribute's type is permanent
// once created, so the mismatch is a maintainer correction, not something
// this process can or may "fix". Any other failure fails the startup too
// — a run upserting an unregistered attribute fails every workflow task
// with BadSearchAttributes and retries forever, the wedge a hand-run
// `temporal operator search-attribute create` once had to repair.
func RegisterSearchAttributes(ctx context.Context, c client.Client) error {
	ctx, cancel := context.WithTimeout(ctx, searchAttributeRegisterTimeout)
	defer cancel()
	op := c.OperatorService()
	listing, err := op.ListSearchAttributes(ctx,
		&operatorservice.ListSearchAttributesRequest{Namespace: searchAttributeNamespace})
	if err != nil {
		return fmt.Errorf("list search attributes on namespace %q: %w", searchAttributeNamespace, err)
	}
	missing := make(map[string]enums.IndexedValueType)
	for name, typ := range searchAttributes {
		got, ok := listing.CustomAttributes[name]
		if !ok {
			missing[name] = typ
		} else if got != typ {
			return fmt.Errorf("search attribute %q is registered as type %s but daedalus declares %s — the type is permanent once created; correct the namespace registration or the declared type, then start the worker again",
				name, got, typ)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	if _, err := op.AddSearchAttributes(ctx,
		&operatorservice.AddSearchAttributesRequest{
			Namespace:        searchAttributeNamespace,
			SearchAttributes: missing,
		}); err != nil {
		names := slices.Sorted(maps.Keys(missing))
		return fmt.Errorf("register search attributes %v on namespace %q: %w", names, searchAttributeNamespace, err)
	}
	return nil
}
