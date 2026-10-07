package memory

import (
	"context"
	"encoding/json"
	"testing"

	drivermem "github.com/aurora-capcompute/aurora-dispatchers/memory"
)

// Every call for one session returns the same space; another session or tenant
// gets its own.
func TestScratchSpacesAreOnePerSession(t *testing.T) {
	ctx := context.Background()
	spaces := NewScratchSpaces()
	if _, err := spaces.ScratchSpace("t", "ses1").Put(ctx, "k", json.RawMessage(`1`), nil, drivermem.PutAny, ""); err != nil {
		t.Fatal(err)
	}
	if _, _, _, ok, _ := spaces.ScratchSpace("t", "ses1").Get(ctx, "k"); !ok {
		t.Fatal("a second lookup of ses1 did not see its own key")
	}
	for _, other := range [][2]string{{"t", "ses2"}, {"rival", "ses1"}} {
		if _, _, _, ok, _ := spaces.ScratchSpace(other[0], other[1]).Get(ctx, "k"); ok {
			t.Fatalf("%v read ses1's key", other)
		}
	}
}
