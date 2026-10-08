package memory

import (
	"context"
	"encoding/json"
	"testing"
)

// Every call for one session returns the same space; another session gets its
// own.
func TestScratchSpacesAreOnePerSession(t *testing.T) {
	ctx := context.Background()
	spaces := NewScratchSpaces()
	if err := spaces.ScratchSpace("ses1").Put(ctx, "k", json.RawMessage(`1`), nil); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, _ := spaces.ScratchSpace("ses1").Get(ctx, "k"); !ok {
		t.Fatal("a second lookup of ses1 did not see its own key")
	}
	if _, _, ok, _ := spaces.ScratchSpace("ses2").Get(ctx, "k"); ok {
		t.Fatal("ses2 read ses1's key")
	}
}
