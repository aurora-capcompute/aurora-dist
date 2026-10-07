package memory

import (
	"context"
	"testing"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
)

func TestEventLogAppendReadStreams(t *testing.T) {
	log := NewEventLog()
	ctx := context.Background()
	scope := aurora.LogScope{TenantID: "t", SessionID: "ses"}

	head, err := log.Append(ctx, scope, aurora.LogEvent{Kind: "a"}, aurora.LogEvent{Kind: "b"})
	if err != nil || head != 2 {
		t.Fatalf("append head = %d, err = %v", head, err)
	}
	events, err := log.Read(ctx, scope, 1)
	if err != nil || len(events) != 1 || events[0].Kind != "b" || events[0].Seq != 2 {
		t.Fatalf("read = %+v, err = %v", events, err)
	}
	streams, err := log.Streams(ctx, "t")
	if err != nil || len(streams) != 1 || streams[0] != scope {
		t.Fatalf("streams = %v, err = %v", streams, err)
	}
}
