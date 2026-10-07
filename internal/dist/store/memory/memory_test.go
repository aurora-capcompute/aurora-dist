package memory

import (
	"context"
	"testing"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
)

func TestEventLogAppendReadSessions(t *testing.T) {
	log := NewEventLog()
	ctx := context.Background()

	head, err := log.Append(ctx, "ses", aurora.LogEvent{Kind: "a"}, aurora.LogEvent{Kind: "b"})
	if err != nil || head != 2 {
		t.Fatalf("append head = %d, err = %v", head, err)
	}
	events, err := log.Read(ctx, "ses", 1)
	if err != nil || len(events) != 1 || events[0].Kind != "b" || events[0].Seq != 2 {
		t.Fatalf("read = %+v, err = %v", events, err)
	}
	sessions, err := log.Sessions(ctx)
	if err != nil || len(sessions) != 1 || sessions[0] != "ses" {
		t.Fatalf("sessions = %v, err = %v", sessions, err)
	}
}
