package memory

import (
	"context"
	"testing"
	"time"

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

func TestLeasesExcludeOtherHolders(t *testing.T) {
	leases := NewLeases()
	ctx := context.Background()
	now := time.Unix(0, 0)

	ok, err := leases.Acquire(ctx, "t", "process", "p1", "holder-a", now, time.Minute)
	if err != nil || !ok {
		t.Fatalf("first acquire = %v, %v", ok, err)
	}
	// A different holder is excluded until expiry; the same holder renews.
	if ok, _ := leases.Acquire(ctx, "t", "process", "p1", "holder-b", now.Add(time.Second), time.Minute); ok {
		t.Fatal("second holder acquired an unexpired lease")
	}
	if ok, _ := leases.Acquire(ctx, "t", "process", "p1", "holder-a", now.Add(time.Second), time.Minute); !ok {
		t.Fatal("holder could not renew its own lease")
	}
	if ok, _ := leases.Acquire(ctx, "t", "process", "p1", "holder-b", now.Add(2*time.Minute), time.Minute); !ok {
		t.Fatal("expired lease was not reacquirable")
	}
	if err := leases.Release(ctx, "t", "process", "p1", "holder-b"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if ok, _ := leases.Acquire(ctx, "t", "process", "p1", "holder-a", now.Add(2*time.Minute), time.Minute); !ok {
		t.Fatal("released lease was not reacquirable")
	}
}
