package dist

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	"github.com/aurora-capcompute/aurora-capcompute/capability"
	"github.com/aurora-capcompute/aurora-dispatchers/registry"
	"github.com/aurora-capcompute/aurora-dispatchers/scratch"
	distmem "github.com/aurora-capcompute/aurora-dist/internal/dist/store/memory"
	"github.com/aurora-capcompute/capcompute/sys"
)

// The process credential, never the manifest, picks the scratch space: two
// processes of one session share it, and a process of another session — with
// the identical grant — sees none of it.
func TestScratchSpaceFollowsTheCredentialsSession(t *testing.T) {
	provider := newProvider(nil, registry.Services{}, distmem.NewScratchSpaces())
	manifest := aurora.Manifest{
		Version: aurora.ManifestVersion,
		Syscalls: []aurora.Syscall{
			{Syscall: scratch.Capability, Config: json.RawMessage(`{"capabilities":[{"operation":"put"},{"operation":"get"}]}`)},
		},
	}
	call := func(cred aurora.ProcessContext, args string) sys.SyscallResult {
		t.Helper()
		table, err := provider.NewDispatcher(context.Background(), cred, manifest)
		if err != nil {
			t.Fatalf("new dispatcher: %v", err)
		}
		result, err := capability.NewDispatcher[aurora.ProcessContext](table).Dispatch(context.Background(), cred,
			sys.Syscall{Name: scratch.Capability, Args: json.RawMessage(args)}, sys.Authorization{})
		if err != nil || result.Status() != sys.StatusResult {
			t.Fatalf("dispatch %s = %#v, %v", args, result, err)
		}
		return result
	}

	writer := aurora.ProcessContext{SessionID: "ses1", ProcessID: "proc_a"}
	sibling := aurora.ProcessContext{SessionID: "ses1", ProcessID: "proc_b"}
	stranger := aurora.ProcessContext{SessionID: "ses2", ProcessID: "proc_c"}

	call(writer, `{"operation":"put","key":"handoff","value":"for the next run"}`)
	if got := call(sibling, `{"operation":"get","key":"handoff"}`); !strings.Contains(string(got.Result()), "for the next run") {
		t.Fatalf("a later process of the session missed the value: %s", got.Result())
	}
	if got := call(stranger, `{"operation":"get","key":"handoff"}`); !strings.Contains(string(got.Result()), `"found":false`) {
		t.Fatalf("another session read the value: %s", got.Result())
	}
}
