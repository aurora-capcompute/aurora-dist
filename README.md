# aurora-dist

The Aurora distribution: **one binary** assembling the
[`aurora-capcompute`](https://github.com/aurora-capcompute/aurora-capcompute)
runtime with a compiled-in driver set and concrete stores, exposing the
runtime over **one HTTP API** — the single way in, versioned `/v1` from
birth.

The cores stay interfaces-only; this repo is where the choices live:

- **Drivers** (from `aurora-dispatchers`): the builtin leaf router,
  `core.internet`, `core.memory`, `sys.spawn`, `sys.timer`, and
  `core.openaiApi` (the `openaillm` cognition driver).
- **Stores** (absorbed from the deprecated `aurora-stores`): an in-memory set
  for throwaway runs and a SQLite store — append-only event log (one stream
  per session, `session_id`/`process_id` vocabulary), lease table,
  hash-chained kernel journal store with a `VerifyJournal` audit path, and
  the tenant-memory KV behind `core.memory`.
- **Runtime-adjacent services** that must not live in terminals:
  - **Timer firing** — durable `sys.timer` tasks are armed by reconciling
    against runtime state on a ticker and resolved at their deadline; the same
    reconcile runs at boot, re-arming pending timers from persisted state and
    firing elapsed ones immediately. Fire times are absolute (`created_at +
    duration`), so discovery latency never shifts a deadline.
  - **Program directory** — programs load from a directory of `*.wasm`
    artifacts (id = file name), and the directory is re-scanned into the
    runtime on a ticker (digest-diffed — unchanged programs keep running), so
    the in-memory set tracks the filesystem without a manual reload. Processes
    are immutably bound to the (name, digest) they were created from:
    replacing a `*.wasm` strands its in-flight processes — they cannot resume
    or restart under the new bytes, only be killed to settle their effects —
    and the new artifact serves new processes.
  - **Capability ceiling** — an operator-configured list of capability names;
    process creation refuses manifests granting beyond it (`sys.Attenuate` at
    the door, recursing through `sys.spawn` trees). Defense in depth against
    a compromised policy layer — the kernel's Validator remains the reference
    monitor.

There is deliberately **no principal authentication**: the distribution
serves one trusted client (a local terminal such as `aurora-cli`, or the
policy layer once multi-principal — that service owns authn, manifest
registries, per-credential ceilings, and session directories). Task
resolution still authenticates its bearer `resolution_token`.

## Run

```sh
export AURORA_TASK_SECRET=change-me
aurora-dist -addr :8080 -data ./data -programs ./programs
```

`-data` empty runs on in-memory stores. Optional `-config dist.json`:

```json
{
  "addr": ":8080",
  "tenant_id": "local",
  "data_dir": "./data",
  "programs": {"dir": "./programs", "default": "agent"},
  "capability_ceiling": ["sys.timer", "openai.chat", "openai.responses",
                          "openai.embeddings", "openai.models.list"]
}
```

## API (/v1)

| Method & path | Meaning |
| --- | --- |
| `GET /v1/sessions` · `POST /v1/sessions` | list summaries / create |
| `GET /v1/sessions/{id}` | **the one comprehensive read** — the session log |
| `POST /v1/sessions/{id}/processes` | start a process: `{message, manifest}` |
| `GET /v1/processes/{id}` | cheap single-process status poll |
| `POST /v1/processes/{id}/stop` · `/retry` | steer (`{"mode":"resume"\|"restart"}`) |
| `POST /v1/tasks/{id}/resolve` | `{resolution_token, resolution:{decision,...}}` |
| `GET /v1/programs` | the loaded program artifacts (read-only) |

**One read, many renderings.** `GET /v1/sessions/{id}` returns the whole
session log: session metadata, conversation history, and every process with
its full state, delegation links, complete journal across all revisions
(each entry carries its `position` and `revision`, so any single revision's
effective journal is reconstructible), and tasks. The call graph, the current
journal, a specific revision, a task list — every narrower view is a
client-side grouping of that one payload. The server owns the fold; the
terminal owns the rendering. There is no separate `/graph`, `/journal`, or
`/tasks` endpoint by design.

Manifests arrive per-process from the client and are validated server-side
(`aurora.ValidateManifest` against the compiled driver set); there is no
manifest entity in the core.

## Verification

```sh
go vet ./...
go test -race ./...
```

The end-to-end tests build the real Rust agent program from the sibling
`aurora-brains` checkout (`cargo build --target wasm32-wasip1`), drive it
through the HTTP API against a scripted OpenAI-compatible stub — including a
full distribution restart mid-timer-wait — and skip when the Rust toolchain
is unavailable.
