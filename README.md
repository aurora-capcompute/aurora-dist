# aurora-dist

The Aurora distribution: **one binary** assembling the
[`aurora-capcompute`](https://github.com/aurora-capcompute/aurora-capcompute)
runtime with a compiled-in driver set and concrete stores, exposing the
runtime over **one HTTP+SSE API** — the single way in, versioned `/v1` from
birth.

The cores stay interfaces-only; this repo is where the choices live:

- **Drivers** (from `aurora-dispatchers`): the builtin leaf router,
  `core.internet`, `core.mcp`, `core.memory`, `core.timer`, `core.log`, and
  `core.openaiApi` (the `openaillm` cognition driver).
- **Stores** (absorbed from the deprecated `aurora-stores`): an in-memory set
  for throwaway runs and a SQLite store — append-only event log (one stream
  per session, `session_id`/`process_id` vocabulary), lease table,
  hash-chained kernel journal store with a `VerifyJournal` audit path, and
  the tenant-memory KV behind `core.memory`.
- **Runtime-adjacent services** that must not live in terminals:
  - **Timer firing** — durable `timer.set` tasks are armed from the event
    stream and resolved at their deadline; restart recovery re-arms pending
    timers from persisted state and fires elapsed ones immediately.
  - **Program registry + retention** — programs load from a directory of
    `*.wasm` artifacts (id = file name), hot-reload via
    `POST /v1/programs/reload` (digest-diffed), and
    `GET /v1/programs/retention` answers the decommissioning gate: which
    digests are still referenced by non-terminal processes
    (drain-and-deprecate).
  - **Tenant event firehose** — `GET /v1/events` merges every session's
    events into one resumable SSE stream (per-session SSE alone cannot serve
    a connector). At-least-once: resume inside the replay ring continues
    seamlessly (`Last-Event-ID` / `?after=`), an older cursor re-syncs from a
    fresh `snapshot` event; a subscriber that cannot keep up is disconnected
    rather than silently skipped.
  - **Capability ceiling** — an operator-configured list of capability names;
    process creation refuses manifests granting beyond it (`sys.Attenuate` at
    the door, recursing through `core.agent` trees). Defense in depth against
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
  "mcp_servers": {"docs": {"command": "docs-mcp"}},
  "capability_ceiling": ["timer.set", "openai.chat", "openai.responses",
                          "openai.embeddings", "openai.models.list"]
}
```

## API (/v1)

| Method & path | Meaning |
| --- | --- |
| `GET /v1/events` | tenant firehose (SSE; resume via `Last-Event-ID`/`?after=`) |
| `GET /v1/sessions` · `POST /v1/sessions` | list / create sessions |
| `GET /v1/sessions/{id}` · `/graph` · `/events` (SSE) | session snapshot, graph, per-session stream |
| `POST /v1/sessions/{id}/processes` | start a process: `{message, manifest}` |
| `GET /v1/processes/{id}` · `/graph` · `/journal` · `/journal/revisions` · `/tasks` | process projections |
| `POST /v1/processes/{id}/stop` · `/retry` | steer (`{"mode":"resume"\|"restart"}`) |
| `POST /v1/tasks/{id}/resolve` | `{resolution_token, resolution:{decision,...}}` |
| `GET /v1/programs` · `POST /v1/programs/reload` · `GET /v1/programs/retention` | program registry |

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
