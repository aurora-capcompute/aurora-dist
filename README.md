# aurora-dist

**The Aurora server you actually run.** `aurora-dist` is *one binary* that bundles
the whole Aurora agent runtime — kernel, orchestration, capability drivers, and
storage — and exposes it over *one* HTTP API (`/v1`). Start it, point a client at
it, and you're running AI agents locally.

> New here? This is the best place to start if you want to **run Aurora**. Read
> [What is this](#what-is-this-in-plain-words), then follow
> [Run the whole stack locally](#run-the-whole-stack-locally-5-minutes).

---

## What is this, in plain words?

Aurora's core libraries are deliberately abstract — they define *interfaces* and
leave the concrete choices open. Someone has to pick the real drivers, pick a real
database, run the background housekeeping, and put an API in front. **That's
`aurora-dist`.** ("dist" = *distribution*, in the Linux‑distro / "batteries‑included
build" sense — not "distributed node.")

It takes the [aurora-capcompute](https://github.com/aurora-capcompute/aurora-capcompute)
runtime and:

- compiles in a fixed set of **drivers** (internet, LLM, memory, filesystem, …),
- wires in a concrete **store** (in‑memory for throwaway runs, or SQLite for
  durability),
- runs the always‑on **services** (firing timers, hot‑reloading programs, enforcing
  a capability ceiling), and
- serves it all behind a single versioned **HTTP `/v1` API**.

The result is the deployable server that a client like
[aurora-cli](https://github.com/aurora-capcompute/aurora-cli) or the
[Slack bot](https://github.com/aurora-capcompute/aurora-slack-connector) talks to.

## Where this fits in the Aurora system

```
        you (a human)
              │
   aurora-cli / aurora-slack-connector      ← clients you talk to
              │  HTTP /v1
         aurora-dist                         ◀── YOU ARE HERE (the server)
              │  assembled from…
   ┌──────────┴──────────┐
 aurora-capcompute    aurora-dispatchers     ← orchestration runtime + capability drivers
   └──────────┬──────────┘
              │  both built on
         capcompute                          ← the kernel (the foundation)

   aurora-brains  →  the Wasm agent "programs" it loads and runs
```

## What it does (features)

| Feature | What it means |
| --- | --- |
| **One binary, one API** | Versioned `/v1` HTTP API from the start — the single way in |
| **Compiled‑in drivers** | `core.internet`, `core.memory`, `core.scratch`, `core.filesystem`, `core.httpTemplate`, `core.openaiApi` (LLM), plus `sys.spawn` and `sys.timer` |
| **Two store backends** | In‑memory (nothing survives restart) or SQLite (durable append‑only event log, leases, KV) — chosen by whether you set `-data` |
| **Durable timers** | `sys.timer` fires whether or not a client is attached; fire times are absolute and re‑armed at boot, so a restart never shifts a deadline |
| **Program hot‑reload** | Loads `<name>.wasm` + `<name>.json` pairs from a directory and re‑scans on a ticker; edit a program and it reloads (a `.wasm` with no `.json` is refused) |
| **Capability ceiling** | An operator allowlist of capability names; process creation refuses any manifest that grants beyond it (recursing through `sys.spawn`) |
| **Crash recovery** | On boot it re‑drives every process a host failure left mid‑flight, idempotently |
| **Host‑held secrets** | Manifests reference a secret by name; the value comes from `AURORA_SECRET_<NAME>` and never enters the manifest, journal, or guest |
| **"One read, many views"** | `GET /v1/sessions/{id}` returns the entire session log; every narrower view is a client‑side grouping of that one payload |

## Run the whole stack locally (5 minutes)

This gets you a working agent you can talk to from your terminal.

**Prerequisites:** Go 1.26+. For a real agent program, also a Rust toolchain with
the `wasm32-wasip1` target. For talking to a real LLM, an OpenAI‑compatible API key.

**1. Build an agent program** (from
[aurora-brains](https://github.com/aurora-capcompute/aurora-brains), cloned beside
this repo):

```sh
cd ../aurora-brains
rustup target add wasm32-wasip1
sh programs/agent/build.sh          # → programs/agent/dist/{agent.wasm, agent.json}

mkdir -p ../aurora-dist/programs
cp programs/agent/dist/agent.* ../aurora-dist/programs/
```

**2. Build and run the server:**

```sh
cd ../aurora-dist
go build ./cmd/aurora-dist          # → ./aurora-dist

export AURORA_TASK_SECRET=change-me-at-least-16-bytes   # required, ≥ 16 bytes
./aurora-dist -addr :8080 -data ./data -programs ./programs
```

That's it — the server is up on `http://localhost:8080`. Check it:

```sh
curl http://localhost:8080/healthz      # → ok
curl http://localhost:8080/v1/programs  # the loaded agent + its input/output schema
```

**3. Drive it.** Use [aurora-cli](https://github.com/aurora-capcompute/aurora-cli):

```sh
aurora mount http://127.0.0.1:8080
aurora mkdir demo && aurora cd demo
export AURORA_MANIFEST=manifest.json    # the agent's grant set (see below)
aurora spawn "say hello"
```

### The simplest possible run

No program directory, no Rust, nothing persisted — just boot the server on
in‑memory stores to poke the API:

```sh
export AURORA_TASK_SECRET=change-me-at-least-16-bytes
./aurora-dist -addr :8080            # no -data → in-memory; no -programs → zero programs
```

## Configuration

**Flags** (flag > config‑file field > default):

| Flag | Default | Meaning |
| --- | --- | --- |
| `-addr` | `127.0.0.1:8080` | Listen address (loopback by default — see the security note) |
| `-data` | *(empty)* | Data directory for SQLite; empty = in‑memory |
| `-programs` | *(empty)* | Directory of `<name>.wasm` + `<name>.json` program pairs |
| `-default-program` | | Default program id |
| `-tenant` | `local` | Tenant id |
| `-config` | | Path to a JSON config file |
| `-version` | | Print version and exit |

**Environment variables:**

- `AURORA_TASK_SECRET` (or `…_FILE`) — **required**, ≥ 16 bytes. Keys the bearer
  tokens that authenticate task resolution.
- `AURORA_SECRET_<NAME>` (or `…_FILE`) — value for a manifest secret reference named
  `<NAME>`. The `_FILE` form keeps secrets out of the environment.
- `AURORA_AUDIT_KEY` (or `…_FILE`) — optional; keys the credential fingerprints in
  audit logs.

**Config file** (`-config dist.json`):

```json
{
  "addr": ":8080",
  "tenant_id": "local",
  "data_dir": "./data",
  "programs": {"dir": "./programs", "default": "agent"},
  "capability_ceiling": ["sys.timer", "core.openaiApi"]
}
```

### A starter manifest

A manifest is the grant set for each process — it names the LLM driver and the leaf
capabilities the agent may use. Point `$AURORA_MANIFEST` at a file like:

```json
{
  "version": 4,
  "syscalls": [
    {
      "syscall": "core.openaiApi", "hidden": true,
      "base_url": "https://api.openai.com/v1",
      "api_key": {"secret": "OPENAI_KEY"},
      "default_model": "gpt-4o",
      "capabilities": [{"operation": "chat", "require_approval": false}]
    },
    {"syscall": "core.internet",
     "capabilities": [{"methods": ["GET"], "domain": "status.example.com"}]},
    {"syscall": "sys.timer"}
  ]
}
```

The `api_key` here is a **secret reference**, not the key itself: the value is
resolved host‑side from the matching environment variable and never enters the
manifest, journal, or guest. Set it before starting the server:

```sh
export AURORA_SECRET_OPENAI_KEY=sk-...        # or AURORA_SECRET_OPENAI_KEY_FILE=/run/secrets/openai
```

(A literal `"api_key": "sk-..."` still works but is discouraged — it puts the key in
the manifest file.)

> **Security note.** There is deliberately **no principal/API authentication** —
> `aurora-dist` serves one trusted local client, so it binds to `127.0.0.1` by
> default. The `-addr :8080` example above exposes it on all interfaces; only do
> that behind network isolation. (Task resolution still authenticates its bearer
> `resolution_token`.)

## The `/v1` API

| Method & path | Meaning |
| --- | --- |
| `GET /v1/sessions` · `POST /v1/sessions` | list summaries / create a session |
| `GET /v1/sessions/{id}` | **the one comprehensive read** — the whole session log |
| `POST /v1/sessions/{id}/processes` | start a process: `{message, manifest}` |
| `GET /v1/processes/{id}` | cheap single‑process status poll |
| `POST /v1/processes/{id}/stop` · `/retry` | steer (`{"mode":"resume"\|"restart"}`) |
| `POST /v1/tasks/{id}/resolve` | `{resolution_token, resolution:{decision,…}}` |
| `GET /v1/programs` | the loaded programs, each with its interface schema |
| `GET /healthz` | returns `ok` |

`GET /v1/sessions/{id}` returns everything about a session — metadata, history, and
every process with its full state, delegation links, journal across all revisions,
and tasks. The call graph, a single revision, a task list — every narrower view is a
client‑side grouping of that one payload. There is no separate `/graph`,
`/journal`, or `/tasks` endpoint by design.

## Project layout

```
cmd/aurora-dist/main.go     THE binary: flag/env parsing, HTTP server lifecycle
internal/dist/
  dist.go                   assembles stores + drivers + runtime + background loops
  ceiling.go                the capability-ceiling gate at process creation
  secrets.go                AURORA_SECRET_* name → value resolution
  log.go                    the one comprehensive session read (the fold)
  api/                      the /v1 HTTP handlers
  programs/                 loads *.wasm + *.json pairs, hot-reload
  timers/                   the durable sys.timer reconcile/fire loop
  store/memory/             in-memory event log, leases, KV, process table
  store/sqlite/             durable SQLite store
```

## Verification

```sh
go vet ./...
go test -race ./...   # end-to-end tests build the Rust agent from a sibling
                      # aurora-brains checkout; they auto-skip if the toolchain is absent
```

## Related repos

- [capcompute](https://github.com/aurora-capcompute/capcompute) — the kernel
- [aurora-capcompute](https://github.com/aurora-capcompute/aurora-capcompute) — the runtime this server assembles
- [aurora-dispatchers](https://github.com/aurora-capcompute/aurora-dispatchers) — the drivers compiled in
- [aurora-brains](https://github.com/aurora-capcompute/aurora-brains) — the agent programs you drop in `-programs`
- [aurora-cli](https://github.com/aurora-capcompute/aurora-cli) — the terminal client for this API
- [aurora-slack-connector](https://github.com/aurora-capcompute/aurora-slack-connector) — a Slack bot client for this API
