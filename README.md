# aurora-dist

`aurora-dist` is the Aurora server: one binary with one HTTP API (`/v1`). It
assembles the [aurora-capcompute](https://github.com/aurora-capcompute/aurora-capcompute)
runtime with:

- a fixed set of **drivers**: `core.internet`, `core.httpTemplate`,
  `core.command`, `core.scratch`, `core.openaiApi`, plus the runtime's
  `sys.timer` and `sys.declassify`.
- a **store**: in memory, or SQLite for durability.
- its **programs**, read from a directory at boot.

"Dist" means distribution, as in a Linux distribution. Clients such as
[aurora-cli](https://github.com/aurora-capcompute/aurora-cli) and the
[Slack connector](https://github.com/aurora-capcompute/aurora-slack-connector)
talk to it.

```
   aurora-cli / aurora-slack-connector      clients
              │  HTTP /v1
         aurora-dist                         the server (this repo)
              │
   ┌──────────┴──────────┐
 aurora-capcompute    aurora-dispatchers     runtime + drivers
   └──────────┬──────────┘
         capcompute                          the processor

   aurora-brains                             the Wasm programs it loads
```

## Behavior

- **Store.** Without `-data`, everything is in memory and a restart loses it.
  With `-data`, a SQLite file holds the event log and each session's
  `core.scratch` store.
- **Programs.** Each `<name>.wasm` needs a `<name>.json` interface beside it.
  The server reads them once at boot. To change a program, restart. A process
  bound to the old program cannot resume under the new one.
- **Crash recovery.** At boot the server runs again every process that a host
  failure interrupted. Replay serves recorded results. Only a call whose
  completion was not journaled runs again.
- **Timers.** The runtime fires `sys.timer` with no client attached. A fire time
  is absolute, so a restart does not move it. A timer that came due during
  downtime fires at boot.
- **Secrets.** A manifest names a secret. The value comes from
  `AURORA_SECRET_<NAME>` and never enters the manifest, the journal or the
  guest.
- **One read.** `GET /v1/sessions/{id}` returns the whole session. Clients
  group it into narrower views.

## Run locally

Requirements: Go 1.26 or later. For a real agent, a Rust toolchain with the
`wasm32-wasip1` target and an OpenAI-compatible API key.

1. Build the agent program in an [aurora-brains](https://github.com/aurora-capcompute/aurora-brains)
   checkout beside this repo:

   ```sh
   cd ../aurora-brains
   rustup target add wasm32-wasip1
   sh programs/agent/build.sh        # programs/agent/dist/{agent.wasm,agent.json}
   mkdir -p ../aurora-dist/programs
   cp programs/agent/dist/agent.* ../aurora-dist/programs/
   ```

2. Build and start the server:

   ```sh
   cd ../aurora-dist
   go build ./cmd/aurora-dist
   export AURORA_TASK_SECRET=change-me-at-least-16-bytes
   export AURORA_SECRET_OPENAI_KEY=sk-...
   ./aurora-dist -data ./data -programs ./programs
   curl http://127.0.0.1:8080/healthz       # ok
   curl http://127.0.0.1:8080/v1/programs   # the loaded programs and their schemas
   ```

3. Talk to it with [aurora-cli](https://github.com/aurora-capcompute/aurora-cli):

   ```sh
   aurora mount http://127.0.0.1:8080
   aurora mkdir demo && aurora cd demo
   export AURORA_MANIFEST=manifest.json
   aurora spawn "say hello"
   ```

A minimal `manifest.json`:

```json
{
  "version": 4,
  "program": "agent",
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

`api_key` is a reference, not the key. Every credential field works this way:
`api_key`, `inject_headers`, and secret values in a command's `env`. The server
refuses an inline value such as `"api_key": "sk-..."`.

## Configuration

| Flag | Default | Meaning |
| --- | --- | --- |
| `-addr` | `127.0.0.1:8080` | Listen address |
| `-data` | empty | SQLite data directory. Empty means in memory. |
| `-programs` | empty | Directory of `<name>.wasm` + `<name>.json` |
| `-config` | | JSON config file |
| `-version` | | Print the version and exit |

A flag overrides the config file. The config file:

```json
{
  "addr": "127.0.0.1:8080",
  "data_dir": "./data",
  "programs": {"dir": "./programs"},
  "max_concurrent_processes": 16,
  "quantum_deadline_seconds": 1800,
  "trusted_hosts": ["aurora-dist"]
}
```

- `quantum_deadline_seconds` bounds one run between two yields, including its
  LLM and tool calls (default 30 minutes). A run past it becomes
  `interrupted`, and a retry resumes it.
- `trusted_hosts` lists host names the API accepts besides `localhost` and IP
  addresses, for example the service name a connector uses. The API refuses any
  other name, to block DNS rebinding.

Environment (each also accepts a `_FILE` form that names a file):

- `AURORA_TASK_SECRET`: required, at least 16 bytes. Keys the task resolution
  tokens.
- `AURORA_SECRET_<NAME>`: the value of the manifest secret `<NAME>`.
- `AURORA_AUDIT_KEY`: optional. Keys the credential fingerprints in results.

## Deployment

The API has no principal authentication. It serves one trusted client, so it
binds to loopback by default. Put it behind network isolation before you bind
it to any other address, and list the name clients use in `trusted_hosts`.
Task resolution still needs each task's token.

Confine the server, because `core.command` does not confine what an allowed
binary does:

- Run it as a dedicated user, in a container or pod.
- Give each command least-privilege credentials in its `env` (for example, a
  read-only service account).
- Limit egress in the platform (for example, a NetworkPolicy).
- Run one replica: SQLite has one writer, and the runtime reads the log once,
  at boot.

## The `/v1` API

| Method and path | Meaning |
| --- | --- |
| `GET /v1/sessions` | Session summaries |
| `POST /v1/sessions` | Create a session: `{name, tags}` |
| `GET /v1/sessions/{id}` | The whole session: metadata, history, every process with its journal and tasks |
| `POST /v1/sessions/{id}/rename` | Rename: `{name}` |
| `POST /v1/sessions/{id}/processes` | Start a process: `{input, manifest}` |
| `GET /v1/processes/{id}` | One process's status |
| `POST /v1/processes/{id}/stop` | Stop a process |
| `POST /v1/processes/{id}/retry` | `{"mode":"resume"}` or `{"mode":"restart"}` |
| `POST /v1/tasks/{id}/resolve` | `{resolution_token, resolution:{decision, …}}` |
| `GET /v1/programs` | The loaded programs and their interfaces |
| `GET /healthz` | `ok` |

## Layout

```
cmd/aurora-dist/main.go   flags, environment, HTTP server
internal/dist/
  dist.go                 assembles the stores, drivers, runtime and programs
  provider.go             builds each process's grant table
  secrets.go              AURORA_SECRET_* resolution
  api/                    the /v1 handlers
  programs/               reads *.wasm + *.json pairs
  store/memory/           in-memory event log and scratch
  store/sqlite/           SQLite event log and scratch
```

## Test

```sh
go vet ./...
go test -race ./...
```

The end-to-end tests build the agent from a sibling `aurora-brains` checkout.
They skip when `cargo` is not installed.
