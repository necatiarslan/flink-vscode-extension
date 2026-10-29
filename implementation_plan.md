# Flink Manager for VS Code: Plan

I built the API mapping from the REST reference you linked. I haven't opened your airflow extension repo, so the structure is generic and easy to align with it.

## 1. Connection and authentication

**Connection profile** (one per cluster):

| Field | Notes |
|---|---|
| name, color/tag | e.g. "prod-eu", an env tag to warn before destructive actions |
| `baseUrl` | e.g. `https://flink.example.com:8081`, may include a path prefix when behind a proxy |
| auth type | see below |
| TLS options | custom CA, client cert/key, "allow self-signed" (off by default) |
| refresh interval | default 5-10s, only while something is visible |
| `readOnly` flag | hides or blocks all mutating actions |
| `sqlGatewayUrl` | optional, for a later phase |

**Storage:** non-secret fields go in settings (`flink.clusters`) or `globalState`. Tokens, passwords and key passphrases go in `context.secrets` (SecretStorage), never in settings.json.

**Auth methods.** Flink's REST endpoint has no user authentication of its own. Its built-in security is TLS and mutual TLS, so most real setups put a proxy or gateway in front. Support these in order:

1. **None**, for local, docker and dev clusters.
2. **Bearer token or custom header(s)**, for gateways and ingress.
3. **Basic auth**, usually via a reverse proxy.
4. **mTLS** (client cert, key, CA), matching Flink's `security.ssl.rest.*` options.
5. **Kubernetes port-forward helper**: pick a context, namespace and `<name>-rest` service, and the extension runs `kubectl port-forward` and points the profile at localhost. This is very common with the Flink Kubernetes Operator.
6. **Later:** AWS Managed Service for Apache Flink (dashboard access through a presigned URL), and YARN proxy with Kerberos/SPNEGO. Both are niche and complex, so keep them behind a "custom auth provider" interface.

**Implementation:** use Node's `https.Agent` / `fetch` inside the extension host, not the webview, so certs and secrets never reach the browser context. Wrap everything in one `FlinkClient` class with timeouts, `AbortController`, retries for GETs only, and typed errors (401/403, TLS, unreachable, 404 for unsupported version).

**"Test connection"** calls `GET /config` (returns the Flink version) and `GET /overview`. Cache the version and gate features on it, since some endpoints differ across versions.

## 2. Functionality

**MVP (read-only)**
- Manage cluster profiles (add, edit, remove, test)
- Cluster overview (slots, TMs, job counts, version)
- Job list and job detail (status, plan, exceptions, config)
- Task Manager list and detail
- JobManager config and logs list

**v1 (operations)**
- Cancel job
- Trigger savepoint, stop with savepoint (with async status tracking)
- Trigger checkpoint
- Upload jar, submit job, delete jar
- Checkpoint history and details

**v2 (diagnostics)**
- Vertex detail, backpressure, flamegraph, watermarks, subtask times
- Metrics explorer with charts
- Thread dumps (JM/TM)
- Rescale job
- Savepoint disposal

**Later**
- SQL Gateway (run SQL, sessions)
- Flink Kubernetes Operator CRDs
- Multi-cluster compare
- Alerts on job failure via notifications

## 3. Tree view

```
FLINK CLUSTERS
├─ ● prod-eu                      (status: connected / error)
│  ├─ Overview
│  ├─ Jobs
│  │  ├─ Running (3)
│  │  │  └─ ▶ orders-enrichment    [cancel] [savepoint] [open]
│  │  ├─ Finished (12)
│  │  ├─ Failed (1)
│  │  └─ Canceled (2)
│  ├─ Task Managers (4)
│  │  └─ tm-1  (slots 2/4)
│  ├─ Job Manager
│  ├─ Jars (5)
│  └─ Savepoints          (from tracked operations)
└─ ○ local-dev
```

- `contextValue` values like `cluster`, `job.running`, `job.failed` and `taskmanager` drive the right-click menus and inline icons.
- Groups lazy-load, with counts coming from `/jobs/overview`.
- Click opens a webview panel, and the same panel is reused per resource (keyed by `clusterId:jobId`).
- Welcome view with an "Add cluster" button when empty.
- Status icons via `ThemeIcon` (running, failed and so on), and a `description` showing duration or parallelism.

## 4. Screens and APIs

| Screen (webview) | Endpoints |
|---|---|
| **Cluster Overview** | `GET /overview`, `GET /config`, `GET /jobmanager/config`, `GET /jobmanager/metrics` |
| **Jobs list** | `GET /jobs/overview`, `PATCH /jobs/:id?mode=cancel` |
| **Job detail: Overview + DAG** | `GET /jobs/:id`, `GET /jobs/:id/plan` |
| **Job detail: Exceptions** | `GET /jobs/:id/exceptions` (`maxExceptions`, `failureLabelFilter`) |
| **Job detail: Checkpoints** | `GET /jobs/:id/checkpoints`, `/checkpoints/config`, `/checkpoints/details/:cpid`, `.../subtasks/:vertexid`, `POST /jobs/:id/checkpoints` + `GET .../checkpoints/:triggerid` |
| **Job detail: Config** | `GET /jobs/:id/config`, `/jobs/:id/jobmanager/config` |
| **Vertex detail** | `GET /jobs/:id/vertices/:vid`, `/backpressure`, `/flamegraph`, `/watermarks`, `/subtasktimes`, `/taskmanagers`, `/metrics`, `/subtasks/:idx`, `/attempts/:n` |
| **Job metrics** | `GET /jobs/:id/metrics?get=...`, `/jobs/metrics?agg=...` |
| **Savepoint dialog** | `POST /jobs/:id/savepoints` then poll `GET /jobs/:id/savepoints/:triggerid` |
| **Stop with savepoint** | `POST /jobs/:id/stop` then poll the same savepoints status endpoint |
| **Rescale / resources** | `PATCH /jobs/:id/rescaling?parallelism=` + `GET .../rescaling/:triggerid`, `GET/PUT /jobs/:id/resource-requirements` |
| **Task Managers list** | `GET /taskmanagers`, `/taskmanagers/metrics` |
| **Task Manager detail** | `GET /taskmanagers/:id`, `/logs`, `/metrics`, `/thread-dump` |
| **Job Manager** | `/jobmanager/config`, `/environment`, `/logs`, `/metrics`, `/thread-dump` |
| **Jars + Submit** | `GET /jars`, `POST /jars/upload`, `DELETE /jars/:id`, `POST /jars/:id/plan`, `POST /jars/:id/run` (entry-class, programArg, parallelism, savepointPath, allowNonRestoredState) |
| **Savepoint disposal** | `POST /savepoint-disposal` + `GET /savepoint-disposal/:triggerid` |
| **Shutdown cluster** | `DELETE /cluster` (heavy confirmation, off by default) |

Notes on the API:
- Async operations (savepoints, checkpoints, rescale, disposal) return a `triggerid`. Build one reusable `AsyncOperationTracker` that polls and shows progress via `withProgress` or the status bar. For savepoints and stop, you can supply your own `triggerId` in the body, which makes retries safe.
- The jar endpoints only work on session clusters. Application-mode clusters don't allow upload, so hide the "Jars" node when `/jars` fails, and don't treat that as an error.
- Reading log file contents (`/jobmanager/logs/:file`, `/taskmanagers/:id/logs/:file`) is used by the dashboard but isn't listed in the reference page you linked. Verify those against your target Flink version before relying on them.
- Version prefixes (`/v1/...`) are optional. Leave them off unless you need a specific one.
- Flink also has a separate SQL Gateway REST API (its own service, default port 8083). Treat it as a distinct client for a later phase.

## 5. Architecture

```
src/
  extension.ts              activate, register commands/views
  config/ClusterStore.ts    profiles + SecretStorage
  api/FlinkClient.ts        HTTP, TLS, auth, retries, typed errors
  api/types.ts              response models (hand-written, per endpoint)
  api/AsyncOperationTracker.ts
  tree/ClusterTreeProvider.ts, nodes/*.ts
  panels/BasePanel.ts       webview lifecycle, CSP, message bus
  panels/{Overview,Job,TaskManager,Jars,...}Panel.ts
  services/PollingService.ts   only polls what's visible
  webview-ui/               frontend source, bundled by esbuild
  util/logger.ts            OutputChannel
```

- **Webview rules:** strict CSP with nonces, bundle all assets locally, and use a typed message protocol (`{type, payload}`) between host and webview. All HTTP stays in the extension host.
- **Frontend:** plain TypeScript or a light framework (Lit, Preact or Svelte) with VS Code CSS variables for theming. Avoid the Webview UI Toolkit, which is deprecated. For the job DAG use dagre or ELK with SVG, and for charts use uPlot or Chart.js.
- **Polling:** pause when the panel is hidden (`onDidChangeViewState`), back off on errors, and let a manual refresh override the interval.
- **Safety:** confirmation dialogs for cancel, stop and shutdown, a `readOnly` profile flag, and production-tagged clusters that require typing the job name to confirm.
- **Testing:** unit tests for `FlinkClient` against a mock HTTP server, plus integration tests in CI against a docker-compose Flink session cluster (a couple of pinned Flink versions). `@vscode/test-electron` covers the activation and tree tests.
- **Distribution:** publish to both the VS Code Marketplace and Open VSX, and add an extension pack or recommendation for the Kubernetes tools.

## 6. Roadmap

1. **Skeleton:** activation, profile CRUD, `FlinkClient`, tree with clusters, test connection.
2. **Read-only MVP:** overview, jobs list/detail with DAG and exceptions, TMs, JM config.
3. **Operations:** cancel, savepoint, stop, checkpoint trigger, async tracker.
4. **Jars and submit**, plus checkpoint history.
5. **Diagnostics:** vertices, backpressure, flamegraph, metrics explorer, thread dumps.
6. **Extras:** K8s port-forward helper, SQL Gateway, and other auth providers.

## 7. Decisions to make early

- Which deployment modes to target first (standalone/session, Kubernetes operator, YARN, managed services)? This decides the auth and jar features that matter.
- Minimum supported Flink version (for example 1.17+)?
- Should destructive actions default to off or require confirmation?
- Single extension or a core package plus optional integrations?

