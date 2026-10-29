# Flink VS Code Extension

Manage Apache Flink REST clusters from the VS Code Explorer.

## Getting started

1. Open the **Apache Flink** activity bar view.
2. Select **Add Cluster** and enter a name and Flink REST URL. For a local cluster, use `http://localhost:8081`.
3. Use **Test Connection** on the cluster to verify its REST API.
4. Expand the cluster to inspect its overview, jobs, Task Managers, JobManager, checkpoints, and JARs.

Cluster profiles are stored in VS Code global state. Profiles default to **Read-only**; edit a profile and choose **Allow operations** to enable mutations. Existing profiles are treated as read-only. Actions that cancel, stop, rescale, delete, submit, or dispose ask for confirmation. Resource details open as formatted JSON documents.

## Included functionality

- Cluster overview and Flink version; jobs grouped by status, with overview, plan, exceptions, configuration, and checkpoints
- Task Manager list, details, logs, metrics charts, and thread dumps
- JobManager configuration, environment, logs, metrics charts, and thread dumps
- Checkpoint history, details, and per-vertex subtask details
- Cancel jobs, trigger savepoints/checkpoints, stop with savepoint, and rescale jobs
- Upload, submit, plan, and delete session-cluster JARs
- Savepoint tracking and asynchronous savepoint disposal
- Vertex details, backpressure, flamegraphs, watermarks, subtask times, Task Managers, and metrics charts

The extension communicates with the Flink REST API from the extension host. For clusters behind a proxy, enter the full REST base URL, including any path prefix. This release supports unauthenticated HTTP/HTTPS REST endpoints; authentication providers and SQL Gateway support are not included.

## Development

- `npm run compile` builds the extension.
- `npm run lint` lints the TypeScript source.
- `npm test` runs both checks.
