# Flink VS Code Extension

Manage Apache Flink REST clusters from the VS Code Explorer.

## Getting started

1. Open the **Apache Flink** activity bar view.
2. Select **Add Cluster** and enter a name and Flink REST URL. For a local cluster, use `http://localhost:8081`.
3. Use **Test Connection** on the cluster to verify its REST API.
4. Expand the cluster to inspect its overview, jobs, Task Managers, and JobManager.

Cluster profiles are stored in VS Code global state. The current MVP uses unauthenticated HTTP/HTTPS REST endpoints and makes read-only API requests. Resource details open as formatted JSON documents. Authentication providers, write operations, and SQL Gateway support are not included yet.

## Included read-only views

- Cluster overview and Flink version
- Jobs grouped by status, with job overview, plan, exceptions, and configuration
- Task Manager list, details, and logs
- JobManager configuration and logs

The extension communicates with the Flink REST API from the extension host. For clusters behind a proxy, enter the full REST base URL, including any path prefix.

## Development

- `npm run compile` builds the extension.
- `npm run lint` lints the TypeScript source.
- `npm test` runs both checks.
