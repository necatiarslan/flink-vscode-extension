# Apache Flink Manager for VS Code

![Apache Flink cluster explorer and overview panel](media/readme/main-screen.png)

Manage Apache Flink clusters directly from VS Code. Browse jobs, Task Managers, checkpoints, and JARs, inspect diagnostics in dedicated panels, and run cluster operations without leaving your editor.

## Features

### 🌳 Cluster Explorer
- Manage multiple Flink REST endpoints from one Explorer view
- Add, edit, remove, refresh, and test cluster profiles
- Support local clusters and REST URLs behind a proxy or path prefix
- Store profiles in VS Code global state

### 📊 Cluster and Job Insights
- View Flink version, job counts, Task Managers, and slot availability
- Group jobs by status and inspect job details, execution plans, exceptions, and configuration
- Inspect job status, accumulators, execution results, resource requirements, and rescale history
- Browse checkpoint history, checkpoint details, and vertex subtask data
- Inspect vertex and subtask accumulators, attempts, and aggregated metrics
- Open aggregate job and Task Manager metrics, and view job IDs and statuses
- Open resource-specific panels with summaries and tables instead of raw JSON

### 🧭 Task Manager and JobManager Diagnostics
- Inspect Task Manager details, hardware, slots, logs, and aggregate metrics
- View JobManager configuration, runtime environment, and logs, including per-job log URLs
- Open JobManager and Task Manager thread dumps
- Explore vertex details, backpressure, flamegraphs, watermarks, and subtask timings
- Inspect subtask details and metrics, and retrieve attempt details and accumulators

### ⚙️ Flink Operations
- Cancel jobs, trigger checkpoints and savepoints, stop jobs with savepoints, and rescale jobs
- Upload JARs with the **+** action on the Jars tree item; plan, submit jobs or applications from,
  and delete session-cluster JARs
- Track savepoints created by the extension, restart a job from a tracked savepoint, and request
  asynchronous savepoint disposal
- Update job resource requirements and inspect rescale configuration, history, and summaries
- Follow progress for supported asynchronous operations

### 📈 Metrics
- Browse JobManager, Task Manager, job, and vertex metrics
- Chart numeric metrics with periodic samples
- Pause metric polling when the panel is hidden

### 🧾 Flink SQL
- Run the selection or full contents of a `.sql` file through a Flink SQL Gateway
- Review statement results, row kinds, truncation status, and submitted INSERT jobs
- Open an INSERT job directly from the results panel
- Build a streaming job in the **Flink: New Job** wizard with event-time or processing-time sources,
  window aggregates, Top-N, and deduplication; save or load `.flinkjob.json` specs
- Validate generated SQL with the gateway before submitting; the gateway-resolved schema drives column pickers
- Preview the source or any step in a bounded temporary session, with row limits, cancellation,
  changelog inspection, and client-side final-state reduction
- Infer editable source fields from a local JSON Lines or CSV sample; sample contents stay on the host

## Getting Started

1. Open the **Apache Flink** icon in the VS Code Activity Bar.
2. Select **Add Cluster** and provide a name and Flink REST URL. For a local cluster, use `http://localhost:8081`.
3. Use the cluster's **Test Connection** action to check the REST API and Flink version.
4. Expand the cluster to browse jobs, Task Managers, JobManager resources, savepoints, and JARs.
   Select a resource to open its detail panel, or expand a job, vertex, JAR, or tracked savepoint
   to access its available actions.

Use **Add Cluster** and **Refresh** in the view title. Cluster actions such as edit, remove, and
test connection are available on the cluster tree item. Use **+** beside **Jars** to upload a JAR.
Expand **Savepoints** and then a tracked savepoint to restart a job from it.

### SQL Gateway setup

The local Compose stack includes a Flink 1.20 SQL Gateway on port 8083. Start it with:

```sh
docker compose up -d
```

Add or edit a cluster profile and set **SQL Gateway URL** to `http://localhost:8083`.
**Test Connection** checks the JobManager REST API and reports the SQL Gateway API versions.
Open a `.sql` file and choose **Flink: Run SQL** from the editor title or context menu. The
default maximum result size is 100 rows; configure `flink.sql.maxRows` and
`flink.sql.statementTimeoutSeconds` in VS Code settings as needed. The timeout is enforced by
the extension because Flink 1.20's SQL Gateway does not implement the optional server-side
`executionTimeout` request field.

Choose **Flink: New Job** from the Command Palette or a gateway-enabled cluster's context menu.
The wizard supports datagen and filesystem sources, filters, projections, JSON extraction,
split-to-rows, grouped aggregates, event-time and processing-time attributes, tumble/hop/cumulate/
session windows, Top-N, deduplication, and print, blackhole, or filesystem sinks. Joins and
Kafka/JDBC connectors are not included. Filesystem sinks require append-only results; the
validator checks changelog compatibility before sending SQL.

The packaged job templates are available under `examples/jobs/`:

- Open [Spec A](examples/jobs/spec-a-aggregate.flinkjob.json) for a JSON filter and aggregate,
  [Spec B](examples/jobs/spec-b-word-count.flinkjob.json) for a watched raw-text word count,
  and Specs C–H for windows, Top-N, and deduplication. Use **Flink: New Job** → **From template**
  to load a starter spec.
- To feed Spec B, run `npm run producer:files`; the development-only tool writes sample lines into
  `flink-data/input`, shared with the Docker Flink services.
- To feed event-time examples, run `npm run produce:events -- flink-data/input`. This development
  tool writes one atomic JSON event file per second with second-precision ISO-8601 timestamps.

On Flink 1.20, both `keep: first` and `keep: last` deduplication plans are classified as updating
changelogs, so they cannot be written to a filesystem sink under the append-only rule. HOP slide
may exceed its window size in Flink 1.20; the validator follows that behavior rather than imposing
an extra restriction. CUMULATE max size must be a multiple of its step.

The wizard uses a temporary SQL Gateway session to create the source and step views and run
`DESCRIBE`; it then uses the returned Flink schema for column selection and sink generation.
**Validate** runs `EXPLAIN INSERT`; **Submit** requires modal confirmation and refreshes the job
tree after Flink returns a job ID. Close of the SQL session does not cancel the submitted job.

Select **Preview** on the source or any step card to inspect bounded sample rows; the Run tab
previews the final step. Preview sessions are cancelled when a newer preview starts, and the
configured row limit and timeout apply. A preview omits filesystem monitoring and bounds datagen
to `flink.preview.maxRows * 4` records. For a watched filesystem source, it reads files already in
the mounted folder; it does not wait for new files. Empty or timed-out filesystem previews point
out that the folder may be empty or its path may be wrong inside the Flink container.

Use **Infer from sample file...** in the Source tab to inspect a local JSON Lines or CSV file.
Inference reads at most 200 lines or 64 KB, whichever comes first. Review and edit the suggested
fields before applying them; inference never sends sample contents to Flink and never applies
suggestions automatically. A single inferred `TIMESTAMP(3)` field may be suggested as event time,
but that choice also requires explicit user confirmation.

See [docs/manual-qa.md](docs/manual-qa.md) for end-to-end checks covering SQL, templates, preview,
inference, read-only profiles, and packaging.

## Safety and Compatibility

- Cluster profiles can be marked read-only when adding or editing a profile. Read-only profiles
  allow only query, show, explain, and set statements in **Flink: Run SQL**, and block job
  submission and Flink REST mutations such as cancel, savepoint, checkpoint, rescale, and JAR changes.
- Mutating actions require confirmation; stop-with-savepoint and other disruptive actions use modal confirmation.
- The extension currently supports unauthenticated HTTP/HTTPS Flink REST and SQL Gateway endpoints.
  Authentication providers are not included.
- Read-only mode is an extension-side guard, not a substitute for Flink endpoint access controls.
- JAR upload, planning, and job submission are intended for session clusters; these endpoints may
  not be available on application-mode clusters.
- For a reverse proxy, include any required path prefix in the cluster REST URL.


## Support

- Report issues on [GitHub](https://github.com/necatiarslan/flink-vscode-extension/issues)
- Check [CHANGELOG.md](CHANGELOG.md) for version history

## Donate
If you find this extension helpful, consider supporting its development through a donation. Your contributions help maintain and improve the extension.
Click [here](https://github.com/sponsors/necatiarslan) to donate. Thank you for your support!
