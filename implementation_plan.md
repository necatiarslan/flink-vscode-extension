# Flink REST API implementation plan

This plan compares the extension's current REST calls with `rest_v1_dispatcher.yml`.
The dispatcher specification is the contract for endpoint paths, HTTP methods, parameters,
and request bodies. The extension currently targets a session-cluster workflow; not every
dispatcher endpoint is relevant to that workflow.

## Current coverage

Implemented route groups:

- Cluster overview: `GET /overview`, `/config`, `/jobmanager/config`, `/jobmanager/metrics`.
- Jobs: overview, detail, plan, exceptions, configuration, metrics, and `PATCH /jobs/{jobid}`
  cancellation.
- Checkpoints: history, configuration, details, per-vertex subtask details, triggering, and
  asynchronous trigger-status polling.
- Vertex diagnostics: details, backpressure, flamegraph, watermarks, subtask times, task managers,
  accumulators, JobManager operator metrics, aggregate subtask metrics, subtask/attempt details
  and accumulators, and metrics.
- Task Managers: list, details, log listing, metrics, and thread dump.
- JobManager: configuration, environment, log listing, metrics, and thread dump.
- Session JARs: list, upload, plan generation, delete, submit a job or application, and restart a
  job from a savepoint.
- Savepoints: trigger, stop with savepoint, track results locally, dispose, and poll asynchronous
  operations.
- Job controls: status, accumulators, execution result, JobManager resources/log URLs, resource
  requirements, and rescale configuration/history/details/overview/summary.
- Cluster aggregations: job and Task Manager metrics, plus the IDs/statuses job listing.

## Contract mismatches

The following request mismatches have been fixed:

- JAR plan generation now uses `POST /jars/{jarid}/plan` with a JSON body.
- Regular JAR submission now uses the documented JSON body fields, including `programArgsList`
  rather than the nonstandard `program-args` query parameter.
- Savepoint and stop-with-savepoint requests now use the field names defined by their respective
  schemas (`cancel-job` / `target-directory` for savepoints; `drain` / `targetDirectory` for stop).
- Savepoint disposal now sends `savepoint-path`; the trigger ID is taken from the response rather
  than sent in an unsupported request field.

**Still to verify:** Log contents are fetched through paths absent from this dispatcher spec. The extension
   appends a selected filename to the JobManager or Task Manager log-list route. Verify these
   routes against the supported Flink versions or present log listings without attempting
   unsupported file reads.

## Remaining dispatcher functionality

Prioritize by the current session-cluster use case. These are not necessarily defects when the
corresponding deployment mode or workflow is intentionally out of scope.

### Lower priority or deployment-specific

- Application-mode routes: application overview/details/exceptions, cancellation, and
  application JobManager configuration under `/applications/...`.
- Dataset listing, deletion, and deletion-status routes: `GET /datasets`,
  `DELETE /datasets/{datasetid}`, and `GET /datasets/delete/{triggerid}`.
- `PATCH /jobs/{jobid}/clientHeartbeat` has no current user-facing workflow; do not add without
  a defined use case.
- `DELETE /cluster` shuts down the Flink cluster. Keep out of the UI unless explicitly requested
  and protected by a strong confirmation.

## Implementation and verification sequence

1. Fix the contract mismatches above and add request-shape tests for methods, paths, query
   parameters, and JSON bodies using a mock HTTP server.
2. Add high-priority job/JAR resources with explicit UI actions and useful response panels.
3. Add medium-priority diagnostics and resource controls, validating each against the dispatcher
   schema and supported Flink versions.
4. Consider deployment-specific and destructive endpoints only when a clear workflow justifies
   them.

For every async operation, verify both the trigger request and its status route. Keep all REST
traffic in the extension host, and avoid treating unsupported endpoints as successful empty
results.
