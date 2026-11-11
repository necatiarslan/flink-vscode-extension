# Unreleased

- Add Flink 1.20 SQL Gateway profile setup, connection checks, and SQL file execution with results.
- Add SQL result paging and row limits, INSERT job links, SQL examples, and a local SQL Gateway service.
- Add read-only cluster profiles and enforce the SQL statement allow-list before execution.
- Add a typed JobSpec validator/compiler, live DESCRIBE schema resolution, and the New Job wizard.
- Add job-spec examples, a file-based word producer, and optional live-gateway integration tests.
- Move development data producers into tools and add an atomic event-time JSON producer.
- Add event-time watermarks, processing time, idle-timeout settings, window aggregates, Top-N, and
  deduplication to the JobSpec compiler and wizard, with Flink 1.20 changelog-aware sink validation.
- Add Spec C–H window, Top-N, and deduplication templates and update the producer and SQL support docs.
- Add bounded source/per-step Job Designer previews with changelog final-state display, cancellation,
  local JSONL/CSV schema inference, and end-to-end manual QA instructions.

# v1.0.2

- Add job restart from tracked savepoints, JAR application submission, and a Jars upload button.
- Refresh the JAR list after uploads and deletions.
- Add job status, accumulator, execution result, resource requirement, and rescaling views.
- Add vertex and subtask diagnostics, aggregate job and Task Manager metrics, and job-specific log
  URLs.
- Match JAR planning, job submission, savepoint, and savepoint-disposal requests to the Flink REST API specification.
- Enable cluster operations by default and remove the read-only profile option.
- Improve detail panels so long key labels do not wrap.

# v1.0.0
    -initial release
