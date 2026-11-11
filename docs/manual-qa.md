# Manual QA: Flink SQL and Job Designer (Phases 1–4)

Run the following checks from the repository root. Steps marked **UI** require the VS Code
extension; other steps can be run in a terminal.

## 1. Start and check the local Flink stack

```sh
docker compose up -d
curl -fsS http://localhost:8081/overview
curl -fsS http://localhost:8083/api_versions
```

**Expected:** the JobManager responds on port 8081, and the SQL Gateway reports supported API
versions on port 8083. In VS Code, add a cluster with REST URL `http://localhost:8081` and SQL
Gateway URL `http://localhost:8083`; **Test Connection** should report the Flink version and
Gateway API versions.

## 2. Run SQL files

**UI**

1. Open `examples/sql/hello-select.sql` and run **Flink: Run SQL** from the editor title or
   context menu.
2. Select a Gateway-enabled cluster if prompted.
3. Open `examples/sql/datagen-print.sql` and run it. Confirm the modal before its `INSERT`.

**Expected:** the first results panel shows `2`. The second file reports its INSERT job ID and
provides a button that opens that job in the existing job panel. Read-only profiles may run
query/show/explain/set statements, but must block INSERT; the blocked run explains that the
profile is read-only.

## 3. Feed filesystem sources

For the watched raw-text word-count template, start the producer and leave it running:

```sh
npm run producer:files
```

**Expected:** new sample lines appear under `flink-data/input`. Stop the producer with Ctrl-C when
enough data has been generated.

For event-time examples, start this producer in a separate terminal:

```sh
npm run produce:events -- ./flink-data/input
```

It writes one JSON event file per second with second-precision event timestamps, using a
temporary file followed by an atomic rename. Stop it with Ctrl-C after the windows have had time
to emit rows.

## 4. Create jobs from templates and inspect output

**UI**

1. Choose **Flink: New Job** and select **From template**.
2. Load each of Spec A (aggregate), Spec B (word count), Spec C (per-minute totals), Spec D
   (hop totals), Spec E (cumulate), Spec F (user sessions), Spec G (window Top-2), and Spec H
   (deduplication).
3. For each template, inspect the source/steps, choose **Validate**, then **Submit**. Confirm any
   submission modal.
4. While Spec C–G are running, keep the event producer from step 3 running. Inspect
   `docker compose logs --tail=100 taskmanager` for emitted rows.
5. Cancel a submitted job using its tree action. Repeat with a read-only profile and confirm both
   Submit and SQL `INSERT` remain blocked.

**Expected:** valid templates validate and submit. The taskmanager log shows print-sink output;
event-time results arrive after newer timestamps advance the watermark. The job tree reflects
cancellation. Read-only profiles can preview but cannot submit or run INSERT.

```sh
docker compose logs --tail=100 taskmanager
```

## 5. Preview at the source and after steps

**UI**

1. Open **Flink: New Job** and load Spec A, B, C, or F.
2. Click **Preview** on the source card and each step card. Then open the Run tab and preview the
   final result.
3. On an updating aggregate preview, verify the `kind` column and toggle **Show final state**.
4. Start another preview before the first completes and verify the previous operation is
   cancelled. Also press **Cancel** during a preview.
5. Point a filesystem source at a nonexistent container path and preview it.

**Expected:** rows or a plain-language empty/timed-out hint appear within the configured timeout.
The preview shows columns/types, row count, elapsed time, changelog kinds, and truncation where
applicable. The final-state toggle applies changelog updates locally. Preview never submits an
INSERT. A missing or empty filesystem path produces a readable hint that the folder may be empty
or the path may be wrong inside the container.

## 6. Infer a schema from a local sample

Create a small local JSON Lines sample:

```sh
cat > /tmp/flink-schema-sample.jsonl <<'EOF'
{"user":"alice","amount":12,"event_time":"2025-01-02T03:04:05","active":true}
{"user":"bob","amount":12.5,"event_time":"2025-01-02T03:04:06","active":false,"extra":{"x":1}}
EOF
```

**UI**

1. In the New Job wizard's Source tab, choose **Infer from sample file...** and select
   `/tmp/flink-schema-sample.jsonl`.
2. Review and edit the inferred fields before applying. Check the `ISO-8601` timestamp format,
   the widening note for `amount`, and the optional event-time suggestion.
3. Apply the fields, configure a filesystem path mounted inside Flink, and preview the source.

**Expected:** inference proposes `STRING`, `DOUBLE`, `TIMESTAMP(3)`, and `BOOLEAN` fields (plus
the nested value as `STRING`), with no automatic application. The local sample contents are not
sent to Flink or logged. The applied source compiles and can be previewed.

## 7. Automated checks

```sh
npm run compile
npm run lint
npm test
FLINK_GATEWAY_URL=http://localhost:8083 \
FLINK_DATA_HOST_DIR="$PWD/flink-data" \
npm run test:integration
```

**Expected:** compile, lint, and unit tests pass. Integration tests run against the local Gateway
and filesystem mount; without the two environment variables they are skipped.

## 8. Package and install the extension

```sh
npx @vscode/vsce package --out /tmp/apache-flink-manager.vsix
code --install-extension /tmp/apache-flink-manager.vsix --force
```

**Expected:** VS Code installs the generated VSIX. The packaged extension contains the JobSpec
templates under `examples/jobs/`, but excludes development tools, tests, docs, `justfile`,
`docker-compose.yml`, SQL example files, and source maps.
