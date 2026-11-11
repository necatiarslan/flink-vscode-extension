import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { basename, join } from 'node:path';
import { test } from 'node:test';
import { FlinkClient } from '../flink/FlinkClient';
import { SqlGatewayClient } from '../flink/SqlGatewayClient';
import { ClusterProfile } from '../flink/types';
import { compilePreview } from './compile';
import { JobSpec } from './spec';

const gatewayUrl = process.env['FLINK_GATEWAY_URL'];
const hostDataDirectory = process.env['FLINK_DATA_HOST_DIR'];
const enabled = Boolean(gatewayUrl && hostDataDirectory);

function integrationProfile(id: string): ClusterProfile {
    const restUrl = new URL(process.env['FLINK_REST_URL'] ?? gatewayUrl!);
    if (!process.env['FLINK_REST_URL'] && restUrl.port === '8083') {
        restUrl.port = '8081';
    }
    return {
        id,
        name: id,
        baseUrl: restUrl.toString(),
        sqlGatewayUrl: gatewayUrl!
    };
}

test('previews Specs A, C, and F, nested JSON strings, and a missing source path', { skip: !enabled }, async () => {
    const hostDirectory = hostDataDirectory!;
    const temporaryDirectory = await mkdtemp(join(hostDirectory, 'phase4-preview-'));
    const containerPath = `/opt/flink/data/${basename(temporaryDirectory)}`;
    const profile = integrationProfile('phase4-preview-integration');
    const client = new SqlGatewayClient(profile);
    const timestamps = [
        new Date(Date.now() - 3_000).toISOString().slice(0, 19),
        new Date(Date.now() - 2_000).toISOString().slice(0, 19),
        new Date(Date.now() - 1_000).toISOString().slice(0, 19)
    ];
    const rows = [
        { user: 'alice', amount: 21, event_time: timestamps[0] },
        { user: 'bob', amount: 13, event_time: timestamps[1] },
        { user: 'alice', amount: 17, event_time: timestamps[2] }
    ];
    await writeFile(join(temporaryDirectory, 'events.json'), `${rows.map(row => JSON.stringify(row)).join('\n')}\n`);

    try {
        for (const [file, targets] of [
            ['spec-a-aggregate.flinkjob.json', ['source', 0, 1] as const],
            ['spec-c-window-totals.flinkjob.json', ['source', 0, 1] as const],
            ['spec-f-user-sessions.flinkjob.json', ['source', 0] as const]
        ] as const) {
            const spec = await readSpec(file);
            if (spec.source.type !== 'filesystem') {
                throw new Error(`${file} must use a filesystem source for this integration test.`);
            }
            spec.source.path = containerPath;
            spec.source.watch = true;
            for (const target of targets) {
                const result = await runPreview(client, spec, target);
                assert.ok(result.rows.length > 0, `${file} preview at ${String(target)} should return rows`);
            }
        }

        const nestedSpec: JobSpec = {
            version: 1,
            name: 'nested_json_string_preview',
            source: {
                type: 'filesystem',
                path: containerPath,
                format: 'json',
                watch: false,
                fields: [
                    { name: 'user', type: 'STRING' },
                    { name: 'payload', type: 'STRING' }
                ]
            },
            steps: [],
            sink: { type: 'print' },
            settings: {}
        };
        await writeFile(
            join(temporaryDirectory, 'nested.json'),
            `${JSON.stringify({ user: 'nested', payload: { count: 3 } })}\n`
        );
        const nested = await runPreview(client, nestedSpec, 'source');
        assert.ok(nested.rows.length > 0, 'nested JSON object should be readable in a STRING field');
        const nestedRecord = nested.rows.find(row => row.fields?.[0] === 'nested');
        assert.ok(nestedRecord, 'nested-object sample record should be returned');
        assert.match(String(nestedRecord.fields?.[1]), /count/);

        const missing = { ...nestedSpec, name: 'missing_source_preview' };
        if (missing.source.type !== 'filesystem') {
            throw new Error('Expected filesystem source.');
        }
        missing.source.path = `${containerPath}/missing-directory`;
        await assert.rejects(
            runPreview(client, missing, 'source'),
            error => {
                assert.match(String(error), /folder may be empty|source path may be wrong/i);
                return true;
            }
        );
    } finally {
        await rm(temporaryDirectory, { recursive: true, force: true });
    }
});

test('cancels a live bounded datagen preview without leaving a running Flink job', { skip: !enabled }, async () => {
    const profile = integrationProfile('phase4-preview-cancel');
    const client = new SqlGatewayClient(profile);
    const cluster = new FlinkClient(profile);
    const spec: JobSpec = {
        version: 1,
        name: 'phase4_preview_cancel',
        source: {
            type: 'datagen',
            rowsPerSecond: 1,
            fields: [{ name: 'value', type: 'BIGINT', kind: 'random', min: 0, max: 100 }]
        },
        steps: [],
        sink: { type: 'print' },
        settings: {}
    };
    const compiled = compilePreview(spec, 'source', { maxRows: 100_000 });
    const session = await client.openSession();
    let jobID: string | undefined;
    try {
        for (const statement of compiled.statements.slice(0, -1)) {
            const result = await client.runStatement(session, statement, { maxRows: 1_000 });
            assert.equal(result.status, 'FINISHED');
        }
        const preview = await client.runStatement(session, compiled.statements.at(-1)!, {
            maxRows: 1,
            pollingIntervalMs: 20
        });
        jobID = preview.jobID;
        assert.ok(jobID, 'row-limited preview should expose its Flink job ID');
        assert.ok(preview.truncated, 'row limit should cancel the streaming preview query');
        const deadline = Date.now() + 10_000;
        let state = '';
        while (Date.now() < deadline) {
            try {
                const job = await cluster.get<{ state?: string }>(`/jobs/${encodeURIComponent(jobID)}`);
                state = job.state?.toUpperCase() ?? '';
            } catch (error) {
                if (!(error instanceof Error) || !/HTTP 404/.test(error.message)) {
                    throw error;
                }
                state = 'MISSING';
                break;
            }
            if (['CANCELED', 'CANCELLED', 'FAILED', 'FINISHED'].includes(state)) {
                break;
            }
            await new Promise(resolve => setTimeout(resolve, 100));
        }
        if (state !== 'MISSING') {
            assert.ok(
                ['CANCELED', 'CANCELLED', 'FAILED', 'FINISHED'].includes(state),
                `preview job should be terminal, not ${state || 'unknown'}`
            );
        } else {
            const overview = await cluster.get<{ jobs?: Array<{ jid: string; state: string }> }>('/jobs/overview');
            assert.equal(
                overview.jobs?.some(job => job.jid === jobID && job.state.toUpperCase() === 'RUNNING'),
                false,
                'a preview job missing from job details must not remain RUNNING in the overview'
            );
        }
    } finally {
        if (jobID) {
            try {
                const job = await cluster.get<{ state?: string }>(`/jobs/${encodeURIComponent(jobID)}`);
                if (job.state && !['CANCELED', 'CANCELLED', 'FAILED', 'FINISHED'].includes(job.state.toUpperCase())) {
                    await cluster.patch(`/jobs/${encodeURIComponent(jobID)}?mode=cancel`);
                }
            } catch (error) {
                if (!(error instanceof Error) || !/HTTP 404/.test(error.message)) {
                    throw error;
                }
            }
        }
        await client.closeSession(session.sessionHandle);
    }
});

async function runPreview(
    client: SqlGatewayClient,
    spec: JobSpec,
    target: 'source' | number
): Promise<Awaited<ReturnType<SqlGatewayClient['runStatement']>>> {
    const compiled = compilePreview(spec, target, { maxRows: 50 });
    const session = await client.openSession();
    try {
        let result: Awaited<ReturnType<SqlGatewayClient['runStatement']>> | undefined;
        for (const [index, statement] of compiled.statements.entries()) {
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), 20_000);
            try {
                result = await client.runStatement(session, statement, {
                    maxRows: index === compiled.statements.length - 1 ? 50 : 1_000,
                    signal: controller.signal
                });
            } finally {
                clearTimeout(timeout);
            }
            if (result.status !== 'FINISHED') {
                throw new Error(
                    result.error
                    ?? (result.status === 'CANCELED'
                        ? 'Preview timed out. The folder may be empty, or the source path may be wrong inside the Flink container.'
                        : `Preview statement ended with status ${result.status}.`)
                );
            }
        }
        if (!result) {
            throw new Error('Preview did not execute a SELECT statement.');
        }
        return result;
    } finally {
        await client.closeSession(session.sessionHandle);
    }
}

async function readSpec(file: string): Promise<JobSpec> {
    return JSON.parse(
        await readFile(join(process.cwd(), 'examples', 'jobs', file), 'utf8')
    ) as JobSpec;
}
