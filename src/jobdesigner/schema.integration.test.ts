import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { test } from 'node:test';
import { SqlGatewayClient } from '../flink/SqlGatewayClient';
import { ClusterProfile } from '../flink/types';
import { compileSpec } from './compile';
import { resolveJobSchema } from './schema';
import { JobSpec } from './spec';

const gatewayUrl = process.env['FLINK_GATEWAY_URL'];

test('resolves schemas and EXPLAINs Specs A and B on a live SQL Gateway', { skip: !gatewayUrl }, async () => {
    const profile: ClusterProfile = {
        id: 'integration',
        name: 'integration',
        baseUrl: gatewayUrl!,
        sqlGatewayUrl: gatewayUrl!
    };
    const client = new SqlGatewayClient(profile);

    for (const spec of [specA(), specB()]) {
        const resolved = await resolveJobSchema(client, spec);
        assert.ok(resolved.source.length > 0);
        assert.equal(resolved.schemaByStep.length, spec.steps.length);
        assert.ok(resolved.finalSchema.length > 0);
        assert.deepEqual(resolved.finalSchema.map(column => column.name), spec.name === 'integration_spec_a'
            ? ['user', 'total']
            : ['word', 'cnt']);
        if (spec.name === 'integration_spec_b') {
            assert.equal(resolved.finalSchema[1].type, 'BIGINT');
        }

        const compiled = compileSpec(spec);
        const session = await client.openSession();
        try {
            for (const statement of compiled.statements.slice(0, -1)) {
                const result = await client.runStatement(session, statement, { maxRows: 10_000 });
                assert.equal(result.status, 'FINISHED', `${statement} should finish`);
            }
            const explain = await client.runStatement(session, `EXPLAIN ${compiled.statements.at(-1)}`, {
                maxRows: 10_000
            });
            assert.equal(explain.status, 'FINISHED', `EXPLAIN INSERT should finish for ${spec.name}`);
            assert.ok(explain.rows.length > 0, `EXPLAIN should return a plan for ${spec.name}`);
        } finally {
            await client.closeSession(session.sessionHandle);
        }
    }
});

test('resolves and EXPLAINs Specs C-H and compares predicted changelog modes', { skip: !gatewayUrl }, async () => {
    const profile: ClusterProfile = {
        id: 'phase3-integration',
        name: 'phase3-integration',
        baseUrl: gatewayUrl!,
        sqlGatewayUrl: gatewayUrl!
    };
    const client = new SqlGatewayClient(profile);
    const examples = [
        'spec-c-window-totals.flinkjob.json',
        'spec-d-hop-totals.flinkjob.json',
        'spec-e-cumulate-daily.flinkjob.json',
        'spec-f-user-sessions.flinkjob.json',
        'spec-g-window-top-two.flinkjob.json',
        'spec-h-deduplicate.flinkjob.json'
    ];

    for (const file of examples) {
        const spec = readJobSpec(file);
        const resolved = await resolveJobSchema(client, spec);
        assert.ok(resolved.source.length > 0, `${file} should resolve a source schema`);
        assert.equal(resolved.schemaByStep.length, spec.steps.length, `${file} should resolve every step`);
        assert.ok(resolved.finalSchema.length > 0, `${file} should resolve its final schema`);
        const compiled = compileSpec(spec, resolved);
        const session = await client.openSession();
        try {
            for (const statement of compiled.statements.slice(0, -1)) {
                const result = await client.runStatement(session, statement, { maxRows: 10_000 });
                assert.equal(result.status, 'FINISHED', `${file}: ${statement} should finish`);
            }
            const insert = compiled.statements.at(-1)!;
            const explainInsert = await client.runStatement(session, `EXPLAIN ${insert}`, { maxRows: 10_000 });
            assert.equal(explainInsert.status, 'FINISHED', `${file} EXPLAIN INSERT should finish`);
            assert.ok(explainInsert.rows.length > 0, `${file} EXPLAIN INSERT should return a plan`);

            const explainMode = await client.runStatement(
                session,
                `EXPLAIN CHANGELOG_MODE ${insert}`,
                { maxRows: 10_000 }
            );
            assert.equal(explainMode.status, 'FINISHED', `${file} EXPLAIN CHANGELOG_MODE should finish`);
            const planText = explainMode.rows.flatMap(row => row.fields ?? []).join('\n');
            assert.ok(planText, `${file} EXPLAIN CHANGELOG_MODE should return a plan`);
            assert.equal(
                modeFromExplain(planText),
                compiled.changelogMode,
                `${file} predicted ${compiled.changelogMode}, but Flink's EXPLAIN CHANGELOG_MODE differed`
            );
        } finally {
            await client.closeSession(session.sessionHandle);
        }
    }
});

test('verifies event-time survives split-to-rows on a live Gateway', { skip: !gatewayUrl }, async () => {
    const profile: ClusterProfile = {
        id: 'rowtime-unnest',
        name: 'rowtime-unnest',
        baseUrl: gatewayUrl!,
        sqlGatewayUrl: gatewayUrl!
    };
    const client = new SqlGatewayClient(profile);
    const spec: JobSpec = {
        version: 1,
        name: 'rowtime_unnest_probe',
        source: {
            type: 'filesystem',
            path: '/opt/flink/data/input',
            format: 'json',
            watch: false,
            fields: [
                { name: 'line', type: 'STRING' },
                { name: 'event_time', type: 'TIMESTAMP(3)' }
            ],
            options: { timestampFormat: 'ISO-8601' },
            timeAttribute: {
                kind: 'eventTime',
                column: 'event_time',
                maxOutOfOrderness: { value: 2, unit: 'SECOND' }
            }
        },
        steps: [{
            type: 'parse',
            kind: 'splitToRows',
            column: 'line',
            delimiter: ' ',
            as: 'word'
        }],
        sink: { type: 'print' },
        settings: {}
    };
    const resolved = await resolveJobSchema(client, spec);
    assert.equal(resolved.sourceTimeAttribute, 'event_time');
    assert.equal(resolved.timeAttributeByStep[0], 'event_time');
    assert.equal(compileSpec(spec, resolved).pipelineByStep[0].timeAttribute, 'event_time');
});

test('resolves and EXPLAINs processing-time window SQL on a live Gateway', { skip: !gatewayUrl }, async () => {
    const profile: ClusterProfile = {
        id: 'processing-time-probe',
        name: 'processing-time-probe',
        baseUrl: gatewayUrl!,
        sqlGatewayUrl: gatewayUrl!
    };
    const client = new SqlGatewayClient(profile);
    const spec: JobSpec = {
        version: 1,
        name: 'processing_time_probe',
        source: {
            type: 'datagen',
            rowsPerSecond: 1,
            fields: [{ name: 'user', type: 'STRING', kind: 'random' }],
            timeAttribute: { kind: 'processingTime' }
        },
        steps: [{
            type: 'windowAggregate',
            window: { kind: 'tumble', size: { value: 1, unit: 'MINUTE' } },
            groupBy: ['user'],
            aggregates: [{ fn: 'COUNT', as: 'n' }],
            includeWindowStart: true,
            includeWindowEnd: true
        }],
        sink: { type: 'print' },
        settings: {}
    };
    const resolved = await resolveJobSchema(client, spec);
    assert.equal(resolved.sourceTimeAttribute, 'proc_time');
    assert.deepEqual(resolved.source.find(column => column.name === 'proc_time'), {
        name: 'proc_time',
        type: 'TIMESTAMP(3)'
    });
    const compiled = compileSpec(spec, resolved);
    const session = await client.openSession();
    try {
        for (const statement of compiled.statements.slice(0, -1)) {
            const result = await client.runStatement(session, statement, { maxRows: 10_000 });
            assert.equal(result.status, 'FINISHED', `${statement} should finish`);
        }
        const explain = await client.runStatement(
            session,
            `EXPLAIN ${compiled.statements.at(-1)}`,
            { maxRows: 10_000 }
        );
        assert.equal(explain.status, 'FINISHED');
        assert.ok(explain.rows.length > 0);
    } finally {
        await client.closeSession(session.sessionHandle);
    }
});

test('verifies Flink-enforced window duration rules on a live Gateway', { skip: !gatewayUrl }, async () => {
    const profile: ClusterProfile = {
        id: 'window-duration-probe',
        name: 'window-duration-probe',
        baseUrl: gatewayUrl!,
        sqlGatewayUrl: gatewayUrl!
    };
    const client = new SqlGatewayClient(profile);
    const session = await client.openSession();
    try {
        const source = await client.runStatement(session,
            "CREATE TEMPORARY TABLE `duration_probe` (`user` STRING, `event_time` TIMESTAMP(3), WATERMARK FOR `event_time` AS `event_time` - INTERVAL '1' SECOND) WITH ('connector' = 'filesystem', 'path' = 'file:///opt/flink/data/input', 'format' = 'json', 'json.timestamp-format.standard' = 'ISO-8601')",
            { maxRows: 100 }
        );
        assert.equal(source.status, 'FINISHED');

        const cases: Array<{ label: string; expression: string; accepted: boolean }> = [
            { label: 'positive tumble', expression: "TUMBLE(TABLE `duration_probe`, DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE)", accepted: true },
            { label: 'zero tumble size', expression: "TUMBLE(TABLE `duration_probe`, DESCRIPTOR(`event_time`), INTERVAL '0' MINUTE)", accepted: false },
            { label: 'negative tumble size', expression: "TUMBLE(TABLE `duration_probe`, DESCRIPTOR(`event_time`), INTERVAL '-1' MINUTE)", accepted: false },
            { label: 'positive hop', expression: "HOP(TABLE `duration_probe`, DESCRIPTOR(`event_time`), INTERVAL '30' SECOND, INTERVAL '1' MINUTE)", accepted: true },
            { label: 'zero hop slide', expression: "HOP(TABLE `duration_probe`, DESCRIPTOR(`event_time`), INTERVAL '0' SECOND, INTERVAL '1' MINUTE)", accepted: false },
            { label: 'hop slide larger than size', expression: "HOP(TABLE `duration_probe`, DESCRIPTOR(`event_time`), INTERVAL '2' MINUTE, INTERVAL '1' MINUTE)", accepted: true },
            { label: 'valid cumulate multiple', expression: "CUMULATE(TABLE `duration_probe`, DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE, INTERVAL '2' MINUTE)", accepted: true },
            { label: 'invalid cumulate multiple', expression: "CUMULATE(TABLE `duration_probe`, DESCRIPTOR(`event_time`), INTERVAL '2' MINUTE, INTERVAL '3' MINUTE)", accepted: false },
            { label: 'zero cumulate step', expression: "CUMULATE(TABLE `duration_probe`, DESCRIPTOR(`event_time`), INTERVAL '0' MINUTE, INTERVAL '2' MINUTE)", accepted: false },
            { label: 'positive session gap', expression: "SESSION(TABLE `duration_probe` PARTITION BY `user`, DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE)", accepted: true },
            { label: 'zero session gap', expression: "SESSION(TABLE `duration_probe` PARTITION BY `user`, DESCRIPTOR(`event_time`), INTERVAL '0' MINUTE)", accepted: false }
        ];
        for (const { label, expression, accepted } of cases) {
            const result = await client.runStatement(
                session,
                `EXPLAIN SELECT window_start FROM TABLE(${expression})`,
                { maxRows: 10_000 }
            );
            assert.equal(result.status === 'FINISHED', accepted, `${label} acceptance should match Flink`);
        }
    } finally {
        await client.closeSession(session.sessionHandle);
    }
});

function readJobSpec(file: string): JobSpec {
    return JSON.parse(readFileSync(join(process.cwd(), 'examples', 'jobs', file), 'utf8')) as JobSpec;
}

function modeFromExplain(plan: string): 'APPEND-ONLY' | 'UPDATING' {
    if (!/changelogMode\s*=\s*\[[^\]]+\]/i.test(plan)) {
        throw new Error('Flink EXPLAIN CHANGELOG_MODE did not include changelogMode details.');
    }
    return /\b(?:UA|UB|UPDATE_BEFORE|UPDATE_AFTER|DELETE)\b/i.test(plan)
        ? 'UPDATING'
        : 'APPEND-ONLY';
}

function specA(): JobSpec {
    return {
        version: 1,
        name: 'integration_spec_a',
        source: {
            type: 'filesystem',
            path: '/opt/flink/data/input',
            format: 'json',
            watch: false,
            fields: [
                { name: 'user', type: 'STRING' },
                { name: 'amount', type: 'DOUBLE' }
            ]
        },
        steps: [
            {
                type: 'filter',
                mode: 'all',
                conditions: [{ column: 'amount', op: '>=', value: 10 }]
            },
            {
                type: 'aggregate',
                groupBy: ['user'],
                aggregates: [{ fn: 'SUM', column: 'amount', as: 'total' }]
            }
        ],
        sink: { type: 'print' },
        settings: {}
    };
}

function specB(): JobSpec {
    return {
        version: 1,
        name: 'integration_spec_b',
        source: {
            type: 'filesystem',
            path: '/opt/flink/data/input',
            format: 'raw',
            watch: true
        },
        steps: [
            {
                type: 'parse',
                kind: 'splitToRows',
                column: 'line',
                delimiter: ' ',
                as: 'word'
            },
            {
                type: 'aggregate',
                groupBy: ['word'],
                aggregates: [{ fn: 'COUNT', as: 'cnt' }]
            }
        ],
        sink: { type: 'print' },
        settings: {}
    };
}
