import assert from 'node:assert/strict';
import { test } from 'node:test';
import { compileSpec } from './compile';
import { JobSpec } from './spec';

function specA(sink: JobSpec['sink'] = { type: 'print' }): JobSpec {
    return {
        version: 1,
        name: 'spec_a',
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
        sink,
        settings: {}
    };
}

function specB(): JobSpec {
    return {
        version: 1,
        name: 'word_count',
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

test('compiles Spec A to the exact expected filesystem/filter/aggregate SQL', () => {
    const compiled = compileSpec(specA());
    assert.deepEqual(compiled.statements.slice(2), [
        "CREATE TEMPORARY TABLE `job_source` (`user` STRING, `amount` DOUBLE) WITH ('connector' = 'filesystem', 'path' = 'file:///opt/flink/data/input', 'format' = 'json')",
        'CREATE TEMPORARY VIEW `job_step_1` AS SELECT `user`, `amount` FROM `job_source` WHERE `amount` >= 10',
        'CREATE TEMPORARY VIEW `job_step_2` AS SELECT `user`, SUM(`amount`) AS `total` FROM `job_step_1` GROUP BY `user`',
        "CREATE TEMPORARY TABLE `job_sink` (`user` STRING, `total` DOUBLE) WITH ('connector' = 'print')",
        'INSERT INTO `job_sink` SELECT `user`, `total` FROM `job_step_2`'
    ]);
    assert.deepEqual(compiled.schemaByStep, [
        [{ name: 'user', type: 'STRING' }, { name: 'amount', type: 'DOUBLE' }],
        [{ name: 'user', type: 'STRING' }, { name: 'total', type: 'DOUBLE' }]
    ]);
});

test('compiles Spec B with raw watch, split-to-rows, and BIGINT COUNT output', () => {
    const compiled = compileSpec(specB());
    assert.deepEqual(compiled.statements.slice(2), [
        "CREATE TEMPORARY TABLE `job_source` (`line` STRING) WITH ('connector' = 'filesystem', 'path' = 'file:///opt/flink/data/input', 'source.monitor-interval' = '1 s', 'format' = 'raw')",
        "CREATE TEMPORARY VIEW `job_step_1` AS SELECT `line`, `word` FROM `job_source` CROSS JOIN UNNEST(SPLIT(`line`, ' ')) AS t(`word`)",
        'CREATE TEMPORARY VIEW `job_step_2` AS SELECT `word`, COUNT(*) AS `cnt` FROM `job_step_1` GROUP BY `word`',
        "CREATE TEMPORARY TABLE `job_sink` (`word` STRING, `cnt` BIGINT) WITH ('connector' = 'print')",
        'INSERT INTO `job_sink` SELECT `word`, `cnt` FROM `job_step_2`'
    ]);
});

test('escapes backticks in identifiers and quotes in string literals', () => {
    const spec: JobSpec = {
        version: 1,
        name: 'escaped',
        source: {
            type: 'filesystem',
            path: '/tmp/input',
            format: 'json',
            watch: false,
            fields: [{ name: 'a`b', type: 'STRING' }]
        },
        steps: [{
            type: 'filter',
            mode: 'all',
            conditions: [{ column: 'a`b', op: '=', value: "O'Reilly" }]
        }],
        sink: { type: 'print' },
        settings: {}
    };
    const sql = compileSpec(spec).statements;
    assert.ok(sql.some(statement => statement.includes('CREATE TEMPORARY TABLE `job_source` (`a``b` STRING)')));
    assert.ok(sql.some(statement => statement.includes("WHERE `a``b` = 'O''Reilly'")));
});

test('emits settings before DDL and defaults checkpoint interval to ten seconds', () => {
    const spec = specA();
    spec.settings = { parallelism: 2, checkpointIntervalSeconds: 30, stateTtlSeconds: 600 };
    const sql = compileSpec(spec).statements;
    assert.deepEqual(sql.slice(0, 4), [
        "SET 'pipeline.name' = 'spec_a'",
        "SET 'parallelism.default' = '2'",
        "SET 'execution.checkpointing.interval' = '30 s'",
        "SET 'table.exec.state.ttl' = '600 s'"
    ]);
});

test('uses gateway-resolved types for the sink table schema', () => {
    const spec = specA();
    const compiled = compileSpec(spec, {
        source: [
            { name: 'user', type: 'STRING' },
            { name: 'amount', type: 'DOUBLE' }
        ],
        schemaByStep: [
            [
                { name: 'user', type: 'STRING' },
                { name: 'amount', type: 'DOUBLE' }
            ],
            [
                { name: 'user', type: 'STRING' },
                { name: 'total', type: 'BIGINT' }
            ]
        ]
    });
    assert.ok(compiled.statements.includes(
        "CREATE TEMPORARY TABLE `job_sink` (`user` STRING, `total` BIGINT) WITH ('connector' = 'print')"
    ));
});

test('rejects filesystem sink for aggregate changelog output before compilation', () => {
    assert.throws(
        () => compileSpec(specA({ type: 'filesystem', path: '/tmp/output', format: 'json' })),
        /filesystem sink requires append-only rows/
    );
});

test('rejects control characters, unknown formats, and unsafe raw SQL tokens', () => {
    const withControl = specA();
    withControl.source = {
        type: 'filesystem',
        path: '/tmp/input\nbad',
        format: 'json',
        watch: false,
        fields: [{ name: 'user', type: 'STRING' }]
    };
    assert.throws(() => compileSpec(withControl), /source\.path/);

    const withUnknownFormat = specA();
    withUnknownFormat.source = {
        ...withUnknownFormat.source,
        format: 'avro'
    } as never;
    assert.throws(() => compileSpec(withUnknownFormat), /source\.format/);

    const withRawInjection = specA();
    withRawInjection.steps = [{
        type: 'select',
        columns: [],
        computed: [{
            as: 'value',
            expr: { type: 'raw', text: 'amount); DROP TABLE job_source' }
        }]
    }];
    assert.throws(() => compileSpec(withRawInjection), /at least one output column|Unsupported token/);
});

test('raw expressions quote identifiers and reject control characters', () => {
    const spec = specA();
    spec.steps = [{
        type: 'select',
        columns: [],
        computed: [{ as: 'upper_user', expr: { type: 'raw', text: 'UPPER(user)' } }]
    }];
    assert.ok(compileSpec(spec).statements.includes(
        'CREATE TEMPORARY VIEW `job_step_1` AS SELECT UPPER ( `user` ) AS `upper_user` FROM `job_source`'
    ));

    spec.steps = [{
        type: 'select',
        columns: [],
        computed: [{ as: 'bad', expr: { type: 'raw', text: 'user\nFROM job_source' } }]
    }];
    assert.throws(() => compileSpec(spec), /control characters/);
});

function eventTimeSource(): JobSpec['source'] {
    return {
        type: 'filesystem',
        path: '/opt/flink/data/input',
        format: 'json',
        watch: false,
        fields: [
            { name: 'user', type: 'STRING' },
            { name: 'amount', type: 'DOUBLE' },
            { name: 'event_time', type: 'TIMESTAMP(3)' }
        ],
        options: { timestampFormat: 'ISO-8601' },
        timeAttribute: {
            kind: 'eventTime',
            column: 'event_time',
            maxOutOfOrderness: { value: 2, unit: 'SECOND' }
        }
    };
}

function windowedSpec(name: string, steps: JobSpec['steps']): JobSpec {
    return {
        version: 1,
        name,
        source: eventTimeSource(),
        steps,
        sink: { type: 'print' },
        settings: {}
    };
}

test('compiles Spec C with event-time watermark and tumble aggregation', () => {
    const spec = windowedSpec('spec_c', [
        {
            type: 'filter',
            mode: 'all',
            conditions: [{ column: 'amount', op: '>=', value: 10 }]
        },
        {
            type: 'windowAggregate',
            window: { kind: 'tumble', size: { value: 1, unit: 'MINUTE' } },
            groupBy: ['user'],
            aggregates: [
                { fn: 'SUM', column: 'amount', as: 'total' },
                { fn: 'COUNT', as: 'n' }
            ],
            includeWindowStart: true,
            includeWindowEnd: false
        }
    ]);
    assert.deepEqual(compileSpec(spec).statements.slice(2), [
        "CREATE TEMPORARY TABLE `job_source` (`user` STRING, `amount` DOUBLE, `event_time` TIMESTAMP(3), WATERMARK FOR `event_time` AS `event_time` - INTERVAL '2' SECOND) WITH ('connector' = 'filesystem', 'path' = 'file:///opt/flink/data/input', 'format' = 'json', 'json.timestamp-format.standard' = 'ISO-8601')",
        'CREATE TEMPORARY VIEW `job_step_1` AS SELECT `user`, `amount`, `event_time` FROM `job_source` WHERE `amount` >= 10',
        "CREATE TEMPORARY VIEW `job_step_2` AS SELECT `window_start`, `user`, SUM(`amount`) AS `total`, COUNT(*) AS `n` FROM TABLE(TUMBLE(TABLE `job_step_1`, DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE)) GROUP BY `window_start`, `window_end`, `user`",
        "CREATE TEMPORARY TABLE `job_sink` (`window_start` TIMESTAMP(3), `user` STRING, `total` DOUBLE, `n` BIGINT) WITH ('connector' = 'print')",
        'INSERT INTO `job_sink` SELECT `window_start`, `user`, `total`, `n` FROM `job_step_2`'
    ]);
});

test('compiles Spec D hop with slide before size', () => {
    const spec = windowedSpec('spec_d', [{
        type: 'windowAggregate',
        window: {
            kind: 'hop',
            slide: { value: 30, unit: 'SECOND' },
            size: { value: 1, unit: 'MINUTE' }
        },
        groupBy: ['user'],
        aggregates: [{ fn: 'SUM', column: 'amount', as: 'total' }],
        includeWindowStart: true,
        includeWindowEnd: true
    }]);
    assert.ok(compileSpec(spec).statements.includes(
        'CREATE TEMPORARY VIEW `job_step_1` AS SELECT `window_start`, `window_end`, `user`, SUM(`amount`) AS `total` FROM TABLE(HOP(TABLE `job_source`, DESCRIPTOR(`event_time`), INTERVAL \'30\' SECOND, INTERVAL \'1\' MINUTE)) GROUP BY `window_start`, `window_end`, `user`'
    ));
});

test('compiles Spec E cumulate with step before maximum size', () => {
    const spec = windowedSpec('spec_e', [{
        type: 'windowAggregate',
        window: {
            kind: 'cumulate',
            step: { value: 1, unit: 'MINUTE' },
            maxSize: { value: 2, unit: 'MINUTE' }
        },
        groupBy: ['user'],
        aggregates: [{ fn: 'SUM', column: 'amount', as: 'total' }],
        includeWindowStart: true,
        includeWindowEnd: true
    }]);
    assert.ok(compileSpec(spec).statements.includes(
        'CREATE TEMPORARY VIEW `job_step_1` AS SELECT `window_start`, `window_end`, `user`, SUM(`amount`) AS `total` FROM TABLE(CUMULATE(TABLE `job_source`, DESCRIPTOR(`event_time`), INTERVAL \'1\' MINUTE, INTERVAL \'2\' MINUTE)) GROUP BY `window_start`, `window_end`, `user`'
    ));
});

test('compiles Spec F session with group keys in PARTITION BY', () => {
    const spec = windowedSpec('spec_f', [{
        type: 'windowAggregate',
        window: { kind: 'session', gap: { value: 1, unit: 'MINUTE' } },
        groupBy: ['user'],
        aggregates: [{ fn: 'COUNT', as: 'n' }],
        includeWindowStart: true,
        includeWindowEnd: true
    }]);
    assert.ok(compileSpec(spec).statements.includes(
        'CREATE TEMPORARY VIEW `job_step_1` AS SELECT `window_start`, `window_end`, `user`, COUNT(*) AS `n` FROM TABLE(SESSION(TABLE `job_source` PARTITION BY `user`, DESCRIPTOR(`event_time`), INTERVAL \'1\' MINUTE)) GROUP BY `window_start`, `window_end`, `user`'
    ));
});

test('compiles Spec G window Top-2 partitioned by both window boundaries', () => {
    const spec = windowedSpec('spec_g', [
        {
            type: 'windowAggregate',
            window: { kind: 'tumble', size: { value: 2, unit: 'MINUTE' } },
            groupBy: ['user'],
            aggregates: [{ fn: 'SUM', column: 'amount', as: 'total' }],
            includeWindowStart: true,
            includeWindowEnd: true
        },
        {
            type: 'topN',
            partitionBy: ['window_start', 'window_end'],
            orderBy: { column: 'total', direction: 'DESC' },
            n: 2
        }
    ]);
    assert.deepEqual(compileSpec(spec).statements.slice(4, 6), [
        "CREATE TEMPORARY VIEW `job_step_2` AS SELECT `window_start`, `window_end`, `user`, `total` FROM (SELECT `window_start`, `window_end`, `user`, `total`, ROW_NUMBER() OVER (PARTITION BY `window_start`, `window_end` ORDER BY `total` DESC) AS `__flink_row_number` FROM `job_step_1`) WHERE `__flink_row_number` <= 2",
        "CREATE TEMPORARY TABLE `job_sink` (`window_start` TIMESTAMP(3), `window_end` TIMESTAMP(3), `user` STRING, `total` DOUBLE) WITH ('connector' = 'print')"
    ]);
});

test('compiles Spec H keep-first deduplication by the event-time column', () => {
    const spec = windowedSpec('spec_h', [{
        type: 'deduplicate',
        keyColumns: ['user'],
        keep: 'first',
        orderColumn: 'event_time'
    }]);
    assert.ok(compileSpec(spec).statements.includes(
        'CREATE TEMPORARY VIEW `job_step_1` AS SELECT `user`, `amount`, `event_time` FROM (SELECT `user`, `amount`, `event_time`, ROW_NUMBER() OVER (PARTITION BY `user` ORDER BY `event_time` ASC) AS `__flink_row_number` FROM `job_source`) WHERE `__flink_row_number` = 1'
    ));
});

test('tracks time-attribute loss and predicted changelog mode through each step', () => {
    const windowed = windowedSpec('metadata', [
        {
            type: 'windowAggregate',
            window: { kind: 'tumble', size: { value: 1, unit: 'MINUTE' } },
            groupBy: ['user'],
            aggregates: [{ fn: 'SUM', column: 'amount', as: 'total' }],
            includeWindowStart: true,
            includeWindowEnd: true
        },
        {
            type: 'topN',
            partitionBy: ['window_start', 'window_end'],
            orderBy: { column: 'total', direction: 'DESC' },
            n: 2
        }
    ]);
    const compiled = compileSpec(windowed);
    assert.equal(compiled.sourceMetadata.timeAttribute, 'event_time');
    assert.equal(compiled.pipelineByStep[0].timeAttribute, undefined);
    assert.equal(compiled.pipelineByStep[0].lostTimeAttributeAt, 'steps[0] (windowAggregate)');
    assert.equal(compiled.pipelineByStep[0].changelogMode, 'APPEND-ONLY');
    assert.equal(compiled.pipelineByStep[1].changelogMode, 'APPEND-ONLY');
    assert.equal(compiled.changelogMode, 'APPEND-ONLY');

    const deduplicate = windowedSpec('dedup_metadata', [{
        type: 'deduplicate',
        keyColumns: ['user'],
        keep: 'first',
        orderColumn: 'event_time'
    }]);
    assert.equal(compileSpec(deduplicate).changelogMode, 'UPDATING');
});
