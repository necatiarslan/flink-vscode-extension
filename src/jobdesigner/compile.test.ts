import assert from 'node:assert/strict';
import { test } from 'node:test';
import { validateSpec } from './compile';
import { JobSpec } from './spec';

function baseSpec(): JobSpec {
    return {
        version: 1,
        name: 'word_count',
        source: {
            type: 'filesystem',
            path: '/opt/flink/data/input/',
            format: 'raw',
            watch: true
        },
        steps: [],
        sink: { type: 'print' },
        settings: {}
    };
}

function issuesFor(spec: unknown): string[] {
    return validateSpec(spec).map(issue => `${issue.path}: ${issue.message}`);
}

test('accepts raw source defaults and requires exactly one STRING column', () => {
    const spec = baseSpec();
    assert.deepEqual(validateSpec(spec), []);

    const invalid = {
        ...spec,
        source: {
            ...spec.source,
            fields: [{ name: 'other', type: 'INT' }]
        }
    };
    assert.ok(issuesFor(invalid).some(message => message.startsWith('source.fields:')));
});

test('validates names, allowed types, numeric bounds, and duplicate source columns', () => {
    const spec = baseSpec();
    spec.name = 'bad name';
    spec.source = {
        type: 'datagen',
        rowsPerSecond: 0,
        fields: [
            { name: 'duplicate', type: 'INT', kind: 'sequence', min: 5, max: 1 },
            { name: 'duplicate', type: 'STRING', kind: 'random', length: 0 },
            { name: 'unsupported', type: 'FLOAT' as never, kind: 'random' }
        ]
    };
    const messages = issuesFor(spec);
    assert.ok(messages.some(message => message.startsWith('name:')));
    assert.ok(messages.some(message => message.startsWith('source.rowsPerSecond:')));
    assert.ok(messages.some(message => message.startsWith('source.fields[2].type:')));
    assert.ok(messages.some(message => message.startsWith('source.fields[0]:')));
    assert.ok(messages.some(message => message.startsWith('source.fields[1].length:')));
    assert.ok(messages.some(message => message.includes('Duplicate output column "duplicate"')));
});

test('watch mode requires a directory path and known filesystem formats', () => {
    const spec = baseSpec();
    spec.source = {
        type: 'filesystem',
        path: '/tmp/input.json',
        format: 'json',
        watch: true,
        fields: [{ name: 'payload', type: 'STRING' }]
    };
    assert.ok(issuesFor(spec).some(message => message.startsWith('source.path:')));

    const unknownFormat = {
        ...spec,
        source: { ...spec.source, format: 'avro' }
    };
    assert.ok(issuesFor(unknownFormat).some(message => message.startsWith('source.format:')));

    const badOptions = {
        ...spec,
        source: {
            ...spec.source,
            options: { timestampFormat: 'unix', delimiter: 'xx' }
        }
    };
    assert.ok(issuesFor(badOptions).some(message => message.startsWith('source.options.timestampFormat:')));

    const badDelimiter = {
        ...spec,
        source: { ...spec.source, format: 'csv', options: { delimiter: 'xx' } }
    };
    assert.ok(issuesFor(badDelimiter).some(message => message.startsWith('source.options.delimiter:')));
});

test('validates references at each pipeline stage and output duplicates', () => {
    const spec = baseSpec();
    spec.source = {
        type: 'filesystem',
        path: '/tmp/input',
        format: 'json',
        watch: false,
        fields: [{ name: 'amount', type: 'DOUBLE' }, { name: 'user', type: 'STRING' }]
    };
    spec.steps = [
        {
            type: 'filter',
            mode: 'all',
            conditions: [{ column: 'missing', op: '>=', value: 10 }]
        },
        {
            type: 'select',
            columns: [{ name: 'amount', as: 'same' }],
            computed: [{ as: 'same', expr: { type: 'column', name: 'user' } }]
        },
        {
            type: 'aggregate',
            groupBy: ['user'],
            aggregates: [            { fn: 'SUM', column: 'missing', as: 'total' }]
        }
    ];
    const messages = issuesFor(spec);
    assert.ok(messages.some(message => message.startsWith('steps[0].conditions[0].column:')));
    assert.ok(messages.some(message => message.includes('Duplicate output column "same"')));
    assert.ok(messages.some(message => message.startsWith('steps[2].groupBy[0]:')));
    assert.ok(messages.some(message => message.startsWith('steps[2].aggregates[0].column:')));
});

test('checks filter values against the referenced column type and operator rules', () => {
    const spec = baseSpec();
    spec.source = {
        type: 'datagen',
        rowsPerSecond: 1,
        fields: [{ name: 'active', type: 'BOOLEAN', kind: 'random' }]
    };
    spec.steps = [{
        type: 'filter',
        mode: 'all',
        conditions: [
            { column: 'active', op: '=', value: 'yes' },
            { column: 'active', op: 'IN', value: [] },
            { column: 'active', op: 'IS NULL', value: true }
        ]
    }];
    const messages = issuesFor(spec);
    assert.ok(messages.some(message => message.startsWith('steps[0].conditions[0].value:')));
    assert.ok(messages.some(message => message.startsWith('steps[0].conditions[1].value:')));
    assert.ok(messages.some(message => message.startsWith('steps[0].conditions[2].value:')));
});

test('validates parse fields, aggregate columns/functions, and settings', () => {
    const spec = baseSpec();
    spec.steps = [
        {
            type: 'parse',
            kind: 'jsonValue',
            column: 'line',
            fields: [{ path: '$.n', as: 'parsed', type: 'FLOAT' as never }]
        },
        {
            type: 'aggregate',
            groupBy: ['missing'],
            aggregates: [
                { fn: 'SUM', column: 'line', as: 'total' },
                { fn: 'COUNT', column: 'missing', as: 'count' },
                { fn: 'SUM', column: 'line', as: 'total' }
            ]
        }
    ];
    spec.settings = { parallelism: 0 };
    const messages = issuesFor(spec);
    assert.ok(messages.some(message => message.startsWith('steps[0].fields[0].type:')));
    assert.ok(messages.some(message => message.startsWith('steps[1].groupBy[0]:')));
    assert.ok(messages.some(message => message.startsWith('steps[1].aggregates[0].column:')));
    assert.ok(messages.some(message => message.startsWith('steps[1].aggregates[1].column:')));
    assert.ok(messages.some(message => message.includes('Duplicate output column "total"')));
    assert.ok(messages.some(message => message.startsWith('settings.parallelism:')));
});

test('warns on raw expressions and rejects control characters', () => {
    const spec = baseSpec();
    spec.source = {
        type: 'datagen',
        rowsPerSecond: 1,
        fields: [{ name: 'line', type: 'STRING', kind: 'random' }]
    };
    spec.steps = [{
        type: 'select',
        columns: [],
        computed: [{
            as: 'unsafe',
            expr: { type: 'raw', text: 'CURRENT_TIMESTAMP' }
        }]
    }];
    const warnings = validateSpec(spec).filter(issue => issue.severity === 'warning');
    assert.equal(warnings.length, 1);
    assert.equal(warnings[0].path, 'steps[0].computed[0].expr');

    const control = {
        ...spec,
        source: { type: 'filesystem', path: '/tmp/bad\npath', format: 'raw', watch: false }
    };
    assert.ok(issuesFor(control).some(message => message.startsWith('source.path:')));
});

test('rejects aggregate output sent to an append-only filesystem sink', () => {
    const spec = baseSpec();
    spec.steps = [{
        type: 'aggregate',
        groupBy: ['line'],
        aggregates: [{ fn: 'COUNT', as: 'cnt' }]
    }];
    spec.sink = { type: 'filesystem', path: '/tmp/out', format: 'json' };
    assert.ok(issuesFor(spec).some(message => message.includes('updates grouped results over time')));
});

function eventTimeSpec(): JobSpec {
    return {
        ...baseSpec(),
        source: {
            type: 'filesystem',
            path: '/tmp/events',
            format: 'json',
            watch: true,
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
        }
    };
}

function oneMinuteWindow(): JobSpec['steps'][number] {
    return {
        type: 'windowAggregate',
        window: { kind: 'tumble', size: { value: 1, unit: 'MINUTE' } },
        groupBy: ['user'],
        aggregates: [{ fn: 'SUM', column: 'amount', as: 'total' }],
        includeWindowStart: true,
        includeWindowEnd: true
    };
}

test('requires a timestamp source column and validates event-time durations', () => {
    const wrongType = eventTimeSpec();
    wrongType.source = {
        type: 'filesystem',
        path: '/tmp/events',
        format: 'json',
        watch: false,
        fields: [{ name: 'event_time', type: 'STRING' }],
        timeAttribute: {
            kind: 'eventTime',
            column: 'event_time',
            maxOutOfOrderness: { value: 0, unit: 'SECOND' }
        }
    };
    const messages = issuesFor(wrongType);
    assert.ok(messages.some(message => message.startsWith('source.timeAttribute.column:')));
    assert.ok(messages.some(message => message.startsWith('source.timeAttribute.maxOutOfOrderness:')));
});

test('requires time attributes and names the step that lost one', () => {
    const noTime = baseSpec();
    noTime.steps = [oneMinuteWindow()];
    assert.ok(issuesFor(noTime).some(message =>
        message.includes('Window aggregation requires an input time attribute')
    ));

    const lostTime = eventTimeSpec();
    lostTime.steps = [
        {
            type: 'select',
            columns: [{ name: 'user' }],
            computed: [{
                as: 'event_time',
                expr: {
                    type: 'call',
                    fn: 'CAST',
                    args: [{ type: 'column', name: 'event_time' }],
                    to: 'TIMESTAMP(3)'
                }
            }]
        },
        oneMinuteWindow()
    ];
    assert.ok(issuesFor(lostTime).some(message =>
        message.includes('lost at steps[0] (select)')
    ));
});

test('validates positive window durations, cumulative multiples, and duplicate output names', () => {
    const spec = eventTimeSpec();
    spec.steps = [{
        type: 'windowAggregate',
        window: { kind: 'hop', slide: { value: 2, unit: 'MINUTE' }, size: { value: 1, unit: 'MINUTE' } },
        groupBy: [],
        aggregates: [{ fn: 'COUNT', as: 'window_start' }],
        includeWindowStart: true,
        includeWindowEnd: false
    }];
    let messages = issuesFor(spec);
    assert.ok(!messages.some(message => message.includes('Hop slide must not exceed the window size')));
    assert.ok(messages.some(message => message.includes('Duplicate output column "window_start"')));

    spec.steps = [{
        type: 'windowAggregate',
        window: { kind: 'cumulate', step: { value: 2, unit: 'MINUTE' }, maxSize: { value: 3, unit: 'MINUTE' } },
        groupBy: [],
        aggregates: [{ fn: 'COUNT', as: 'cnt' }],
        includeWindowStart: false,
        includeWindowEnd: false
    }];
    messages = issuesFor(spec);
    assert.ok(messages.some(message => message.includes('max size must be a multiple of the step')));

    spec.steps = [{
        type: 'windowAggregate',
        window: { kind: 'tumble', size: { value: -1, unit: 'DAY' } },
        groupBy: [],
        aggregates: [{ fn: 'COUNT', as: 'cnt' }],
        includeWindowStart: false,
        includeWindowEnd: false
    }];
    assert.ok(issuesFor(spec).some(message => message.startsWith('steps[0].window.size:')));

    spec.steps = [{
        type: 'windowAggregate',
        window: { kind: 'hop', slide: { value: 0, unit: 'SECOND' }, size: { value: 1, unit: 'MINUTE' } },
        groupBy: [],
        aggregates: [{ fn: 'COUNT', as: 'cnt' }],
        includeWindowStart: false,
        includeWindowEnd: false
    }];
    assert.ok(issuesFor(spec).some(message => message.startsWith('steps[0].window.slide:')));
});

test('validates Top-N and deduplication time/changelog constraints', () => {
    const plainTopN = eventTimeSpec();
    plainTopN.steps = [{
        type: 'topN',
        partitionBy: ['user'],
        orderBy: { column: 'amount', direction: 'DESC' },
        n: 2
    }];
    plainTopN.sink = { type: 'filesystem', path: '/tmp/out', format: 'json' };
    assert.ok(issuesFor(plainTopN).some(message =>
        message.includes('filesystem sink requires append-only rows')
    ));

    const windowTopN = eventTimeSpec();
    windowTopN.steps = [
        oneMinuteWindow(),
        {
            type: 'topN',
            partitionBy: ['window_start', 'window_end'],
            orderBy: { column: 'total', direction: 'DESC' },
            n: 2
        }
    ];
    windowTopN.sink = { type: 'filesystem', path: '/tmp/out', format: 'json' };
    assert.deepEqual(validateSpec(windowTopN), []);

    const deduplicate = eventTimeSpec();
    deduplicate.steps = [{
        type: 'deduplicate',
        keyColumns: ['user'],
        keep: 'first',
        orderColumn: 'amount'
    }];
    assert.ok(issuesFor(deduplicate).some(message =>
        message.includes('must be the current time attribute "event_time"')
    ));

    deduplicate.steps[0] = {
        type: 'deduplicate',
        keyColumns: ['user'],
        keep: 'first',
        orderColumn: 'event_time'
    };
    deduplicate.sink = { type: 'filesystem', path: '/tmp/out', format: 'json' };
    assert.ok(issuesFor(deduplicate).some(message =>
        message.includes('filesystem sink requires append-only rows')
    ));
});

test('returns path-based issues instead of throwing on untrusted malformed values', () => {
    assert.deepEqual(
        validateSpec({ version: 1, name: 'x', source: { type: 'datagen', fields: 'bad' }, steps: [{}], sink: { type: 'nope' } })
            .map(issue => issue.path),
        ['source.rowsPerSecond', 'source.fields', 'steps[0]', 'sink.type', 'settings']
    );
    const malformedSelect = {
        ...baseSpec(),
        steps: [{ type: 'select', columns: [], computed: 'not-an-array' }]
    };
    assert.ok(validateSpec(malformedSelect).some(issue => issue.path === 'steps[0]'));
});
