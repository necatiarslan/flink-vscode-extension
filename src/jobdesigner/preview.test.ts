import assert from 'node:assert/strict';
import { test } from 'node:test';
import { isReadOnlySqlAllowed } from '../flink/sqlStatements';
import { compilePreview } from './compile';
import { applyChangelog, isCompilerGeneratedPreviewSequence } from './preview';
import { JobSpec } from './spec';

function sampleSpec(): JobSpec {
    return {
        version: 1,
        name: 'preview_spec',
        source: {
            type: 'filesystem',
            path: '/opt/flink/data/input',
            format: 'json',
            watch: true,
            fields: [
                { name: 'user', type: 'STRING' },
                { name: 'amount', type: 'DOUBLE' }
            ]
        },
        steps: [
            { type: 'filter', mode: 'all', conditions: [{ column: 'amount', op: '>=', value: 10 }] },
            { type: 'aggregate', groupBy: ['user'], aggregates: [{ fn: 'SUM', column: 'amount', as: 'total' }] }
        ],
        sink: { type: 'print' },
        settings: {}
    };
}

test('compiles source preview with explicit columns and bounded filesystem source', () => {
    const preview = compilePreview(sampleSpec(), 'source', { maxRows: 7 });
    assert.deepEqual(preview.statements, [
        "SET 'pipeline.name' = 'preview_spec'",
        "SET 'execution.checkpointing.interval' = '10 s'",
        "CREATE TEMPORARY TABLE `job_source` (`user` STRING, `amount` DOUBLE) WITH ('connector' = 'filesystem', 'path' = 'file:///opt/flink/data/input', 'format' = 'json')",
        'SELECT `user`, `amount` FROM `job_source`'
    ]);
    assert.deepEqual(preview.columns, [
        { name: 'user', type: 'STRING' },
        { name: 'amount', type: 'DOUBLE' }
    ]);
    assert.equal(preview.targetName, 'job_source');
});

test('compiles preview through the selected step and emits no sink or INSERT', () => {
    const preview = compilePreview(sampleSpec(), 0, { maxRows: 50 });
    assert.ok(preview.statements.includes(
        'CREATE TEMPORARY VIEW `job_step_1` AS SELECT `user`, `amount` FROM `job_source` WHERE `amount` >= 10'
    ));
    assert.equal(preview.statements.at(-1), 'SELECT `user`, `amount` FROM `job_step_1`');
    assert.equal(preview.statements.some(statement => /CREATE TEMPORARY TABLE `job_sink`|INSERT INTO/i.test(statement)), false);
    assert.deepEqual(preview.columns.map(column => column.name), ['user', 'amount']);
});

test('compiles aggregate preview with updating metadata and its group key', () => {
    const preview = compilePreview(sampleSpec(), 1, { maxRows: 20 });
    assert.equal(preview.changelogMode, 'UPDATING');
    assert.deepEqual(preview.keyColumns, ['user']);
    assert.equal(preview.statements.at(-1), 'SELECT `user`, `total` FROM `job_step_2`');
});

test('bounds watched filesystem and datagen sources for preview', () => {
    const filesystem = compilePreview(sampleSpec(), 'source', { maxRows: 12 });
    assert.equal(filesystem.statements.some(statement => statement.includes('source.monitor-interval')), false);

    const datagenSpec = sampleSpec();
    datagenSpec.source = {
        type: 'datagen',
        rowsPerSecond: 4,
        fields: [{ name: 'amount', type: 'BIGINT', kind: 'random', min: 1, max: 10 }]
    };
    const datagen = compilePreview(datagenSpec, 'source', { maxRows: 12 });
    assert.ok(datagen.statements.some(statement =>
        statement.includes("'rows-per-second' = '1000'")
        && statement.includes("'number-of-rows' = '48'")
    ));
});

test('read-only profiles can use compiler previews but the typed-SQL DDL rule remains blocked', () => {
    const preview = compilePreview(sampleSpec(), 'source', { maxRows: 5 });
    assert.ok(preview.statements.some(statement => statement.startsWith('CREATE TEMPORARY TABLE `job_source`')));
    assert.equal(preview.statements.some(statement => /\bINSERT\b/i.test(statement)), false);
    assert.equal(isCompilerGeneratedPreviewSequence(preview.statements), true);
    assert.equal(isCompilerGeneratedPreviewSequence([...preview.statements.slice(0, -1), 'INSERT INTO arbitrary_table']), false);
    assert.equal(isReadOnlySqlAllowed('ddl'), false);
    assert.equal(isReadOnlySqlAllowed('query'), true);
});

test('applies insert, update-before/update-after, and delete changelog kinds by key', () => {
    const rows = [
        { kind: '+I', user: 'alice', total: 1 },
        { kind: '+I', user: 'bob', total: 2 },
        { kind: '-U', user: 'alice', total: 1 },
        { kind: '+U', user: 'alice', total: 3 },
        { kind: '-D', user: 'bob', total: 2 }
    ];
    assert.deepEqual(applyChangelog(rows, ['user']), [
        { user: 'alice', total: 3 }
    ]);
});

test('uses explicit row-kind metadata and hides final-state mode when there is no key', () => {
    assert.deepEqual(
        applyChangelog([{ $rowKind: '+I', user: 'alice', total: 3 }], ['user']),
        [{ user: 'alice', total: 3 }]
    );
    assert.equal(applyChangelog([{ kind: '+I', user: 'alice' }], []), undefined);
});
