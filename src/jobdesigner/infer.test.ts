import assert from 'node:assert/strict';
import { test } from 'node:test';
import { inferSample } from './infer';

test('infers JSON primitive, timestamp, date, boolean, and nested types', () => {
    const result = inferSample([
        '{"id":1,"price":1.25,"active":true,"iso":"2026-11-11T09:30:00","sql":"2026-11-11 09:30:00","day":"2026-11-11","nested":{"x":1},"array":[1,2]}',
        '{"id":2,"price":2,"active":false,"iso":"2026-11-11T09:31:00","sql":"2026-11-11 09:31:00","day":"2026-11-12","nested":{"x":2},"array":[3]}'
    ].join('\n'), 'json');
    assert.deepEqual(result.fields.map(field => [field.name, field.type]), [
        ['id', 'BIGINT'],
        ['price', 'DOUBLE'],
        ['active', 'BOOLEAN'],
        ['iso', 'TIMESTAMP(3)'],
        ['sql', 'TIMESTAMP(3)'],
        ['day', 'DATE'],
        ['nested', 'STRING'],
        ['array', 'STRING']
    ]);
    assert.equal(result.timestampFormat, undefined);
    assert.equal(result.eventTimeSuggestion, undefined);
});

test('widens integer and fractional JSON numbers to DOUBLE with a confidence note', () => {
    const result = inferSample('{"amount":1}\n{"amount":1.5}', 'json');
    assert.equal(result.fields[0].type, 'DOUBLE');
    assert.match(result.fields[0].confidence ?? '', /widened to DOUBLE/);
});

test('unions keys in first-seen order and reports missing values', () => {
    const result = inferSample('{"first":1}\n{"second":"x","first":2}', 'json');
    assert.deepEqual(result.fields.map(field => field.name), ['first', 'second']);
    assert.equal(result.fields[0].confidence, undefined);
    assert.match(result.fields[1].confidence ?? '', /Missing in 1 sampled row/);
});

test('handles BOM, CRLF, and blank lines', () => {
    const result = inferSample('\uFEFF\r\n{"name":"a"}\r\n\r\n{"name":"b"}\r\n', 'json');
    assert.equal(result.sampledRows, 2);
    assert.equal(result.fields[0].name, 'name');
});

test('reports the original line number for a non-JSON line', () => {
    assert.throws(
        () => inferSample('\n{"ok":1}\r\nnot-json\r\n', 'json'),
        /Line 3 is not valid JSON/
    );
});

test('rejects empty samples and bounds inference at 64 KB', () => {
    assert.throws(() => inferSample('\uFEFF \r\n', 'json'), /empty/);
    const rows = `${'{"value":1}\n'.repeat(7000)}{"incomplete":`;
    const result = inferSample(rows, 'json');
    assert.ok(result.truncated);
    assert.ok(result.sampledBytes <= 64 * 1024);
    assert.ok(result.sampledRows > 0);
    assert.equal(result.fields.some(field => field.name === 'incomplete'), false);
});

test('infers ISO timestamps and proposes event time without applying it', () => {
    const result = inferSample('{"user":"a","event_time":"2026-11-11T09:30:00"}', 'json');
    assert.equal(result.timestampFormat, 'ISO-8601');
    assert.deepEqual(result.eventTimeSuggestion, {
        column: 'event_time',
        lateness: { value: 2, unit: 'SECOND' }
    });
});

test('infers CSV delimiters, headers, sanitized names, and value types', () => {
    for (const delimiter of [',', ';', '\t', '|']) {
        const result = inferSample(
            `user${delimiter}amount${delimiter}event-time\nalice${delimiter}2.5${delimiter}2026-11-11T09:30:00`,
            'csv'
        );
        assert.equal(result.delimiter, delimiter);
        assert.equal(result.hasHeader, true);
        assert.deepEqual(result.fields.map(field => field.name), ['user', 'amount', 'event_time']);
        assert.deepEqual(result.fields.map(field => field.type), ['STRING', 'DOUBLE', 'TIMESTAMP(3)']);
        assert.equal(result.timestampFormat, 'ISO-8601');
    }
});

test('uses generated names for headerless CSV and detects integer/boolean/date values', () => {
    const result = inferSample('12,true,2026-11-11\n13,false,2026-11-12', 'csv');
    assert.equal(result.hasHeader, false);
    assert.deepEqual(result.fields.map(field => [field.name, field.type]), [
        ['col_1', 'BIGINT'],
        ['col_2', 'BOOLEAN'],
        ['col_3', 'DATE']
    ]);
});

test('detects an all-string CSV header from common field names', () => {
    const result = inferSample('name,city\nAlice,Paris\nBob,Rome', 'csv');
    assert.equal(result.hasHeader, true);
    assert.deepEqual(result.fields.map(field => field.name), ['name', 'city']);
});

test('rejects malformed CSV quoting with a line number', () => {
    assert.throws(() => inferSample('a,b\n"unterminated,2', 'csv'), /Line 2.*unterminated/);
});
