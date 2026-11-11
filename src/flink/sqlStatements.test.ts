import assert from 'node:assert/strict';
import { test } from 'node:test';
import { classifyStatement, redactSqlLiterals, splitSqlStatements } from './sqlStatements';

test('splits statements while preserving semicolons inside strings and identifiers', () => {
    assert.deepEqual(splitSqlStatements(
        "SELECT ';' AS `semi;colon`; SELECT 'it''s; fine'; SELECT `a``;b`; SELECT 'escaped \\'; still string';"
    ), [
        "SELECT ';' AS `semi;colon`",
        "SELECT 'it''s; fine'",
        'SELECT `a``;b`',
        "SELECT 'escaped \\'; still string'"
    ]);
});

test('ignores semicolons in line and block comments', () => {
    assert.deepEqual(splitSqlStatements(
        'SELECT 1 -- ; ignored\n; /* ; ignored */ SELECT 2;'
    ), [
        'SELECT 1 -- ; ignored',
        '/* ; ignored */ SELECT 2'
    ]);
});

test('drops empty and comment-only statements and trailing delimiters', () => {
    assert.deepEqual(splitSqlStatements(' ; \n -- comment ;\n /* comment; */ ; SELECT 1;;; '), [
        'SELECT 1'
    ]);
    assert.deepEqual(splitSqlStatements(' \n ; \t'), []);
});

test('classifies by the first non-comment keyword', () => {
    assert.equal(classifyStatement("/* note */ SELECT 'DROP'"), 'query');
    assert.equal(classifyStatement('-- comment\nWITH source AS (SELECT 1) SELECT * FROM source'), 'query');
    assert.equal(classifyStatement('-- WITH decoy AS (SELECT 1)\nWITH source AS (SELECT 1) INSERT INTO sink SELECT * FROM source'), 'insert');
    assert.equal(classifyStatement('INSERT INTO target SELECT 1'), 'insert');
    assert.equal(classifyStatement('DROP TABLE target'), 'ddl');
    assert.equal(classifyStatement('ALTER TABLE target ADD x INT'), 'ddl');
    assert.equal(classifyStatement('SET execution.parallelism = 2'), 'set');
    assert.equal(classifyStatement('SHOW TABLES'), 'show');
    assert.equal(classifyStatement('EXPLAIN SELECT 1'), 'explain');
    assert.equal(classifyStatement('USE catalog'), 'other');
});

test('redacts string literals, comments and secret-like settings before logging', () => {
    assert.equal(
        redactSqlLiterals("SET 'api.token' = 'sensitive'; -- hidden\nSELECT 'private'"),
        "SET '[literal]' = '[literal]'; --[comment]\nSELECT '[literal]'"
    );
    assert.equal(redactSqlLiterals('SET password=secret123'), 'SET password=[redacted]');
});
