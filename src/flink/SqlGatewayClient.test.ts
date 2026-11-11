import assert from 'node:assert/strict';
import { AddressInfo } from 'node:net';
import { createServer, IncomingMessage, ServerResponse } from 'node:http';
import { afterEach, test } from 'node:test';
import { ClusterProfile } from './types';
import { SqlGatewayClient } from './SqlGatewayClient';
import { classifyStatement, isReadOnlySqlAllowed } from './sqlStatements';

interface MockRequest {
    method: string;
    url: string;
    body: unknown;
}

const servers: ReturnType<typeof createServer>[] = [];

afterEach(async () => {
    await Promise.all(servers.splice(0).map(server => new Promise<void>((resolve, reject) => {
        server.close(error => error ? reject(error) : resolve());
    })));
});

test('uses the SQL Gateway v3 paths, query and request bodies', async () => {
    const requests: MockRequest[] = [];
    const server = mockServer(requests, request => {
        if (request.url === '/v3/sessions' && request.method === 'POST') {
            return { sessionHandle: 'session-id' };
        }
        if (request.url === '/v3/sessions/session-id/statements') {
            return { operationHandle: 'operation-id' };
        }
        if (request.url?.endsWith('/status')) {
            return { status: 'FINISHED' };
        }
        if (request.url?.includes('/result/')) {
            return { resultType: 'EOS', results: { data: [] } };
        }
        return { status: 'CLOSED' };
    });
    const client = new SqlGatewayClient(profile(await listen(server)));

    const session = await client.openSession();
    const operation = await client.executeStatement(session.sessionHandle, 'SELECT 1', {
        executionConfig: { 'pipeline.name': 'hello.sql' },
        executionTimeout: 60
    });
    await client.getStatus(session.sessionHandle, operation.operationHandle);
    await client.fetchResults(session.sessionHandle, operation.operationHandle, 0);
    await client.cancelOperation(session.sessionHandle, operation.operationHandle);
    await client.closeOperation(session.sessionHandle, operation.operationHandle);
    await client.closeSession(session.sessionHandle);

    assert.deepEqual(requests.map(({ method, url }) => [method, url]), [
        ['POST', '/v3/sessions'],
        ['POST', '/v3/sessions/session-id/statements'],
        ['GET', '/v3/sessions/session-id/operations/operation-id/status'],
        ['GET', '/v3/sessions/session-id/operations/operation-id/result/0?rowFormat=JSON'],
        ['POST', '/v3/sessions/session-id/operations/operation-id/cancel'],
        ['DELETE', '/v3/sessions/session-id/operations/operation-id/close'],
        ['DELETE', '/v3/sessions/session-id']
    ]);
    assert.deepEqual(requests.map(request => request.body), [
        {},
        {
            statement: 'SELECT 1',
            executionConfig: { 'pipeline.name': 'hello.sql' },
            executionTimeout: 60
        },
        undefined,
        undefined,
        undefined,
        undefined,
        undefined
    ]);
});

test('polls and fetches result pages until EOS', async () => {
    const requests: MockRequest[] = [];
    let statusPolls = 0;
    const server = mockServer(requests, request => {
        if (request.url === '/v3/sessions') {
            return { sessionHandle: 's' };
        }
        if (request.url === '/v3/sessions/s/statements') {
            return { operationHandle: 'o' };
        }
        if (request.url?.endsWith('/status')) {
            statusPolls++;
            return { status: statusPolls === 1 ? 'RUNNING' : 'FINISHED' };
        }
        if (request.url?.endsWith('/result/0?rowFormat=JSON')) {
            return {
                resultType: 'PAYLOAD',
                nextResultUri: '/v3/sessions/s/operations/o/result/1?rowFormat=JSON',
                results: { columns: [{ name: 'value' }], data: [{ kind: '+I', fields: [1] }] }
            };
        }
        if (request.url?.endsWith('/result/1?rowFormat=JSON')) {
            return {
                resultType: 'PAYLOAD',
                nextResultUri: '/v3/sessions/s/operations/o/result/2?rowFormat=JSON',
                results: { data: [{ kind: '+I', fields: [2] }] }
            };
        }
        if (request.url?.endsWith('/result/2?rowFormat=JSON')) {
            return { resultType: 'EOS', results: { data: [] } };
        }
        return { status: 'CLOSED' };
    });
    const client = new SqlGatewayClient(profile(await listen(server)));

    const session = await client.openSession();
    const result = await client.runStatement(session, 'SELECT value', { pollingIntervalMs: 0 });

    assert.equal(result.status, 'FINISHED');
    assert.deepEqual(result.columns, ['value']);
    assert.deepEqual(result.rows.map(row => row.fields), [[1], [2]]);
    assert.equal(result.truncated, false);
    assert.deepEqual(requests.filter(request => request.url.includes('/result/')).map(request => request.url), [
        '/v3/sessions/s/operations/o/result/0?rowFormat=JSON',
        '/v3/sessions/s/operations/o/result/1?rowFormat=JSON',
        '/v3/sessions/s/operations/o/result/2?rowFormat=JSON'
    ]);
    assert.equal(requests.at(-1)?.url, '/v3/sessions/s/operations/o/close');
});

test('retains the Flink operation error text for failed previews', async () => {
    const server = mockServer([], request => {
        if (request.url === '/v3/sessions') {
            return { sessionHandle: 's' };
        }
        if (request.url === '/v3/sessions/s/statements') {
            return { operationHandle: 'o' };
        }
        if (request.url?.endsWith('/status')) {
            return { status: 'ERROR', error: 'The source path does not exist.' };
        }
        return { status: 'CLOSED' };
    });
    const client = new SqlGatewayClient(profile(await listen(server)));
    const session = await client.openSession();
    const result = await client.runStatement(session, 'SELECT * FROM missing_source');
    assert.equal(result.status, 'ERROR');
    assert.equal(result.error, 'The source path does not exist.');
});

test('cancels the operation when the row limit truncates a page', async () => {
    const requests: MockRequest[] = [];
    const server = mockServer(requests, request => {
        if (request.url === '/v3/sessions') {
            return { sessionHandle: 's' };
        }
        if (request.url === '/v3/sessions/s/statements') {
            return { operationHandle: 'o' };
        }
        if (request.url?.endsWith('/status')) {
            return { status: 'FINISHED' };
        }
        if (request.url?.includes('/result/')) {
            return {
                resultType: 'PAYLOAD',
                nextResultUri: '/v3/sessions/s/operations/o/result/1?rowFormat=JSON',
                results: { data: [{ kind: '+I', fields: [1] }, { kind: '+I', fields: [2] }] }
            };
        }
        return { status: 'CANCELED' };
    });
    const client = new SqlGatewayClient(profile(await listen(server)));

    const session = await client.openSession();
    const result = await client.runStatement(session, 'SELECT value', { maxRows: 1 });

    assert.equal(result.rows.length, 1);
    assert.equal(result.truncated, true);
    assert.ok(requests.some(request =>
        request.method === 'POST' && request.url === '/v3/sessions/s/operations/o/cancel'
    ));
});

test('cancels a running Flink job when truncating a streaming result', async () => {
    const gatewayRequests: MockRequest[] = [];
    const restRequests: MockRequest[] = [];
    const jobId = 'a'.repeat(32);
    const gateway = mockServer(gatewayRequests, request => {
        if (request.url === '/v3/sessions') {
            return { sessionHandle: 's' };
        }
        if (request.url === '/v3/sessions/s/statements') {
            return { operationHandle: 'o' };
        }
        if (request.url?.endsWith('/status')) {
            return { status: 'FINISHED' };
        }
        if (request.url?.includes('/result/')) {
            return {
                jobID: jobId,
                resultType: 'PAYLOAD',
                nextResultUri: '/v3/sessions/s/operations/o/result/1?rowFormat=JSON',
                results: { data: [{ kind: 'INSERT', fields: [1] }, { kind: 'INSERT', fields: [2] }] }
            };
        }
        return { status: 'CLOSED' };
    });
    const rest = mockServer(restRequests, request => {
        if (request.method === 'GET') {
            return { state: 'RUNNING' };
        }
        return { status: 'CANCELED' };
    });
    const [gatewayPort, restPort] = await Promise.all([listen(gateway), listen(rest)]);
    const client = new SqlGatewayClient(profile(gatewayPort, restPort));

    const session = await client.openSession();
    const result = await client.runStatement(session, 'SELECT value', { maxRows: 1 });

    assert.equal(result.truncated, true);
    assert.deepEqual(restRequests.map(request => [request.method, request.url]), [
        ['GET', `/jobs/${jobId}`],
        ['PATCH', `/jobs/${jobId}?mode=cancel`]
    ]);
    assert.ok(!gatewayRequests.some(request => request.url.endsWith('/cancel')));
});

test('applies execution timeout locally and cancels without sending the unsupported option', async () => {
    const requests: MockRequest[] = [];
    const server = mockServer(requests, request => {
        if (request.url === '/v3/sessions') {
            return { sessionHandle: 's' };
        }
        if (request.url === '/v3/sessions/s/statements') {
            return { operationHandle: 'o' };
        }
        return { status: 'CANCELED' };
    });
    const client = new SqlGatewayClient(profile(await listen(server)));

    const session = await client.openSession();
    const result = await client.runStatement(session, 'SELECT 1', { executionTimeout: 0 });

    assert.equal(result.status, 'TIMEOUT');
    assert.deepEqual(requests[1].body, { statement: 'SELECT 1' });
    assert.ok(requests.some(request => request.url === '/v3/sessions/s/operations/o/cancel'));
});

test('reopens a missing session once before retrying statement execution', async () => {
    const requests: MockRequest[] = [];
    let sessionsOpened = 0;
    const server = mockServer(requests, request => {
        if (request.url === '/v3/sessions') {
            sessionsOpened++;
            return { sessionHandle: sessionsOpened === 1 ? 'expired' : 'replacement' };
        }
        if (request.url === '/v3/sessions/expired/statements') {
            return { statusCode: 404, body: { errors: ['Session expired does not exist'] } };
        }
        if (request.url === '/v3/sessions/replacement/statements') {
            return { operationHandle: 'o' };
        }
        if (request.url?.endsWith('/status')) {
            return { status: 'FINISHED' };
        }
        if (request.url?.includes('/result/')) {
            return { resultType: 'EOS', results: { data: [] } };
        }
        return { status: 'CLOSED' };
    });
    const client = new SqlGatewayClient(profile(await listen(server)));

    const session = await client.openSession();
    const result = await client.runStatement(session, 'SELECT 1', { pollingIntervalMs: 0 });

    assert.equal(result.status, 'FINISHED');
    assert.equal(session.sessionHandle, 'replacement');
    assert.equal(sessionsOpened, 2);
    assert.ok(requests.some(request =>
        request.url === '/v3/sessions/replacement/operations/o/close'
    ));
});

test('read-only SQL profiles allow only query, show, explain and set statements', () => {
    const statements = [
        ['SELECT 1', true],
        ['SHOW TABLES', true],
        ['EXPLAIN SELECT 1', true],
        ['SET parallelism.default = 1', true],
        ['INSERT INTO sink SELECT 1', false],
        ['CREATE TABLE source (id INT)', false],
        ['DROP TABLE source', false],
        ['USE CATALOG other', false]
    ] as const;

    for (const [statement, allowed] of statements) {
        assert.equal(isReadOnlySqlAllowed(classifyStatement(statement)), allowed, statement);
    }
    assert.equal(
        isReadOnlySqlAllowed(classifyStatement('WITH source AS (SELECT 1) INSERT INTO sink SELECT * FROM source')),
        false
    );
});

function mockServer(
    requests: MockRequest[],
    respond: (request: MockRequest) => unknown
): ReturnType<typeof createServer> {
    const server = createServer((request, response) => void handleRequest(request, response, requests, respond));
    servers.push(server);
    return server;
}

async function handleRequest(
    request: IncomingMessage,
    response: ServerResponse,
    requests: MockRequest[],
    respond: (request: MockRequest) => unknown
): Promise<void> {
    const chunks: Buffer[] = [];
    for await (const chunk of request) {
        chunks.push(Buffer.from(chunk));
    }
    const text = Buffer.concat(chunks).toString('utf8');
    const mockRequest: MockRequest = {
        method: request.method ?? 'GET',
        url: request.url ?? '/',
        body: text ? JSON.parse(text) as unknown : undefined
    };
    requests.push(mockRequest);
    const result = respond(mockRequest);
    if (isMockResponse(result)) {
        response.writeHead(result.statusCode, { 'content-type': 'application/json' });
        response.end(JSON.stringify(result.body));
    } else {
        response.writeHead(200, { 'content-type': 'application/json' });
        response.end(JSON.stringify(result));
    }
}

function isMockResponse(value: unknown): value is { statusCode: number; body: unknown } {
    return typeof value === 'object'
        && value !== null
        && 'statusCode' in value
        && 'body' in value
        && typeof value.statusCode === 'number';
}

function listen(server: ReturnType<typeof createServer>): Promise<number> {
    return new Promise((resolve, reject) => {
        server.once('error', reject);
        server.listen(0, '127.0.0.1', () => {
            resolve((server.address() as AddressInfo).port);
        });
    });
}

function profile(port: number, restPort = 8081): ClusterProfile {
    return {
        id: 'test',
        name: 'test',
        baseUrl: `http://127.0.0.1:${restPort}`,
        sqlGatewayUrl: `http://127.0.0.1:${port}`
    };
}
