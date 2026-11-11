import { FlinkApiError, FlinkClient } from './FlinkClient';
import {
    ClusterProfile,
    SqlGatewayCloseSessionResponse,
    SqlGatewayExecuteStatementOptions,
    SqlGatewayFetchResults,
    SqlGatewayOpenSessionRequest,
    SqlGatewayOperationStatus,
    SqlGatewayResultRow,
    SqlGatewayRunOptions,
    SqlGatewaySession,
    SqlGatewayStatementResult
} from './types';

const TERMINAL_STATUSES = new Set<SqlGatewayOperationStatus['status']>([
    'FINISHED',
    'CANCELED',
    'CLOSED',
    'ERROR',
    'TIMEOUT'
]);
const OPERATION_STATUSES = new Set<SqlGatewayOperationStatus['status']>([
    'INITIALIZED',
    'PENDING',
    'RUNNING',
    ...TERMINAL_STATUSES
]);
const RESULT_TYPES = new Set<SqlGatewayFetchResults['resultType']>(['NOT_READY', 'PAYLOAD', 'EOS']);

export class SqlGatewayClient {
    private readonly client: FlinkClient;
    private readonly clusterClient: FlinkClient;
    private readonly recoveredSessions = new WeakSet<SqlGatewaySession>();

    constructor(
        profile: ClusterProfile,
        private readonly timeoutMs = 10_000
    ) {
        if (!profile.sqlGatewayUrl) {
            throw new Error('This Flink profile does not have a SQL Gateway URL.');
        }
        this.client = new FlinkClient({ ...profile, baseUrl: profile.sqlGatewayUrl }, timeoutMs);
        this.clusterClient = new FlinkClient(profile, timeoutMs);
    }

    public openSession(request: SqlGatewayOpenSessionRequest = {}): Promise<SqlGatewaySession> {
        return this.client.post<SqlGatewaySession>('/v3/sessions', request).then(session => {
            if (!session || typeof session.sessionHandle !== 'string' || !session.sessionHandle) {
                throw new FlinkApiError('SQL Gateway returned an invalid sessionHandle.');
            }
            return session;
        });
    }

    public async closeSession(sessionHandle: string): Promise<SqlGatewayCloseSessionResponse> {
        const response = await this.client.delete<SqlGatewayCloseSessionResponse>(
            `/v3/sessions/${encodeURIComponent(sessionHandle)}`
        );
        if (!response || typeof response.status !== 'string') {
            throw new FlinkApiError('SQL Gateway returned an invalid close-session response.');
        }
        return response;
    }

    public executeStatement(
        sessionHandle: string,
        statement: string,
        options: SqlGatewayExecuteStatementOptions = {}
    ): Promise<{ operationHandle: string }> {
        return this.client.post<{ operationHandle: string }>(
            `/v3/sessions/${encodeURIComponent(sessionHandle)}/statements`,
            { statement, ...options }
        ).then(response => {
            if (!response || typeof response.operationHandle !== 'string' || !response.operationHandle) {
                throw new FlinkApiError('SQL Gateway returned an invalid operationHandle.');
            }
            return response;
        });
    }

    public async getStatus(sessionHandle: string, operationHandle: string): Promise<SqlGatewayOperationStatus> {
        const response = await this.client.get<SqlGatewayOperationStatus>(
            `/v3/sessions/${encodeURIComponent(sessionHandle)}/operations/${encodeURIComponent(operationHandle)}/status`
        );
        return validateStatus(response);
    }

    public fetchResults(
        sessionHandle: string,
        operationHandle: string,
        token: number
    ): Promise<SqlGatewayFetchResults> {
        return this.client.get<SqlGatewayFetchResults>(
            `/v3/sessions/${encodeURIComponent(sessionHandle)}/operations/${encodeURIComponent(operationHandle)}/result/${token}?rowFormat=JSON`
        ).then(response => {
            if (!response || !RESULT_TYPES.has(response.resultType)) {
                throw new FlinkApiError('SQL Gateway returned an invalid resultType.');
            }
            return response;
        });
    }

    public async cancelOperation(sessionHandle: string, operationHandle: string): Promise<SqlGatewayOperationStatus> {
        const response = await this.client.post<SqlGatewayOperationStatus>(
            `/v3/sessions/${encodeURIComponent(sessionHandle)}/operations/${encodeURIComponent(operationHandle)}/cancel`
        );
        return validateStatus(response);
    }

    public async closeOperation(sessionHandle: string, operationHandle: string): Promise<SqlGatewayOperationStatus> {
        const response = await this.client.delete<SqlGatewayOperationStatus>(
            `/v3/sessions/${encodeURIComponent(sessionHandle)}/operations/${encodeURIComponent(operationHandle)}/close`
        );
        return validateStatus(response);
    }

    public async runStatement(
        session: SqlGatewaySession,
        statement: string,
        options: SqlGatewayRunOptions = {}
    ): Promise<SqlGatewayStatementResult> {
        const maxRows = options.maxRows ?? 100;
        if (!Number.isInteger(maxRows) || maxRows < 0) {
            throw new RangeError('maxRows must be a non-negative integer.');
        }
        const pollingIntervalMs = options.pollingIntervalMs ?? 250;
        if (!Number.isFinite(pollingIntervalMs) || pollingIntervalMs < 0) {
            throw new RangeError('pollingIntervalMs must be a non-negative number.');
        }
        if (
            options.executionTimeout !== undefined
            && (!Number.isSafeInteger(options.executionTimeout) || options.executionTimeout < 0)
        ) {
            throw new RangeError('executionTimeout must be a non-negative integer number of seconds.');
        }
        const startedAt = Date.now();
        const executeOptions: SqlGatewayExecuteStatementOptions = {};
        if (options.executionConfig !== undefined) {
            executeOptions.executionConfig = options.executionConfig;
        }

        const operation = await this.executeWithSessionRecovery(session, statement, executeOptions);
        const operationSessionHandle = session.sessionHandle;
        const rows: SqlGatewayResultRow[] = [];
        let columns: string[] = [];
        let jobID: string | undefined;
        let truncated = false;
        let operationError: string | undefined;
        let status: SqlGatewayOperationStatus['status'] = 'INITIALIZED';
        const deadline = options.executionTimeout === undefined
            ? undefined
            : startedAt + options.executionTimeout * 1000;

        try {
            while (!TERMINAL_STATUSES.has(status)) {
                if (options.signal?.aborted) {
                    await this.cancelActiveOperation(session.sessionHandle, operation.operationHandle, jobID);
                    status = 'CANCELED';
                    break;
                }
                if (deadline !== undefined && Date.now() >= deadline) {
                    await this.cancelActiveOperation(session.sessionHandle, operation.operationHandle, jobID);
                    status = 'TIMEOUT';
                    break;
                }
                const operationStatus = await this.withStatusRecovery(session, operation.operationHandle);
                status = operationStatus.status;
                if (status === 'ERROR') {
                    operationError = operationStatus.error
                        ?? operationStatus.message
                        ?? (typeof operationStatus.exception === 'string'
                            ? operationStatus.exception
                            : operationStatus.exception?.message);
                }
                if (!TERMINAL_STATUSES.has(status)) {
                    await delay(pollingIntervalMs);
                }
            }

            if (status === 'FINISHED') {
                let token = 0;
                while (true) {
                    if (options.signal?.aborted) {
                        await this.cancelActiveOperation(session.sessionHandle, operation.operationHandle, jobID);
                        status = 'CANCELED';
                        break;
                    }
                    if (deadline !== undefined && Date.now() >= deadline) {
                        await this.cancelActiveOperation(session.sessionHandle, operation.operationHandle, jobID);
                        status = 'TIMEOUT';
                        break;
                    }

                    const page = await this.withResultsRecovery(session, operation.operationHandle, token);
                    const pageRows = page.results?.data ?? [];
                    const pageColumns = page.results?.resultSchema?.columnNames
                        ?? page.results?.columns?.map(column => column.name ?? '')
                        ?? page.results?.columnInfos?.map(column => column.name ?? '');
                    if (pageColumns && !columns.length) {
                        columns = pageColumns;
                    }
                    jobID = page.jobID ?? jobID;

                    if (page.resultType === 'NOT_READY') {
                        await delay(pollingIntervalMs);
                        token = nextToken(page.nextResultUri, token, true);
                        continue;
                    }

                    const remainingRows = maxRows - rows.length;
                    rows.push(...pageRows.slice(0, remainingRows));
                    const reachedLimit = rows.length >= maxRows;
                    if (reachedLimit && page.resultType !== 'EOS') {
                        truncated = true;
                        await this.cancelActiveOperation(session.sessionHandle, operation.operationHandle, jobID);
                        break;
                    }
                    if (reachedLimit && pageRows.length > remainingRows) {
                        truncated = true;
                        break;
                    }
                    if (page.resultType === 'EOS') {
                        break;
                    }
                    token = nextToken(page.nextResultUri, token, false);
                }
            }
        } finally {
            if (session.sessionHandle === operationSessionHandle) {
                await this.closeOperation(session.sessionHandle, operation.operationHandle);
            }
        }

        return {
            operationHandle: operation.operationHandle,
            status,
            columns,
            rows,
            jobID,
            durationMs: Date.now() - startedAt,
            truncated,
            ...(operationError ? { error: operationError } : {})
        };
    }

    private async executeWithSessionRecovery(
        session: SqlGatewaySession,
        statement: string,
        options: SqlGatewayExecuteStatementOptions
    ): Promise<{ operationHandle: string }> {
        try {
            return await this.executeStatement(session.sessionHandle, statement, options);
        } catch (error) {
            if (!isMissingSession(error) || !await this.reopenSession(session)) {
                throw error;
            }
            return this.executeStatement(session.sessionHandle, statement, options);
        }
    }

    private async withStatusRecovery(
        session: SqlGatewaySession,
        operationHandle: string
    ): Promise<SqlGatewayOperationStatus> {
        try {
            return await this.getStatus(session.sessionHandle, operationHandle);
        } catch (error) {
            await this.recoverExpiredSession(session, error);
            throw error;
        }
    }

    private async withResultsRecovery(
        session: SqlGatewaySession,
        operationHandle: string,
        token: number
    ): Promise<SqlGatewayFetchResults> {
        try {
            return await this.fetchResults(session.sessionHandle, operationHandle, token);
        } catch (error) {
            await this.recoverExpiredSession(session, error);
            throw error;
        }
    }

    private async recoverExpiredSession(session: SqlGatewaySession, error: unknown): Promise<void> {
        if (!isMissingSession(error)) {
            throw error;
        }
        if (await this.reopenSession(session)) {
            throw new FlinkApiError(
                'The SQL Gateway session expired. A new session was opened, but the current operation cannot be resumed.',
                404
            );
        }
        throw error;
    }

    private async reopenSession(session: SqlGatewaySession): Promise<boolean> {
        if (this.recoveredSessions.has(session)) {
            return false;
        }
        const replacement = await this.openSession();
        session.sessionHandle = replacement.sessionHandle;
        this.recoveredSessions.add(session);
        return true;
    }

    private async cancelActiveOperation(
        sessionHandle: string,
        operationHandle: string,
        jobID: string | undefined
    ): Promise<void> {
        if (!jobID) {
            await this.cancelOperation(sessionHandle, operationHandle);
            return;
        }
        if (!/^[0-9a-f]{32}$/i.test(jobID)) {
            throw new FlinkApiError(`SQL Gateway returned an invalid jobID: ${jobID}`);
        }
        const jobPath = `/jobs/${encodeURIComponent(jobID)}`;
        let job: { state?: string };
        try {
            job = await this.clusterClient.get<{ state?: string }>(jobPath);
        } catch (error) {
            if (error instanceof FlinkApiError && error.statusCode === 404) {
                await this.cancelOperation(sessionHandle, operationHandle);
                return;
            }
            throw error;
        }
        const state = job.state?.toUpperCase();
        if (!state) {
            throw new FlinkApiError(`Flink returned no state for job ${jobID}.`);
        }
        // Flink 1.20 marks streaming SELECT operations FINISHED while their jobs may still be running.
        if (!['FINISHED', 'CANCELED', 'CANCELLED', 'FAILED', 'SUSPENDED', 'CANCELING', 'CANCELLING'].includes(state)) {
            await this.clusterClient.patch(`${jobPath}?mode=cancel`);
        }
    }
}

function isMissingSession(error: unknown): boolean {
    return error instanceof FlinkApiError
        && error.statusCode === 404
        && /session.*(?:not found|does(?: not|n't) exist|expired)|(?:not found|does(?: not|n't) exist|expired).*session/i.test(error.message);
}

function validateStatus(response: SqlGatewayOperationStatus): SqlGatewayOperationStatus {
    if (!response || !OPERATION_STATUSES.has(response.status)) {
        throw new FlinkApiError('SQL Gateway returned an invalid operation status.');
    }
    return response;
}

function nextToken(uri: string | undefined, currentToken: number, allowSameToken: boolean): number {
    if (!uri) {
        throw new FlinkApiError('SQL Gateway did not provide a nextResultUri for a non-final result page.');
    }
    const match = /\/result\/(\d+)(?:\?|$)/.exec(uri);
    if (!match) {
        throw new FlinkApiError(`SQL Gateway returned an invalid nextResultUri: ${uri}`);
    }
    const token = Number(match[1]);
    if (!Number.isSafeInteger(token) || token < 0 || (!allowSameToken && token <= currentToken)) {
        throw new FlinkApiError(`SQL Gateway returned an invalid result token: ${match[1]}`);
    }
    return token;
}

function delay(milliseconds: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, milliseconds));
}
