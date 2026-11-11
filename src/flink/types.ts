export interface ClusterProfile {
    id: string;
    name: string;
    baseUrl: string;
    sqlGatewayUrl?: string;
    readOnly?: boolean;
}

export interface SqlGatewaySession {
    sessionHandle: string;
}

export interface SqlGatewayCloseSessionResponse {
    status: string;
}

export interface SqlGatewayOpenSessionRequest {
    properties?: Record<string, string>;
    sessionName?: string;
}

export interface SqlGatewayExecuteStatementOptions {
    executionConfig?: Record<string, string>;
    executionTimeout?: number;
}

export interface SqlGatewayOperationStatus {
    status: 'INITIALIZED' | 'PENDING' | 'RUNNING' | 'FINISHED' | 'CANCELED' | 'CLOSED' | 'ERROR' | 'TIMEOUT';
    error?: string;
    message?: string;
    exception?: string | { message?: string };
    [key: string]: unknown;
}

export type SqlGatewayResultType = 'NOT_READY' | 'PAYLOAD' | 'EOS';
export type SqlGatewayResultKind = 'SUCCESS' | 'SUCCESS_WITH_CONTENT';

export interface SqlGatewayResultRow {
    kind?: string;
    rowKind?: string;
    fields?: unknown[];
    [key: string]: unknown;
}

export interface SqlGatewayResultInfo {
    columnInfos?: Array<{ name?: string; [key: string]: unknown }>;
    columns?: Array<{ name?: string; [key: string]: unknown }>;
    data?: SqlGatewayResultRow[];
    resultSchema?: {
        columnNames?: string[];
        [key: string]: unknown;
    };
    [key: string]: unknown;
}

export interface SqlGatewayFetchResults {
    jobID?: string;
    nextResultUri?: string;
    queryResult?: boolean;
    isQueryResult?: boolean;
    resultKind?: SqlGatewayResultKind;
    resultType: SqlGatewayResultType;
    results?: SqlGatewayResultInfo;
}

export interface SqlGatewayRunOptions extends SqlGatewayExecuteStatementOptions {
    maxRows?: number;
    pollingIntervalMs?: number;
    signal?: AbortSignal;
}

export interface SqlGatewayStatementResult {
    operationHandle: string;
    status: SqlGatewayOperationStatus['status'];
    columns: string[];
    rows: SqlGatewayResultRow[];
    jobID?: string;
    durationMs: number;
    truncated: boolean;
    error?: string;
}

export interface FlinkJobOverview {
    jid: string;
    name: string;
    state: string;
    startTime?: number;
    duration?: number;
    tasks?: {
        total?: number;
        created?: number;
        scheduled?: number;
        deploying?: number;
        running?: number;
        finished?: number;
        canceling?: number;
        canceled?: number;
        failed?: number;
        reconciling?: number;
        initializing?: number;
    };
}

export interface FlinkJobsOverview {
    jobs?: FlinkJobOverview[];
}

export interface FlinkTaskManagers {
    taskmanagers?: Array<{
        id: string;
        slotsNumber?: number;
        freeSlots?: number;
        [key: string]: unknown;
    }>;
}

export interface FlinkJar {
    id: string;
    name: string;
    uploaded?: number;
    entry?: string;
    [key: string]: unknown;
}

export interface FlinkJars {
    files?: FlinkJar[];
}

export interface FlinkJobDetail extends FlinkJobOverview {
    vertices?: Array<{ id: string; name: string; status?: string; [key: string]: unknown }>;
}

export interface FlinkCheckpoints {
    counts?: Record<string, number>;
    latest?: {
        completed?: { id: number; status?: string; [key: string]: unknown };
        failed?: { id: number; status?: string; [key: string]: unknown };
        [key: string]: unknown;
    };
    history?: Array<{ id: number; status?: string; [key: string]: unknown }>;
    [key: string]: unknown;
}

export interface FlinkMetric {
    id: string;
    value?: string;
}

export interface FlinkAsyncStatus {
    status?: { id?: string };
    operation?: { status?: { id?: string }; [key: string]: unknown };
    [key: string]: unknown;
}
