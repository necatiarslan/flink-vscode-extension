export type ColumnType =
    | 'STRING'
    | 'INT'
    | 'BIGINT'
    | 'DOUBLE'
    | 'BOOLEAN'
    | 'DATE'
    | 'TIMESTAMP(3)'
    | `DECIMAL(${number},${number})`;

export interface Column {
    name: string;
    type: ColumnType;
}

export interface Duration {
    value: number;
    unit: 'SECOND' | 'MINUTE' | 'HOUR' | 'DAY';
}

export type TimeAttributeSpec =
    | { kind: 'processingTime' }
    | { kind: 'eventTime'; column: string; maxOutOfOrderness: Duration };

export interface DatagenField {
    name: string;
    type: ColumnType;
    kind: 'random' | 'sequence';
    min?: number;
    max?: number;
    length?: number;
}

export interface DatagenSource {
    type: 'datagen';
    rowsPerSecond: number;
    fields: DatagenField[];
    timeAttribute?: TimeAttributeSpec;
}

export interface JsonSourceOptions {
    ignoreParseErrors?: boolean;
    timestampFormat?: 'SQL' | 'ISO-8601';
}

export interface CsvSourceOptions {
    delimiter?: string;
    ignoreParseErrors?: boolean;
}

export interface FilesystemRawSource {
    type: 'filesystem';
    path: string;
    format: 'raw';
    watch: boolean;
    timeAttribute?: TimeAttributeSpec;
    columnName?: string;
    fields?: never;
    options?: never;
}

export interface FilesystemJsonSource {
    type: 'filesystem';
    path: string;
    format: 'json';
    watch: boolean;
    timeAttribute?: TimeAttributeSpec;
    fields: Column[];
    options?: JsonSourceOptions;
}

export interface FilesystemCsvSource {
    type: 'filesystem';
    path: string;
    format: 'csv';
    watch: boolean;
    timeAttribute?: TimeAttributeSpec;
    fields: Column[];
    options?: CsvSourceOptions;
}

export type FilesystemSource = FilesystemRawSource | FilesystemJsonSource | FilesystemCsvSource;
export type SourceSpec = DatagenSource | FilesystemSource;

export type ComparisonOperator =
    | '='
    | '<>'
    | '<'
    | '<='
    | '>'
    | '>='
    | 'IS NULL'
    | 'IS NOT NULL'
    | 'LIKE'
    | 'IN';

export type ScalarValue = string | number | boolean | null;

export interface FilterCondition {
    column: string;
    op: ComparisonOperator;
    value?: ScalarValue | ScalarValue[];
}

export interface FilterStep {
    type: 'filter';
    mode: 'all' | 'any';
    conditions: FilterCondition[];
}

export interface SelectColumn {
    name: string;
    as?: string;
}

export type Expression =
    | { type: 'column'; name: string }
    | { type: 'literal'; value: ScalarValue }
    | { type: 'binary'; op: '+' | '-' | '*' | '/'; left: Expression; right: Expression }
    | {
        type: 'call';
        fn: 'UPPER' | 'LOWER' | 'TRIM' | 'CONCAT' | 'ABS' | 'ROUND';
        args: Expression[];
    }
    | { type: 'call'; fn: 'CAST'; args: [Expression]; to: ColumnType }
    | { type: 'raw'; text: string };

export interface ComputedColumn {
    as: string;
    expr: Expression;
}

export interface SelectStep {
    type: 'select';
    columns: SelectColumn[];
    computed?: ComputedColumn[];
}

export interface JsonValueField {
    path: string;
    as: string;
    type: ColumnType;
}

export interface JsonValueParseStep {
    type: 'parse';
    kind: 'jsonValue';
    column: string;
    fields: JsonValueField[];
}

export interface SplitToRowsParseStep {
    type: 'parse';
    kind: 'splitToRows';
    column: string;
    delimiter: string;
    as: string;
}

export type ParseStep = JsonValueParseStep | SplitToRowsParseStep;

export interface AggregateSpec {
    fn: 'COUNT' | 'SUM' | 'AVG' | 'MIN' | 'MAX';
    column?: string;
    as: string;
}

export interface AggregateStep {
    type: 'aggregate';
    groupBy: string[];
    aggregates: AggregateSpec[];
}

export type WindowSpec =
    | { kind: 'tumble'; size: Duration }
    | { kind: 'hop'; slide: Duration; size: Duration }
    | { kind: 'cumulate'; step: Duration; maxSize: Duration }
    | { kind: 'session'; gap: Duration };

export interface WindowAggregateStep {
    type: 'windowAggregate';
    window: WindowSpec;
    groupBy: string[];
    aggregates: AggregateSpec[];
    includeWindowStart: boolean;
    includeWindowEnd: boolean;
}

export interface TopNStep {
    type: 'topN';
    partitionBy: string[];
    orderBy: { column: string; direction: 'ASC' | 'DESC' };
    n: number;
    rankColumn?: string;
}

export interface DeduplicateStep {
    type: 'deduplicate';
    keyColumns: string[];
    keep: 'first' | 'last';
    orderColumn: string;
}

export type StepSpec =
    | FilterStep
    | SelectStep
    | ParseStep
    | AggregateStep
    | WindowAggregateStep
    | TopNStep
    | DeduplicateStep;

export interface PrintSink {
    type: 'print';
}

export interface BlackholeSink {
    type: 'blackhole';
}

export interface FilesystemSink {
    type: 'filesystem';
    path: string;
    format: 'json' | 'csv';
}

export type SinkSpec = PrintSink | BlackholeSink | FilesystemSink;

export interface JobSettings {
    parallelism?: number;
    checkpointIntervalSeconds?: number;
    stateTtlSeconds?: number;
    idleTimeoutSeconds?: number;
}

export interface JobSpec {
    version: 1;
    name: string;
    source: SourceSpec;
    steps: StepSpec[];
    sink: SinkSpec;
    settings: JobSettings;
}

export interface ValidationIssue {
    path: string;
    severity: 'error' | 'warning';
    message: string;
}
