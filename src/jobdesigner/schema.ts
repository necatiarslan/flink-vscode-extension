import { SqlGatewayClient } from '../flink/SqlGatewayClient';
import { SqlGatewayResultRow, SqlGatewaySession } from '../flink/types';
import { compileSpec } from './compile';
import { Column, ColumnType, JobSpec } from './spec';

export interface ResolvedJobSchema {
    source: Column[];
    schemaByStep: Column[][];
    finalSchema: Column[];
    sourceTimeAttribute?: string;
    timeAttributeByStep: (string | undefined)[];
}

export async function resolveJobSchema(
    client: SqlGatewayClient,
    spec: JobSpec
): Promise<ResolvedJobSchema> {
    const compiled = compileSpec(spec);
    const sourceStatementIndex = compiled.statements.findIndex(statement =>
        statement.startsWith('CREATE TEMPORARY TABLE `job_source`')
    );
    if (sourceStatementIndex < 0) {
        throw new Error('Compiled job is missing its source table statement.');
    }

    const session = await client.openSession();
    try {
        for (const statement of compiled.statements.slice(0, sourceStatementIndex + 1)) {
            await runRequired(client, session, statement);
        }

        const sourceDescription = await describe(client, session, 'job_source');
        const source = sourceDescription.columns;
        const schemaByStep: Column[][] = [];
        const timeAttributeByStep: (string | undefined)[] = [];
        for (let index = 0; index < spec.steps.length; index++) {
            const viewStatement = compiled.statements.find(statement =>
                statement.startsWith(`CREATE TEMPORARY VIEW \`job_step_${index + 1}\``)
            );
            if (!viewStatement) {
                throw new Error(`Compiled job is missing step ${index + 1}.`);
            }
            await runRequired(client, session, viewStatement);
            const description = await describe(client, session, `job_step_${index + 1}`);
            schemaByStep.push(description.columns);
            timeAttributeByStep.push(description.timeAttribute);
        }
        return {
            source,
            schemaByStep,
            finalSchema: schemaByStep.at(-1) ?? source,
            ...(sourceDescription.timeAttribute ? { sourceTimeAttribute: sourceDescription.timeAttribute } : { sourceTimeAttribute: undefined }),
            timeAttributeByStep
        };
    } finally {
        await client.closeSession(session.sessionHandle);
    }
}

async function runRequired(
    client: SqlGatewayClient,
    session: SqlGatewaySession,
    statement: string
): Promise<void> {
    const result = await client.runStatement(session, statement, { maxRows: 10_000 });
    if (result.status !== 'FINISHED') {
        throw new Error(`SQL Gateway did not finish schema setup statement: ${statement}`);
    }
}

async function describe(
    client: SqlGatewayClient,
    session: SqlGatewaySession,
    tableName: string
): Promise<{ columns: Column[]; timeAttribute?: string }> {
    const result = await client.runStatement(
        session,
        `DESCRIBE \`${tableName.replace(/`/g, '``')}\``,
        { maxRows: 10_000 }
    );
    if (result.status !== 'FINISHED' || result.truncated) {
        throw new Error(`SQL Gateway could not describe ${tableName}.`);
    }
    const nameIndex = result.columns.indexOf('name');
    const typeIndex = result.columns.indexOf('type');
    if (nameIndex < 0 || typeIndex < 0) {
        throw new Error(`SQL Gateway returned an unexpected DESCRIBE schema for ${tableName}.`);
    }
    let timeAttribute: string | undefined;
    const columns = result.rows.map((row, index) => {
        const values = rowValues(row);
        const name = values[nameIndex];
        const type = values[typeIndex];
        if (typeof name !== 'string' || typeof type !== 'string') {
            throw new Error(`SQL Gateway returned an invalid DESCRIBE row ${index + 1} for ${tableName}.`);
        }
        if (/\*(?:ROWTIME|PROCTIME)\*/i.test(type)) {
            if (timeAttribute) {
                throw new Error(`SQL Gateway described more than one time attribute for ${tableName}.`);
            }
            timeAttribute = name;
        }
        return { name, type: normalizeType(type) };
    });
    if (!columns.length) {
        throw new Error(`SQL Gateway returned no columns for ${tableName}.`);
    }
    return { columns, ...(timeAttribute ? { timeAttribute } : {}) };
}

function rowValues(row: SqlGatewayResultRow): unknown[] {
    if (!Array.isArray(row.fields)) {
        throw new Error('SQL Gateway returned a DESCRIBE row without fields.');
    }
    return row.fields;
}

function normalizeType(type: string): ColumnType {
    const normalized = type.trim().toUpperCase().replace(/\*(?:ROWTIME|PROCTIME)\*/g, '').replace(/\s+/g, '');
    if (normalized === 'VARCHAR' || normalized === 'CHAR' || normalized === 'STRING') {
        return 'STRING';
    }
    if (normalized === 'INTEGER') {
        return 'INT';
    }
    if (normalized === 'FLOAT' || normalized === 'REAL') {
        return 'DOUBLE';
    }
    if (normalized === 'TIMESTAMP' || normalized === 'TIMESTAMP_LTZ(3)') {
        return 'TIMESTAMP(3)';
    }
    if (
        normalized === 'INT'
        || normalized === 'BIGINT'
        || normalized === 'DOUBLE'
        || normalized === 'BOOLEAN'
        || normalized === 'DATE'
        || normalized === 'TIMESTAMP(3)'
        || /^DECIMAL\((?:[1-9]|[12]\d|3[0-8]),(?:0|[1-9]\d?)\)$/.test(normalized)
    ) {
        return normalized as ColumnType;
    }
    throw new Error(`SQL Gateway returned unsupported column type "${type}".`);
}
