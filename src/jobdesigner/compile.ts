import {
    AggregateSpec,
    Column,
    ColumnType,
    Duration,
    Expression,
    JobSpec,
    ScalarValue,
    StepSpec,
    TimeAttributeSpec,
    WindowSpec,
    ValidationIssue
} from './spec';

const ALLOWED_TYPES = new Set([
    'STRING',
    'INT',
    'BIGINT',
    'DOUBLE',
    'BOOLEAN',
    'DATE',
    'TIMESTAMP(3)'
]);
const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;
const CONTROL_CHARACTERS = /[\u0000-\u001f\u007f]/;

export interface CompiledJob {
    statements: string[];
    schemaByStep: Column[][];
    sourceMetadata: StepPipelineMetadata;
    pipelineByStep: StepPipelineMetadata[];
    changelogMode: ChangelogMode;
}

export interface CompiledPreview {
    statements: string[];
    columns: Column[];
    keyColumns?: string[];
    changelogMode: ChangelogMode;
    targetName: string;
}

export interface CompilePreviewOptions {
    maxRows: number;
}

export interface ResolvedSchema {
    source: Column[];
    schemaByStep: Column[][];
    sourceTimeAttribute?: string;
    timeAttributeByStep?: (string | undefined)[];
}

export type ChangelogMode = 'APPEND-ONLY' | 'UPDATING';

export interface StepPipelineMetadata {
    schema: Column[];
    timeAttribute?: string;
    lostTimeAttributeAt?: string;
    changelogMode: ChangelogMode;
}

export function compileSpec(spec: JobSpec, resolvedSchema?: ResolvedSchema): CompiledJob {
    const issues = validateSpec(spec).filter(item => item.severity === 'error');
    if (issues.length) {
        throw new Error(issues.map(item => `${item.path}: ${item.message}`).join('\n'));
    }

    if (resolvedSchema) {
        validateResolvedSchema(spec, resolvedSchema);
    }
    const sourceSchema = resolvedSchema?.source ?? sourceSchemaForCompile(spec);
    const schemaByStep: Column[][] = [];
    let inputSchema = sourceSchema;
    const statements = [
        `SET 'pipeline.name' = ${sqlString(spec.name)}`,
        `SET 'execution.checkpointing.interval' = ${sqlString(`${spec.settings.checkpointIntervalSeconds ?? 10} s`)}`
    ];
    if (spec.settings.parallelism !== undefined) {
        statements.splice(1, 0, `SET 'parallelism.default' = ${sqlString(String(spec.settings.parallelism))}`);
    }
    if (spec.settings.stateTtlSeconds !== undefined) {
        statements.push(`SET 'table.exec.state.ttl' = ${sqlString(`${spec.settings.stateTtlSeconds} s`)}`);
    }
    if (spec.settings.idleTimeoutSeconds !== undefined) {
        statements.push(`SET 'table.exec.source.idle-timeout' = ${sqlString(`${spec.settings.idleTimeoutSeconds} s`)}`);
    }

    statements.push(compileSource(spec, sourceSchema));
    let inputTimeAttribute = sourceTimeAttributeColumn(spec.source.timeAttribute);
    spec.steps.forEach((step, index) => {
        const outputSchema = resolvedSchema?.schemaByStep[index] ?? schemaForStep(step, inputSchema);
        statements.push(compileStep(step, inputSchema, index, inputTimeAttribute));
        inputTimeAttribute = stepTimeAttribute(step, inputSchema, outputSchema, inputTimeAttribute);
        schemaByStep.push(outputSchema);
        inputSchema = outputSchema;
    });
    statements.push(compileSink(spec.sink, inputSchema));
    statements.push(
        `INSERT INTO ${quoteIdentifier('job_sink')} SELECT ${inputSchema.map(column => quoteIdentifier(column.name)).join(', ')} FROM ${quoteIdentifier(spec.steps.length ? `job_step_${spec.steps.length}` : 'job_source')}`
    );
    const pipeline = analyzePipeline(spec, sourceSchema, schemaByStep, resolvedSchema);
    return {
        statements,
        schemaByStep,
        sourceMetadata: pipeline.source,
        pipelineByStep: pipeline.steps,
        changelogMode: pipeline.changelogMode
    };
}

export function compilePreview(
    spec: JobSpec,
    target: 'source' | number,
    options: CompilePreviewOptions
): CompiledPreview {
    if (!Number.isInteger(options.maxRows) || options.maxRows < 1) {
        throw new RangeError('Preview maxRows must be a positive integer.');
    }
    if (target !== 'source' && (!Number.isInteger(target) || target < 0 || target >= spec.steps.length)) {
        throw new RangeError('Preview step index must refer to an existing step.');
    }
    const steps = target === 'source' ? [] : spec.steps.slice(0, target + 1);
    const previewSpec: JobSpec = {
        ...spec,
        steps,
        sink: { type: 'print' }
    };
    const compiled = compileSpec(previewSpec);
    const sourceSchema = sourceSchemaForCompile(previewSpec);
    const sourceIndex = compiled.statements.findIndex(statement =>
        statement.startsWith('CREATE TEMPORARY TABLE `job_source`')
    );
    if (sourceIndex < 0) {
        throw new Error('Compiled preview is missing its source table statement.');
    }
    const targetName = steps.length ? `job_step_${steps.length}` : 'job_source';
    const columns = steps.length ? compiled.schemaByStep[steps.length - 1] : sourceSchema;
    const boundedSource: JobSpec = {
        ...previewSpec,
        source: previewSource(previewSpec.source)
    };
    const statements = [
        ...compiled.statements.slice(0, sourceIndex),
        compileSource(boundedSource, sourceSchema, { maxRows: options.maxRows }),
        ...compiled.statements.slice(sourceIndex + 1, sourceIndex + 1 + steps.length),
        `SELECT ${selectColumnList(columns)} FROM ${quoteIdentifier(targetName)}`
    ];
    const pipeline = analyzePipeline(previewSpec, sourceSchema, compiled.schemaByStep);
    const keyColumns = previewKeyColumns(steps);
    return {
        statements,
        columns,
        ...(keyColumns?.length ? { keyColumns } : {}),
        changelogMode: pipeline.changelogMode,
        targetName
    };
}

function previewSource(source: JobSpec['source']): JobSpec['source'] {
    return source.type === 'datagen'
        ? { ...source, rowsPerSecond: 1000 }
        : { ...source, watch: false };
}

function previewKeyColumns(steps: StepSpec[]): string[] | undefined {
    let keys: string[] | undefined;
    for (const step of steps) {
        if (step.type === 'aggregate') {
            keys = step.groupBy.length ? [...step.groupBy] : undefined;
        } else if (step.type === 'windowAggregate') {
            const windowKeys = [
                ...(step.includeWindowStart ? ['window_start'] : []),
                ...(step.includeWindowEnd ? ['window_end'] : []),
                ...step.groupBy
            ];
            keys = windowKeys.length ? windowKeys : undefined;
        } else if (step.type === 'topN') {
            keys = step.rankColumn ? [...step.partitionBy, step.rankColumn] : undefined;
        } else if (step.type === 'deduplicate') {
            keys = step.keyColumns.length ? [...step.keyColumns] : undefined;
        } else if (step.type === 'select' && keys) {
            const mapped = keys.map(key => {
                const output = step.columns.find(column => column.name === key);
                return output ? output.as ?? output.name : key;
            });
            keys = mapped.every(key => step.columns.some(column => (column.as ?? column.name) === key))
                ? mapped
                : undefined;
        }
    }
    return keys;
}

export function analyzePipeline(
    spec: JobSpec,
    sourceSchema: Column[] = sourceSchemaForCompile(spec),
    resolvedSchemas?: Column[][],
    resolvedMetadata?: ResolvedSchema
): { source: StepPipelineMetadata; steps: StepPipelineMetadata[]; changelogMode: ChangelogMode } {
    const hasResolvedSourceTime = resolvedMetadata
        && Object.prototype.hasOwnProperty.call(resolvedMetadata, 'sourceTimeAttribute');
    let timeAttribute = hasResolvedSourceTime
        ? resolvedMetadata.sourceTimeAttribute
        : sourceTimeAttributeColumn(spec.source.timeAttribute);
    let lostTimeAttributeAt: string | undefined;
    let changelogMode: ChangelogMode = 'APPEND-ONLY';
    let windowContext = false;
    const source: StepPipelineMetadata = {
        schema: sourceSchema,
        ...(timeAttribute ? { timeAttribute } : {}),
        changelogMode
    };
    let input = sourceSchema;
    const steps = spec.steps.map((step, index): StepPipelineMetadata => {
        const output = resolvedSchemas?.[index] ?? schemaForStep(step, input);
        const nextTime = resolvedMetadata?.timeAttributeByStep
            ? resolvedMetadata.timeAttributeByStep[index]
            : stepTimeAttribute(step, input, output, timeAttribute);
        if (timeAttribute && !nextTime) {
            lostTimeAttributeAt = `steps[${index}] (${step.type})`;
        }
        timeAttribute = nextTime;
        windowContext = stepWindowContext(step, input, output, windowContext);
        changelogMode = stepChangelogMode(step, changelogMode);
        input = output;
        return {
            schema: output,
            ...(timeAttribute ? { timeAttribute } : {}),
            ...(lostTimeAttributeAt ? { lostTimeAttributeAt } : {}),
            changelogMode
        };
    });
    return { source, steps, changelogMode };
}

function sourceTimeAttributeColumn(attribute: TimeAttributeSpec | undefined): string | undefined {
    if (!attribute) {
        return undefined;
    }
    return attribute.kind === 'processingTime' ? 'proc_time' : attribute.column;
}

function validateResolvedSchema(spec: JobSpec, schema: ResolvedSchema): void {
    if (schema.schemaByStep.length !== spec.steps.length) {
        throw new Error('Resolved Flink schema does not match the JobSpec step count.');
    }
    let expectedSchema = sourceSchemaForCompile(spec);
    const schemas: Array<[string, Column[], Column[]]> = [
        ['source', expectedSchema, schema.source]
    ];
    spec.steps.forEach((step, index) => {
        const nextSchema = schemaForStep(step, expectedSchema);
        schemas.push([`steps[${index}]`, nextSchema, schema.schemaByStep[index]]);
        expectedSchema = nextSchema;
    });
    for (const [path, expected, columns] of schemas) {
        if (expected.length !== columns.length || expected.some((column, index) => column.name !== columns[index]?.name)) {
            throw new Error(`Resolved Flink schema column names do not match the JobSpec at ${path}.`);
        }
        const names = new Set<string>();
        for (const column of columns) {
            if (
                typeof column.name !== 'string'
                || !column.name.trim()
                || CONTROL_CHARACTERS.test(column.name)
                || typeof column.type !== 'string'
                || !isAllowedType(column.type)
            ) {
                throw new Error(`Resolved Flink schema contains an invalid column at ${path}.`);
            }
            const key = column.name.toLocaleLowerCase('en-US');
            if (names.has(key)) {
                throw new Error(`Resolved Flink schema contains duplicate columns at ${path}.`);
            }
            names.add(key);
        }
    }
}

export function validateSpec(input: unknown): ValidationIssue[] {
    const issues: ValidationIssue[] = [];
    if (!isRecord(input)) {
        return [issue('$', 'Expected a job specification object.')];
    }
    if (input['version'] !== 1) {
        issues.push(issue('version', 'Version must be 1.'));
    }
    if (!validName(input['name'])) {
        issues.push(issue('name', 'Job name must be a non-empty SQL-safe name without control characters.'));
    }
    if (!isRecord(input['source'])) {
        issues.push(issue('source', 'A source is required.'));
        return issues;
    }
    if (!Array.isArray(input['steps'])) {
        issues.push(issue('steps', 'Steps must be an array.'));
        return issues;
    }
    if (!isRecord(input['sink'])) {
        issues.push(issue('sink', 'A sink is required.'));
        return issues;
    }

    let schema = validateSource(input['source'], issues);
    const timeAttribute = validateTimeAttribute(input['source'], schema, issues);
    if (timeAttribute?.spec.kind === 'processingTime') {
        schema = [...schema, { name: 'proc_time', type: 'TIMESTAMP(3)' }];
    }
    let currentTimeAttribute = timeAttribute?.column;
    let lostTimeAttributeAt: string | undefined;
    let windowContext = false;
    let changelogMode: ChangelogMode = 'APPEND-ONLY';
    for (const [index, rawStep] of input['steps'].entries()) {
        const path = `steps[${index}]`;
        if (!isRecord(rawStep) || typeof rawStep['type'] !== 'string') {
            issues.push(issue(path, 'Step must have a recognized type.'));
            continue;
        }
        const step = rawStep as unknown as StepSpec;
        const previousSchema = schema;
        schema = validateStep(step, schema, path, issues, currentTimeAttribute, lostTimeAttributeAt, windowContext);
        const nextTimeAttribute = stepTimeAttribute(step, previousSchema, schema, currentTimeAttribute);
        if (currentTimeAttribute && !nextTimeAttribute) {
            lostTimeAttributeAt = `steps[${index}] (${step.type})`;
        }
        currentTimeAttribute = nextTimeAttribute;
        windowContext = stepWindowContext(step, previousSchema, schema, windowContext);
        changelogMode = stepChangelogMode(step, changelogMode);
    }

    validateSink(input['sink'], issues);
    if (changelogMode === 'UPDATING' && input['sink']['type'] === 'filesystem') {
        issues.push(issue(
            'sink',
            'This job updates grouped results over time or otherwise emits changes, but a filesystem sink requires append-only rows. Choose print or blackhole.'
        ));
    }
    if (input['settings'] === undefined) {
        issues.push(issue('settings', 'Settings are required; use an empty object when no settings are specified.'));
    } else {
        validateSettings(input['settings'], issues);
    }
    return issues;
}

function validateTimeAttribute(
    source: Record<string, unknown>,
    schema: Column[],
    issues: ValidationIssue[]
): { column?: string; spec: TimeAttributeSpec } | undefined {
    const raw = source['timeAttribute'];
    if (raw === undefined) {
        return undefined;
    }
    if (!isRecord(raw) || typeof raw['kind'] !== 'string') {
        issues.push(issue('source.timeAttribute', 'Time attribute must be processingTime or eventTime.'));
        return undefined;
    }
    if (raw['kind'] === 'processingTime') {
        if (schema.some(column => column.name === 'proc_time')) {
            issues.push(issue('source.timeAttribute', 'Processing time uses the reserved output column name "proc_time".'));
        }
        return { column: 'proc_time', spec: { kind: 'processingTime' } };
    }
    if (raw['kind'] !== 'eventTime') {
        issues.push(issue('source.timeAttribute.kind', 'Time attribute kind must be processingTime or eventTime.'));
        return undefined;
    }
    const column = findColumn(schema, raw['column'], 'source.timeAttribute.column', issues);
    if (column && column.type !== 'TIMESTAMP(3)') {
        issues.push(issue('source.timeAttribute.column', 'Event-time column must have type TIMESTAMP(3).'));
    }
    validateDuration(raw['maxOutOfOrderness'], 'source.timeAttribute.maxOutOfOrderness', issues);
    return {
        ...(typeof raw['column'] === 'string' ? { column: raw['column'] } : {}),
        spec: raw as unknown as TimeAttributeSpec
    };
}

function validateSource(source: Record<string, unknown>, issues: ValidationIssue[]): Column[] {
    if (source['type'] === 'datagen') {
        if (!positiveInteger(source['rowsPerSecond'])) {
            issues.push(issue('source.rowsPerSecond', 'Rows per second must be a positive integer.'));
        }
        if (!Array.isArray(source['fields']) || !source['fields'].length) {
            issues.push(issue('source.fields', 'Datagen needs at least one field.'));
            return [];
        }
        const columns: Column[] = [];
        const names = new Set<string>();
        source['fields'].forEach((rawField, index) => {
            const path = `source.fields[${index}]`;
            if (!isRecord(rawField)) {
                issues.push(issue(path, 'Field must be an object.'));
                return;
            }
            const field = rawField as Record<string, unknown>;
            const name = checkName(field['name'], `${path}.name`, issues);
            const type = checkType(field['type'], `${path}.type`, issues);
            if (name && names.has(name)) {
                issues.push(issue('source.fields', `Duplicate output column "${name}".`));
            } else if (name) {
                names.add(name);
            }
            if (field['kind'] !== 'random' && field['kind'] !== 'sequence') {
                issues.push(issue(`${path}.kind`, 'Kind must be random or sequence.'));
            }
            if (field['min'] !== undefined && !finiteNumber(field['min'])) {
                issues.push(issue(`${path}.min`, 'Minimum must be a finite number.'));
            }
            if (field['max'] !== undefined && !finiteNumber(field['max'])) {
                issues.push(issue(`${path}.max`, 'Maximum must be a finite number.'));
            }
            if (field['kind'] === 'sequence' && (field['min'] === undefined || field['max'] === undefined)) {
                issues.push(issue(path, 'Sequence fields need finite min/max values with min no greater than max.'));
            } else if (field['min'] !== undefined && field['max'] !== undefined
                && finiteNumber(field['min']) && finiteNumber(field['max'])
                && Number(field['min']) > Number(field['max'])) {
                issues.push(issue(path, 'Minimum must not exceed maximum.'));
            }
            if (field['kind'] === 'sequence' && type && !isNumericType(type)) {
                issues.push(issue(`${path}.kind`, 'Sequence generation is only supported for numeric fields.'));
            }
            if ((field['min'] !== undefined || field['max'] !== undefined) && type && !isNumericType(type)) {
                issues.push(issue(path, 'Minimum and maximum are only supported for numeric fields.'));
            }
            if (field['length'] !== undefined && (!Number.isInteger(field['length']) || Number(field['length']) < 1)) {
                issues.push(issue(`${path}.length`, 'Length must be a positive integer.'));
            }
            if (field['length'] !== undefined && type && type !== 'STRING') {
                issues.push(issue(`${path}.length`, 'Length is only supported for STRING fields.'));
            }
            if (field['length'] !== undefined && field['kind'] === 'sequence') {
                issues.push(issue(`${path}.length`, 'Length is only supported for random STRING fields.'));
            }
            if (name && type) {
                columns.push({ name, type });
            }
        });
        return columns;
    }

    if (source['type'] !== 'filesystem') {
        issues.push(issue('source.type', 'Source type must be datagen or filesystem.'));
        return [];
    }
    const path = checkText(source['path'], 'source.path', issues);
    const format = source['format'];
    if (!['raw', 'json', 'csv'].includes(String(format))) {
        issues.push(issue('source.format', 'Format must be raw, json, or csv.'));
    }
    if (typeof source['watch'] !== 'boolean') {
        issues.push(issue('source.watch', 'Watch must be true or false.'));
    } else if (source['watch'] && path && /\.[A-Za-z0-9]+$/.test(path.split(/[\\/]/).pop() ?? '')) {
        issues.push(issue('source.path', 'Watch mode requires a directory path, not a file path.'));
    }
    if (format === 'raw') {
        if (source['options'] !== undefined) {
            issues.push(issue('source.options', 'Raw format does not accept format options.'));
        }
        if (source['fields'] !== undefined) {
            issues.push(issue('source.fields', 'Raw format has exactly one STRING column; configure columnName instead of fields.'));
        }
        const name = source['columnName'] === undefined ? 'line' : checkName(source['columnName'], 'source.columnName', issues);
        return name ? [{ name, type: 'STRING' }] : [];
    }
    if (!Array.isArray(source['fields']) || !source['fields'].length) {
        issues.push(issue('source.fields', 'Structured filesystem sources need at least one field.'));
        return [];
    }
    const columns = validateColumns(source['fields'], 'source.fields', issues);
    const options = source['options'];
    if (options !== undefined && !isRecord(options)) {
        issues.push(issue('source.options', 'Options must be an object.'));
    } else if (isRecord(options)) {
        const allowedOptions = format === 'json'
            ? new Set(['ignoreParseErrors', 'timestampFormat'])
            : format === 'csv'
                ? new Set(['ignoreParseErrors', 'delimiter'])
                : new Set<string>();
        for (const key of Object.keys(options)) {
            if (!allowedOptions.has(key)) {
                issues.push(issue(`source.options.${key}`, `Option "${key}" is not supported for ${String(format)} sources.`));
            }
        }
        if (options['ignoreParseErrors'] !== undefined && typeof options['ignoreParseErrors'] !== 'boolean') {
            issues.push(issue('source.options.ignoreParseErrors', 'ignoreParseErrors must be true or false.'));
        }
        if (format === 'json' && options['timestampFormat'] !== undefined
            && options['timestampFormat'] !== 'SQL' && options['timestampFormat'] !== 'ISO-8601') {
            issues.push(issue('source.options.timestampFormat', 'Timestamp format must be SQL or ISO-8601.'));
        }
        if (format !== 'csv' && options['delimiter'] !== undefined) {
            issues.push(issue('source.options.delimiter', 'Delimiter is only supported for CSV sources.'));
        } else if (format === 'csv' && options['delimiter'] !== undefined
            && (typeof options['delimiter'] !== 'string' || options['delimiter'].length !== 1 || CONTROL_CHARACTERS.test(options['delimiter']))) {
            issues.push(issue('source.options.delimiter', 'CSV delimiter must be one printable character.'));
        }
    }
    return columns;
}

function validateStep(
    step: StepSpec,
    inputSchema: Column[],
    path: string,
    issues: ValidationIssue[],
    timeAttribute?: string,
    lostTimeAttributeAt?: string,
    windowContext = false
): Column[] {
    switch (step.type) {
        case 'filter':
            if (step.mode !== 'all' && step.mode !== 'any') {
                issues.push(issue(`${path}.mode`, 'Mode must be all or any.'));
            }
            if (!Array.isArray(step.conditions) || step.conditions.length === 0) {
                issues.push(issue(`${path}.conditions`, 'Filter needs at least one condition.'));
                return inputSchema;
            }
            step.conditions.forEach((condition, index) => {
                const conditionPath = `${path}.conditions[${index}]`;
                if (!isRecord(condition)) {
                    issues.push(issue(conditionPath, 'Condition must be an object.'));
                    return;
                }
                const column = findColumn(inputSchema, condition['column'], `${conditionPath}.column`, issues);
                const op = condition['op'];
                const noValue = op === 'IS NULL' || op === 'IS NOT NULL';
                if (noValue && condition['value'] !== undefined) {
                    issues.push(issue(`${conditionPath}.value`, `${String(op)} does not take a value.`));
                } else if (!noValue && condition['value'] === undefined) {
                    issues.push(issue(`${conditionPath}.value`, `${String(op)} requires a value.`));
                }
                if (!['=', '<>', '<', '<=', '>', '>=', 'IS NULL', 'IS NOT NULL', 'LIKE', 'IN'].includes(String(op))) {
                    issues.push(issue(`${conditionPath}.op`, 'Unsupported filter operator.'));
                }
                if (op === 'LIKE' && column?.type !== 'STRING') {
                    issues.push(issue(`${conditionPath}.op`, 'LIKE requires a STRING column.'));
                }
                if (op === 'IN') {
                    if (!Array.isArray(condition['value']) || condition['value'].length === 0) {
                        issues.push(issue(`${conditionPath}.value`, 'IN requires a non-empty list of values.'));
                    } else if (column) {
                        condition['value'].forEach((value, valueIndex) =>
                            validateLiteral(value, column.type, `${conditionPath}.value[${valueIndex}]`, issues)
                        );
                    }
                } else if (!noValue && condition['value'] !== undefined) {
                    if (Array.isArray(condition['value'])) {
                        issues.push(issue(`${conditionPath}.value`, 'Only IN accepts a list of values.'));
                    } else if (column) {
                        validateLiteral(condition['value'], column.type, `${conditionPath}.value`, issues);
                    }
                }
            });
            return inputSchema;
        case 'select': {
            const output: Column[] = [];
            if (!Array.isArray(step.columns) || (step.computed !== undefined && !Array.isArray(step.computed))) {
                issues.push(issue(path, 'Select columns and computed expressions must be arrays.'));
                return output;
            }
            step.columns.forEach((selection, index) => {
                const selectionPath = `${path}.columns[${index}]`;
                if (!isRecord(selection)) {
                    issues.push(issue(selectionPath, 'Selected column must be an object.'));
                    return;
                }
                const column = findColumn(inputSchema, selection['name'], `${selectionPath}.name`, issues);
                const name = selection['as'] === undefined
                    ? column?.name
                    : checkName(selection['as'], `${selectionPath}.as`, issues);
                if (column && name) {
                    output.push({ name, type: column.type });
                }
            });
            (step.computed ?? []).forEach((computed, index) => {
                const computedPath = `${path}.computed[${index}]`;
                if (!isRecord(computed)) {
                    issues.push(issue(computedPath, 'Computed column must be an object.'));
                    return;
                }
                const name = checkName(computed['as'], `${computedPath}.as`, issues);
                const expression = validateExpression(computed['expr'], inputSchema, `${computedPath}.expr`, issues);
                if (name) {
                    output.push({ name, type: expression });
                }
            });
            checkUniqueColumns(output, path, issues);
            if (!output.length) {
                issues.push(issue(path, 'Select needs at least one output column.'));
            }
            return output;
        }
        case 'parse':
            if (!findColumn(inputSchema, step.column, `${path}.column`, issues)) {
                return inputSchema;
            }
            if (step.kind === 'splitToRows') {
                const name = checkName(step.as, `${path}.as`, issues);
                if (typeof step.delimiter !== 'string' || step.delimiter.length === 0 || CONTROL_CHARACTERS.test(step.delimiter)) {
                    issues.push(issue(`${path}.delimiter`, 'Delimiter must be non-empty and contain no control characters.'));
                }
                const output = [...inputSchema];
                if (name) {
                    output.push({ name, type: 'STRING' });
                }
                checkUniqueColumns(output, path, issues);
                return output;
            }
            if (step.kind === 'jsonValue') {
                if (!Array.isArray(step.fields) || !step.fields.length) {
                    issues.push(issue(`${path}.fields`, 'JSON_VALUE needs at least one output field.'));
                    return inputSchema;
                }
                const output = [...inputSchema];
                step.fields.forEach((field, index) => {
                    const fieldPath = `${path}.fields[${index}]`;
                    if (!isRecord(field)) {
                        issues.push(issue(fieldPath, 'Parsed field must be an object.'));
                        return;
                    }
                    const name = checkName(field['as'], `${fieldPath}.as`, issues);
                    checkText(field['path'], `${fieldPath}.path`, issues);
                    const type = checkType(field['type'], `${fieldPath}.type`, issues);
                    if (name && type) {
                        output.push({ name, type });
                    }
                });
                checkUniqueColumns(output, path, issues);
                return output;
            }
            issues.push(issue(`${path}.kind`, 'Parse kind must be jsonValue or splitToRows.'));
            return inputSchema;
        case 'aggregate': {
            if (!Array.isArray(step.groupBy) || !Array.isArray(step.aggregates)) {
                issues.push(issue(path, 'Aggregate groupBy and aggregates must be arrays.'));
                return [];
            }
            const output: Column[] = [];
            step.groupBy.forEach((name, index) => {
                const column = findColumn(inputSchema, name, `${path}.groupBy[${index}]`, issues);
                if (column) {
                    output.push(column);
                }
            });
            output.push(...validateAggregates(step.aggregates, inputSchema, `${path}.aggregates`, issues));
            checkUniqueColumns(output, path, issues);
            return output;
        }
        case 'windowAggregate': {
            if (!timeAttribute) {
                issues.push(issue(path, timeAttributeRequired(lostTimeAttributeAt, 'Window aggregation')));
            }
            validateWindow(step.window, `${path}.window`, issues);
            if (!Array.isArray(step.groupBy) || !Array.isArray(step.aggregates)) {
                issues.push(issue(path, 'Window aggregate groupBy and aggregates must be arrays.'));
                return [];
            }
            if (typeof step.includeWindowStart !== 'boolean') {
                issues.push(issue(`${path}.includeWindowStart`, 'Include window start must be true or false.'));
            }
            if (typeof step.includeWindowEnd !== 'boolean') {
                issues.push(issue(`${path}.includeWindowEnd`, 'Include window end must be true or false.'));
            }
            const output: Column[] = [];
            if (step.includeWindowStart) {
                output.push({ name: 'window_start', type: 'TIMESTAMP(3)' });
            }
            if (step.includeWindowEnd) {
                output.push({ name: 'window_end', type: 'TIMESTAMP(3)' });
            }
            step.groupBy.forEach((name, index) => {
                const column = findColumn(inputSchema, name, `${path}.groupBy[${index}]`, issues);
                if (column) {
                    output.push(column);
                }
            });
            output.push(...validateAggregates(step.aggregates, inputSchema, `${path}.aggregates`, issues));
            checkUniqueColumns(output, path, issues);
            return output;
        }
        case 'topN': {
            if (!Array.isArray(step.partitionBy)) {
                issues.push(issue(`${path}.partitionBy`, 'Top-N partitionBy must be an array.'));
                return [];
            }
            step.partitionBy.forEach((name, index) => {
                findColumn(inputSchema, name, `${path}.partitionBy[${index}]`, issues);
            });
            if (!isRecord(step.orderBy)) {
                issues.push(issue(`${path}.orderBy`, 'Top-N needs an order column and direction.'));
            } else {
                findColumn(inputSchema, step.orderBy.column, `${path}.orderBy.column`, issues);
                if (step.orderBy.direction !== 'ASC' && step.orderBy.direction !== 'DESC') {
                    issues.push(issue(`${path}.orderBy.direction`, 'Direction must be ASC or DESC.'));
                }
            }
            if (!positiveInteger(step.n)) {
                issues.push(issue(`${path}.n`, 'Top-N must be a positive integer.'));
            }
            const isWindowed = step.partitionBy.includes('window_start') && step.partitionBy.includes('window_end');
            if (isWindowed && !timeAttribute && !windowContext) {
                issues.push(issue(path, timeAttributeRequired(lostTimeAttributeAt, 'Window Top-N')));
            }
            const output = [...inputSchema];
            if (step.rankColumn !== undefined) {
                const rank = checkName(step.rankColumn, `${path}.rankColumn`, issues);
                if (rank) {
                    output.push({ name: rank, type: 'BIGINT' });
                }
            }
            checkUniqueColumns(output, path, issues);
            return output;
        }
        case 'deduplicate': {
            if (!Array.isArray(step.keyColumns)) {
                issues.push(issue(`${path}.keyColumns`, 'Deduplication keyColumns must be an array.'));
                return inputSchema;
            }
            if (!step.keyColumns.length) {
                issues.push(issue(`${path}.keyColumns`, 'Deduplication needs at least one key column.'));
            }
            step.keyColumns.forEach((name, index) => {
                findColumn(inputSchema, name, `${path}.keyColumns[${index}]`, issues);
            });
            const orderColumn = findColumn(inputSchema, step.orderColumn, `${path}.orderColumn`, issues);
            if (!timeAttribute) {
                issues.push(issue(path, timeAttributeRequired(lostTimeAttributeAt, 'Deduplication')));
            } else if (step.orderColumn !== timeAttribute) {
                issues.push(issue(`${path}.orderColumn`, `Deduplication order column must be the current time attribute "${timeAttribute}".`));
            }
            if (orderColumn && orderColumn.type !== 'TIMESTAMP(3)') {
                issues.push(issue(`${path}.orderColumn`, 'Deduplication order column must be a TIMESTAMP(3) time attribute.'));
            }
            if (step.keep !== 'first' && step.keep !== 'last') {
                issues.push(issue(`${path}.keep`, 'Deduplication must keep first or last.'));
            }
            return inputSchema;
        }
        default:
            issues.push(issue(path, 'Unsupported step type.'));
            return inputSchema;
    }
}

function validateAggregates(
    rawAggregates: unknown[],
    inputSchema: Column[],
    path: string,
    issues: ValidationIssue[]
): Column[] {
    if (!rawAggregates.length) {
        issues.push(issue(path, 'At least one aggregate expression is required.'));
    }
    const output: Column[] = [];
    rawAggregates.forEach((aggregate, index) => {
        const aggregatePath = `${path}[${index}]`;
        if (!isRecord(aggregate)) {
            issues.push(issue(aggregatePath, 'Aggregate must be an object.'));
            return;
        }
        const name = checkName(aggregate['as'], `${aggregatePath}.as`, issues);
        const fn = aggregate['fn'];
        let type: ColumnType | undefined;
        if (fn === 'COUNT') {
            type = 'BIGINT';
            if (aggregate['column'] !== undefined && aggregate['column'] !== '*') {
                findColumn(inputSchema, aggregate['column'], `${aggregatePath}.column`, issues);
            }
        } else if (fn === 'SUM' || fn === 'AVG' || fn === 'MIN' || fn === 'MAX') {
            const column = findColumn(inputSchema, aggregate['column'], `${aggregatePath}.column`, issues);
            if (column) {
                type = fn === 'AVG' ? 'DOUBLE' : column.type;
                if ((fn === 'SUM' || fn === 'AVG') && !isNumericType(column.type)) {
                    issues.push(issue(`${aggregatePath}.column`, `${fn} requires a numeric column.`));
                }
            }
        } else {
            issues.push(issue(`${aggregatePath}.fn`, 'Unsupported aggregate function.'));
        }
        if (name && type) {
            output.push({ name, type });
        }
    });
    return output;
}

function validateWindow(raw: unknown, path: string, issues: ValidationIssue[]): void {
    if (!isRecord(raw) || typeof raw['kind'] !== 'string') {
        issues.push(issue(path, 'Window must have a recognized kind.'));
        return;
    }
    const kind = raw['kind'];
    const duration = (field: string): Duration | undefined =>
        validateDuration(raw[field], `${path}.${field}`, issues);
    if (kind === 'tumble') {
        duration('size');
    } else if (kind === 'hop') {
        duration('slide');
        duration('size');
    } else if (kind === 'cumulate') {
        const step = duration('step');
        const maxSize = duration('maxSize');
        if (step && maxSize && durationSeconds(maxSize) % durationSeconds(step) !== 0) {
            issues.push(issue(`${path}.maxSize`, 'Cumulative max size must be a multiple of the step.'));
        }
    } else if (kind === 'session') {
        duration('gap');
    } else {
        issues.push(issue(`${path}.kind`, 'Window kind must be tumble, hop, cumulate, or session.'));
    }
}

function validateDuration(raw: unknown, path: string, issues: ValidationIssue[]): Duration | undefined {
    if (
        !isRecord(raw)
        || !positiveInteger(raw['value'])
        || !['SECOND', 'MINUTE', 'HOUR', 'DAY'].includes(String(raw['unit']))
    ) {
        issues.push(issue(path, 'Duration requires a positive integer value and a supported unit.'));
        return undefined;
    }
    return { value: Number(raw['value']), unit: raw['unit'] as Duration['unit'] };
}

function durationSeconds(duration: Duration): number {
    const multiplier = duration.unit === 'SECOND'
        ? 1
        : duration.unit === 'MINUTE'
            ? 60
            : duration.unit === 'HOUR'
                ? 3_600
                : 86_400;
    return duration.value * multiplier;
}

function timeAttributeRequired(lostAt: string | undefined, operation: string): string {
    return lostAt
        ? `${operation} requires an input time attribute, but it was lost at ${lostAt}. Pass the time column through unchanged or move this step before the operation that loses it.`
        : `${operation} requires an input time attribute. Configure source.timeAttribute first.`;
}

function validateExpression(
    raw: unknown,
    schema: Column[],
    path: string,
    issues: ValidationIssue[]
): ColumnType {
    if (!isRecord(raw) || typeof raw['type'] !== 'string') {
        issues.push(issue(path, 'Expression must have a recognized type.'));
        return 'STRING';
    }
    switch (raw['type']) {
        case 'column':
            return findColumn(schema, raw['name'], `${path}.name`, issues)?.type ?? 'STRING';
        case 'literal':
            if (!isScalar(raw['value']) || (typeof raw['value'] === 'string' && CONTROL_CHARACTERS.test(raw['value']))) {
                issues.push(issue(`${path}.value`, 'Literal must be a string, number, boolean, or null.'));
                return 'STRING';
            }
            return inferLiteralType(raw['value']);
        case 'binary': {
            if (!['+', '-', '*', '/'].includes(String(raw['op']))) {
                issues.push(issue(`${path}.op`, 'Binary operator must be +, -, *, or /.'));
            }
            const left = validateExpression(raw['left'], schema, `${path}.left`, issues);
            const right = validateExpression(raw['right'], schema, `${path}.right`, issues);
            if (!isNumericType(left) || !isNumericType(right)) {
                issues.push(issue(path, 'Arithmetic expressions require numeric operands.'));
            }
            return left === 'DOUBLE' || right === 'DOUBLE' ? 'DOUBLE' : left;
        }
        case 'call': {
            const fn = raw['fn'];
            if (!Array.isArray(raw['args'])) {
                issues.push(issue(`${path}.args`, 'Function arguments must be an array.'));
                return 'STRING';
            }
            const args = raw['args'].map((arg, index) => validateExpression(arg, schema, `${path}.args[${index}]`, issues));
            if (fn === 'CAST') {
                if (args.length !== 1) {
                    issues.push(issue(`${path}.args`, 'CAST requires exactly one argument.'));
                }
                const type = checkType(raw['to'], `${path}.to`, issues);
                return type ?? 'STRING';
            }
            if (!['UPPER', 'LOWER', 'TRIM', 'CONCAT', 'ABS', 'ROUND'].includes(String(fn))) {
                issues.push(issue(`${path}.fn`, 'Unsupported function.'));
            }
            if (['UPPER', 'LOWER', 'TRIM', 'CONCAT'].includes(String(fn)) && args.some(type => type !== 'STRING')) {
                issues.push(issue(`${path}.args`, `${String(fn)} requires STRING arguments.`));
            }
            if (['ABS', 'ROUND'].includes(String(fn)) && args.some(type => !isNumericType(type))) {
                issues.push(issue(`${path}.args`, `${String(fn)} requires numeric arguments.`));
            }
            if (
                (['UPPER', 'LOWER', 'TRIM', 'ABS'].includes(String(fn)) && args.length !== 1)
                || (fn === 'ROUND' && (args.length < 1 || args.length > 2))
                || (fn === 'CONCAT' && args.length < 2)
            ) {
                issues.push(issue(`${path}.args`, `${String(fn)} has an invalid number of arguments.`));
            }
            return ['ABS', 'ROUND'].includes(String(fn)) ? args[0] ?? 'DOUBLE' : 'STRING';
        }
        case 'raw':
            if (typeof raw['text'] !== 'string' || !raw['text'].trim() || CONTROL_CHARACTERS.test(raw['text'])) {
                issues.push(issue(`${path}.text`, 'Raw expression must be non-empty and contain no control characters.'));
            }
            issues.push({
                path,
                severity: 'warning',
                message: 'Unchecked raw expression: review it carefully before submitting.'
            });
            return 'STRING';
        default:
            issues.push(issue(`${path}.type`, 'Unsupported expression type.'));
            return 'STRING';
    }
}

function validateSink(sink: Record<string, unknown>, issues: ValidationIssue[]): void {
    if (sink['type'] === 'print' || sink['type'] === 'blackhole') {
        return;
    }
    if (sink['type'] !== 'filesystem') {
        issues.push(issue('sink.type', 'Sink type must be print, blackhole, or filesystem.'));
        return;
    }
    checkText(sink['path'], 'sink.path', issues);
    if (sink['format'] !== 'json' && sink['format'] !== 'csv') {
        issues.push(issue('sink.format', 'Filesystem sink format must be json or csv.'));
    }
}

function validateSettings(settings: unknown, issues: ValidationIssue[]): void {
    if (settings === undefined) {
        return;
    }
    if (!isRecord(settings)) {
        issues.push(issue('settings', 'Settings must be an object.'));
        return;
    }
    for (const key of ['parallelism', 'checkpointIntervalSeconds', 'stateTtlSeconds', 'idleTimeoutSeconds']) {
        const value = settings[key];
        if (value !== undefined && (!Number.isSafeInteger(value) || Number(value) < 1)) {
            issues.push(issue(`settings.${key}`, `${key} must be a positive integer.`));
        }
    }
}

function validateColumns(raw: unknown[], path: string, issues: ValidationIssue[]): Column[] {
    const columns: Column[] = [];
    raw.forEach((value, index) => {
        const columnPath = `${path}[${index}]`;
        if (!isRecord(value)) {
            issues.push(issue(columnPath, 'Column must be an object.'));
            return;
        }
        const name = checkName(value['name'], `${columnPath}.name`, issues);
        const type = checkType(value['type'], `${columnPath}.type`, issues);
        if (name && type) {
            columns.push({ name, type });
        }
    });
    checkUniqueColumns(columns, path, issues);
    return columns;
}

function checkType(value: unknown, path: string, issues: ValidationIssue[]): ColumnType | undefined {
    if (typeof value !== 'string' || !isAllowedType(value)) {
        issues.push(issue(path, `Type "${String(value)}" is not in the supported column type allow-list.`));
        return undefined;
    }
    return value as ColumnType;
}

function checkName(value: unknown, path: string, issues: ValidationIssue[]): string | undefined {
    if (typeof value !== 'string' || !value.trim() || CONTROL_CHARACTERS.test(value)) {
        issues.push(issue(path, 'Name must be non-empty and contain no control characters.'));
        return undefined;
    }
    return value;
}

function checkText(value: unknown, path: string, issues: ValidationIssue[]): string | undefined {
    if (typeof value !== 'string' || !value.trim() || CONTROL_CHARACTERS.test(value)) {
        issues.push(issue(path, 'Value must be non-empty and contain no control characters.'));
        return undefined;
    }
    return value;
}

function findColumn(
    schema: Column[],
    value: unknown,
    path: string,
    issues: ValidationIssue[]
): Column | undefined {
    if (typeof value !== 'string') {
        issues.push(issue(path, 'Column name must be a string.'));
        return undefined;
    }
    const column = schema.find(item => item.name === value);
    if (!column) {
        issues.push(issue(path, `Column "${value}" does not exist at this step.`));
    }
    return column;
}

function checkUniqueColumns(columns: Column[], path: string, issues: ValidationIssue[]): void {
    const seen = new Set<string>();
    for (const column of columns) {
        const key = column.name.toLocaleLowerCase('en-US');
        if (seen.has(key)) {
            issues.push(issue(path, `Duplicate output column "${column.name}".`));
        }
        seen.add(key);
    }
}

function validateLiteral(value: unknown, type: ColumnType, path: string, issues: ValidationIssue[]): void {
    const valid = type === 'STRING' || type === 'DATE' || type === 'TIMESTAMP(3)'
        ? typeof value === 'string' && !CONTROL_CHARACTERS.test(value)
        : type === 'BOOLEAN'
            ? typeof value === 'boolean'
            : isNumericType(type) && typeof value === 'number' && Number.isFinite(value)
                && ((type !== 'INT' && type !== 'BIGINT') || Number.isInteger(value));
    if (!valid) {
        issues.push(issue(path, `Value does not match column type ${type}.`));
    }
}

function inferLiteralType(value: ScalarValue): ColumnType {
    if (typeof value === 'boolean') {
        return 'BOOLEAN';
    }
    if (typeof value === 'number') {
        return Number.isInteger(value) ? 'BIGINT' : 'DOUBLE';
    }
    return 'STRING';
}

function isAllowedType(value: string): boolean {
    if (ALLOWED_TYPES.has(value)) {
        return true;
    }
    const match = /^DECIMAL\((\d{1,2}),(\d{1,2})\)$/.exec(value);
    if (!match) {
        return false;
    }
    const precision = Number(match[1]);
    const scale = Number(match[2]);
    return precision >= 1 && precision <= 38 && scale <= precision;
}

function isNumericType(type: ColumnType): boolean {
    return type === 'INT' || type === 'BIGINT' || type === 'DOUBLE' || type.startsWith('DECIMAL(');
}

function validName(value: unknown): value is string {
    return typeof value === 'string'
        && IDENTIFIER.test(value)
        && !CONTROL_CHARACTERS.test(value);
}

function positiveInteger(value: unknown): boolean {
    return typeof value === 'number' && Number.isSafeInteger(value) && value > 0;
}

function finiteNumber(value: unknown): boolean {
    return typeof value === 'number' && Number.isFinite(value);
}

function isScalar(value: unknown): value is ScalarValue {
    return value === null || typeof value === 'string' || typeof value === 'boolean'
        || (typeof value === 'number' && Number.isFinite(value));
}

function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function issue(path: string, message: string): ValidationIssue {
    return { path, severity: 'error', message };
}

function sourceSchemaForCompile(spec: JobSpec): Column[] {
    const schema = spec.source.type === 'filesystem' && spec.source.format === 'raw'
        ? [{ name: spec.source.columnName ?? 'line', type: 'STRING' as const }]
        : spec.source.fields.map(field => ({ name: field.name, type: field.type }));
    return spec.source.timeAttribute?.kind === 'processingTime'
        ? [...schema, { name: 'proc_time', type: 'TIMESTAMP(3)' }]
        : schema;
}

function schemaForStep(step: StepSpec, input: Column[]): Column[] {
    switch (step.type) {
        case 'filter':
            return input;
        case 'select':
            return [
                ...step.columns.map(selection => {
                    const source = input.find(column => column.name === selection.name)!;
                    return { name: selection.as ?? source.name, type: source.type };
                }),
                ...(step.computed ?? []).map(computed => ({
                    name: computed.as,
                    type: expressionType(computed.expr, input)
                }))
            ];
        case 'parse':
            return step.kind === 'splitToRows'
                ? [...input, { name: step.as, type: 'STRING' }]
                : [...input, ...step.fields.map(field => ({ name: field.as, type: field.type }))];
        case 'aggregate':
            return [
                ...step.groupBy.map(name => input.find(column => column.name === name)!),
                ...step.aggregates.map(aggregate => ({
                    name: aggregate.as,
                    type: aggregate.fn === 'COUNT'
                        ? 'BIGINT' as const
                        : aggregate.fn === 'AVG'
                            ? 'DOUBLE' as const
                            : input.find(column => column.name === aggregate.column)!.type
                }))
            ];
        case 'windowAggregate':
            return [
                ...(step.includeWindowStart ? [{ name: 'window_start', type: 'TIMESTAMP(3)' as const }] : []),
                ...(step.includeWindowEnd ? [{ name: 'window_end', type: 'TIMESTAMP(3)' as const }] : []),
                ...step.groupBy.map(name => input.find(column => column.name === name)!),
                ...step.aggregates.map(aggregate => ({
                            name: aggregate.as,
                            type: aggregate.fn === 'COUNT'
                                ? 'BIGINT' as const
                                : aggregate.fn === 'AVG'
                                    ? 'DOUBLE' as const
                                    : input.find(column => column.name === aggregate.column)!.type
                }))
            ];
        case 'topN':
            return [
                ...input,
                ...(step.rankColumn ? [{ name: step.rankColumn, type: 'BIGINT' as const }] : [])
            ];
        case 'deduplicate':
            return input;
        default:
            return input;
    }
}

function stepTimeAttribute(
    step: StepSpec,
    input: Column[],
    output: Column[],
    current: string | undefined
): string | undefined {
    if (!current || !input.some(column => column.name === current)) {
        return undefined;
    }
    if (step.type === 'select') {
        const passedThrough = step.columns.some(selection =>
            selection.name === current && (selection.as === undefined || selection.as === current)
        );
        return passedThrough && output.some(column => column.name === current) ? current : undefined;
    }
    if (step.type === 'aggregate' || step.type === 'windowAggregate' || step.type === 'topN') {
        return undefined;
    }
    if (output.some(column => column.name === current)) {
        return current;
    }
    return undefined;
}

function stepWindowContext(
    step: StepSpec,
    input: Column[],
    output: Column[],
    current: boolean
): boolean {
    if (step.type === 'windowAggregate') {
        return true;
    }
    if (step.type === 'aggregate') {
        return false;
    }
    if (step.type === 'topN') {
        return current
            && step.partitionBy.includes('window_start')
            && step.partitionBy.includes('window_end')
            && output.some(column => column.name === 'window_start')
            && output.some(column => column.name === 'window_end');
    }
    return current
        && input.some(column => column.name === 'window_start')
        && input.some(column => column.name === 'window_end')
        && output.some(column => column.name === 'window_start')
        && output.some(column => column.name === 'window_end');
}

function stepChangelogMode(step: StepSpec, current: ChangelogMode): ChangelogMode {
    switch (step.type) {
        case 'aggregate':
        case 'topN':
            return step.type === 'topN'
                && step.partitionBy.includes('window_start')
                && step.partitionBy.includes('window_end')
                ? 'APPEND-ONLY'
                : 'UPDATING';
        case 'windowAggregate':
            return 'APPEND-ONLY';
        case 'deduplicate':
            return 'UPDATING';
        default:
            return current;
    }
}

function expressionType(expression: Expression, input: Column[]): ColumnType {
    switch (expression.type) {
        case 'column':
            return input.find(column => column.name === expression.name)?.type ?? 'STRING';
        case 'literal':
            return inferLiteralType(expression.value);
        case 'binary': {
            const left = expressionType(expression.left, input);
            const right = expressionType(expression.right, input);
            return left === 'DOUBLE' || right === 'DOUBLE' ? 'DOUBLE' : left;
        }
        case 'call':
            if (expression.fn === 'CAST') {
                return expression.to;
            }
            if (expression.fn === 'ABS' || expression.fn === 'ROUND') {
                return expression.args[0] ? expressionType(expression.args[0], input) : 'DOUBLE';
            }
            return 'STRING';
        case 'raw':
            return 'STRING';
    }
}

function compileSource(
    spec: JobSpec,
    schema: Column[],
    preview?: { maxRows: number }
): string {
    const source = spec.source;
    const definitions = schema
        .filter(column => !(source.timeAttribute?.kind === 'processingTime' && column.name === 'proc_time'))
        .map(column => `${quoteIdentifier(column.name)} ${column.type}`);
    if (source.timeAttribute?.kind === 'processingTime') {
        definitions.push(`${quoteIdentifier('proc_time')} AS PROCTIME()`);
    } else if (source.timeAttribute?.kind === 'eventTime') {
        definitions.push(
            `WATERMARK FOR ${quoteIdentifier(source.timeAttribute.column)} AS ${quoteIdentifier(source.timeAttribute.column)} - ${compileDuration(source.timeAttribute.maxOutOfOrderness)}`
        );
    }
    const options: [string, string][] = [];
    if (source.type === 'datagen') {
        options.push(
            ['connector', 'datagen'],
            ['rows-per-second', String(preview ? 1000 : source.rowsPerSecond)]
        );
        if (preview) {
            options.push(['number-of-rows', String(preview.maxRows * 4)]);
        }
        for (const field of source.fields) {
            options.push([`fields.${field.name}.kind`, field.kind]);
            if (field.min !== undefined) {
                options.push([`fields.${field.name}.min`, String(field.min)]);
            }
            if (field.max !== undefined) {
                options.push([`fields.${field.name}.max`, String(field.max)]);
            }
            if (field.length !== undefined) {
                options.push([`fields.${field.name}.length`, String(field.length)]);
            }
        }
    } else {
        options.push(['connector', 'filesystem'], ['path', fileUri(source.path)]);
        if (source.watch && !preview) {
            options.push(['source.monitor-interval', '1 s']);
        }
        options.push(['format', source.format]);
        if (source.format === 'json') {
            if (source.options?.ignoreParseErrors !== undefined) {
                options.push(['json.ignore-parse-errors', String(source.options.ignoreParseErrors)]);
            }
            if (source.options?.timestampFormat !== undefined) {
                options.push(['json.timestamp-format.standard', source.options.timestampFormat]);
            }
        } else if (source.format === 'csv') {
            if (source.options?.delimiter !== undefined) {
                options.push(['csv.field-delimiter', source.options.delimiter]);
            }
            if (source.options?.ignoreParseErrors !== undefined) {
                options.push(['csv.ignore-parse-errors', String(source.options.ignoreParseErrors)]);
            }
        }
    }
    return `CREATE TEMPORARY TABLE ${quoteIdentifier('job_source')} (${definitions.join(', ')}) WITH (${options.map(([key, value]) => `${sqlString(key)} = ${sqlString(value)}`).join(', ')})`;
}

function compileDuration(duration: Duration): string {
    return `INTERVAL '${duration.value}' ${duration.unit}`;
}

function compileStep(step: StepSpec, input: Column[], index: number, inputTimeAttribute?: string): string {
    const sourceName = quoteIdentifier(index ? `job_step_${index}` : 'job_source');
    const viewName = quoteIdentifier(`job_step_${index + 1}`);
    let query: string;
    switch (step.type) {
        case 'filter': {
            const conditions = step.conditions.map(condition => {
                const column = quoteIdentifier(condition.column);
                if (condition.op === 'IS NULL' || condition.op === 'IS NOT NULL') {
                    return `${column} ${condition.op}`;
                }
                if (condition.op === 'IN') {
                    return `${column} IN (${(condition.value as ScalarValue[]).map(sqlValue).join(', ')})`;
                }
                return `${column} ${condition.op} ${sqlValue(condition.value as ScalarValue)}`;
            });
            query = `SELECT ${selectColumnList(input)} FROM ${sourceName} WHERE ${conditions.join(step.mode === 'all' ? ' AND ' : ' OR ')}`;
            break;
        }
        case 'select': {
            const columns = step.columns.map(selection => {
                const inputColumn = input.find(column => column.name === selection.name)!;
                const name = selection.as ?? inputColumn.name;
                return `${quoteIdentifier(inputColumn.name)}${name === inputColumn.name ? '' : ` AS ${quoteIdentifier(name)}`}`;
            });
            for (const computed of step.computed ?? []) {
                columns.push(`${compileExpression(computed.expr)} AS ${quoteIdentifier(computed.as)}`);
            }
            query = `SELECT ${columns.join(', ')} FROM ${sourceName}`;
            break;
        }
        case 'parse':
            if (step.kind === 'jsonValue') {
                const columns = selectColumnList(input);
                const parsed = step.fields.map(field =>
                    `CAST(JSON_VALUE(${quoteIdentifier(step.column)}, ${sqlString(field.path)}) AS ${field.type}) AS ${quoteIdentifier(field.as)}`
                );
                query = `SELECT ${[columns, ...parsed].join(', ')} FROM ${sourceName}`;
            } else {
                query = `SELECT ${selectColumnList(input)}, ${quoteIdentifier(step.as)} FROM ${sourceName} CROSS JOIN UNNEST(SPLIT(${quoteIdentifier(step.column)}, ${sqlString(step.delimiter)})) AS t(${quoteIdentifier(step.as)})`;
            }
            break;
        case 'aggregate': {
            const groupColumns = step.groupBy.map(quoteIdentifier);
            const aggregates = step.aggregates.map(aggregate => {
                const argument = aggregate.fn === 'COUNT' && aggregate.column === undefined
                    ? '*'
                    : quoteIdentifier(aggregate.column ?? '');
                return `${aggregate.fn}(${argument}) AS ${quoteIdentifier(aggregate.as)}`;
            });
            const select = [...groupColumns, ...aggregates].join(', ');
            const groupBy = groupColumns.length ? ` GROUP BY ${groupColumns.join(', ')}` : '';
            query = `SELECT ${select} FROM ${sourceName}${groupBy}`;
            break;
        }
        case 'windowAggregate': {
            if (!inputTimeAttribute) {
                throw new Error('Window aggregation requires a time attribute.');
            }
            const functionCall = compileWindowTableFunction(step.window, sourceName, inputTimeAttribute, step.groupBy);
            const aggregateColumns = compileAggregateColumns(step.aggregates);
            const selectedColumns = [
                ...(step.includeWindowStart ? [quoteIdentifier('window_start')] : []),
                ...(step.includeWindowEnd ? [quoteIdentifier('window_end')] : []),
                ...step.groupBy.map(quoteIdentifier),
                ...aggregateColumns
            ];
            const groupColumns = [
                quoteIdentifier('window_start'),
                quoteIdentifier('window_end'),
                ...step.groupBy.map(quoteIdentifier)
            ];
            query = `SELECT ${selectedColumns.join(', ')} FROM TABLE(${functionCall}) GROUP BY ${groupColumns.join(', ')}`;
            break;
        }
        case 'topN': {
            const helper = uniqueHelperColumn(input, '__flink_row_number');
            const window = [
                ...(step.partitionBy.length ? [`PARTITION BY ${step.partitionBy.map(quoteIdentifier).join(', ')}`] : []),
                `ORDER BY ${quoteIdentifier(step.orderBy.column)} ${step.orderBy.direction}`
            ].join(' ');
            const innerColumns = [
                selectColumnList(input),
                `ROW_NUMBER() OVER (${window}) AS ${quoteIdentifier(helper)}`
            ];
            const outerColumns = [
                selectColumnList(input),
                ...(step.rankColumn ? [`${quoteIdentifier(helper)} AS ${quoteIdentifier(step.rankColumn)}`] : [])
            ];
            query = `SELECT ${outerColumns.join(', ')} FROM (SELECT ${innerColumns.join(', ')} FROM ${sourceName}) WHERE ${quoteIdentifier(helper)} <= ${step.n}`;
            break;
        }
        case 'deduplicate': {
            const helper = uniqueHelperColumn(input, '__flink_row_number');
            const direction = step.keep === 'first' ? 'ASC' : 'DESC';
            const window = `PARTITION BY ${step.keyColumns.map(quoteIdentifier).join(', ')} ORDER BY ${quoteIdentifier(step.orderColumn)} ${direction}`;
            const innerColumns = [
                selectColumnList(input),
                `ROW_NUMBER() OVER (${window}) AS ${quoteIdentifier(helper)}`
            ];
            query = `SELECT ${selectColumnList(input)} FROM (SELECT ${innerColumns.join(', ')} FROM ${sourceName}) WHERE ${quoteIdentifier(helper)} = 1`;
            break;
        }
        default:
            throw new Error('Unsupported step type.');
    }
    return `CREATE TEMPORARY VIEW ${viewName} AS ${query}`;
}

function compileWindowTableFunction(
    window: WindowSpec,
    sourceName: string,
    timeColumn: string,
    groupBy: string[]
): string {
    const descriptor = `DESCRIPTOR(${quoteIdentifier(timeColumn)})`;
    switch (window.kind) {
        case 'tumble':
            return `TUMBLE(TABLE ${sourceName}, ${descriptor}, ${compileDuration(window.size)})`;
        case 'hop':
            return `HOP(TABLE ${sourceName}, ${descriptor}, ${compileDuration(window.slide)}, ${compileDuration(window.size)})`;
        case 'cumulate':
            return `CUMULATE(TABLE ${sourceName}, ${descriptor}, ${compileDuration(window.step)}, ${compileDuration(window.maxSize)})`;
        case 'session':
            return `SESSION(TABLE ${sourceName} PARTITION BY ${groupBy.map(quoteIdentifier).join(', ')}, ${descriptor}, ${compileDuration(window.gap)})`;
    }
}

function compileAggregateColumns(aggregates: AggregateSpec[]): string[] {
    return aggregates.map(aggregate => {
        const argument = aggregate.fn === 'COUNT' && aggregate.column === undefined
            ? '*'
            : quoteIdentifier(aggregate.column ?? '');
        return `${aggregate.fn}(${argument}) AS ${quoteIdentifier(aggregate.as)}`;
    });
}

function uniqueHelperColumn(schema: Column[], base: string): string {
    let helper = base;
    while (schema.some(column => column.name.toLocaleLowerCase('en-US') === helper.toLocaleLowerCase('en-US'))) {
        helper = `_${helper}`;
    }
    return helper;
}

function compileSink(sink: JobSpec['sink'], schema: Column[]): string {
    const definitions = schema.map(column => `${quoteIdentifier(column.name)} ${column.type}`);
    if (sink.type === 'filesystem') {
        return `CREATE TEMPORARY TABLE ${quoteIdentifier('job_sink')} (${definitions.join(', ')}) WITH ('connector' = 'filesystem', 'path' = ${sqlString(fileUri(sink.path))}, 'format' = ${sqlString(sink.format)})`;
    }
    return `CREATE TEMPORARY TABLE ${quoteIdentifier('job_sink')} (${definitions.join(', ')}) WITH ('connector' = ${sqlString(sink.type)})`;
}

function selectColumnList(schema: Column[]): string {
    return schema.map(column => quoteIdentifier(column.name)).join(', ');
}

function compileExpression(expression: Expression): string {
    switch (expression.type) {
        case 'column':
            return quoteIdentifier(expression.name);
        case 'literal':
            return sqlValue(expression.value);
        case 'binary':
            return `(${compileExpression(expression.left)} ${expression.op} ${compileExpression(expression.right)})`;
        case 'call':
            return expression.fn === 'CAST'
                ? `CAST(${compileExpression(expression.args[0])} AS ${expression.to})`
                : `${expression.fn}(${expression.args.map(compileExpression).join(', ')})`;
        case 'raw':
            return compileRawExpression(expression.text);
    }
}

function compileRawExpression(text: string): string {
    const safeFunctions = new Set(['UPPER', 'LOWER', 'TRIM', 'CONCAT', 'ABS', 'ROUND', 'COALESCE', 'NULLIF']);
    const tokens: string[] = [];
    let index = 0;
    while (index < text.length) {
        const rest = text.slice(index);
        const whitespace = /^\s+/.exec(rest);
        if (whitespace) {
            index += whitespace[0].length;
            continue;
        }
        const quotedIdentifier = /^`((?:``|[^`])+)`/.exec(rest);
        if (quotedIdentifier) {
            tokens.push(quoteIdentifier(quotedIdentifier[1].replace(/``/g, '`')));
            index += quotedIdentifier[0].length;
            continue;
        }
        const string = /^'((?:''|[^'])*)'/.exec(rest);
        if (string) {
            tokens.push(sqlString(string[1].replace(/''/g, "'")));
            index += string[0].length;
            continue;
        }
        const number = /^\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/.exec(rest);
        if (number) {
            tokens.push(number[0]);
            index += number[0].length;
            continue;
        }
        const word = /^[A-Za-z_][A-Za-z0-9_]*/.exec(rest);
        if (word) {
            const next = rest.slice(word[0].length).trimStart();
            const keyword = word[0].toUpperCase();
            if (['TRUE', 'FALSE', 'NULL'].includes(keyword)) {
                tokens.push(keyword);
            } else if (next.startsWith('(') && safeFunctions.has(keyword)) {
                tokens.push(keyword);
            } else {
                tokens.push(quoteIdentifier(word[0]));
            }
            index += word[0].length;
            continue;
        }
        const punctuation = /^[()+*/.,-]/.exec(rest);
        if (punctuation) {
            tokens.push(punctuation[0]);
            index += punctuation[0].length;
            continue;
        }
        throw new Error(`Unsupported token in raw expression at character ${index + 1}.`);
    }
    if (!tokens.length) {
        throw new Error('Raw expression must not be empty.');
    }
    return tokens.join(' ');
}

function quoteIdentifier(identifier: string): string {
    return `\`${identifier.replace(/`/g, '``')}\``;
}

function sqlString(value: string): string {
    return `'${value.replace(/'/g, "''")}'`;
}

function sqlValue(value: ScalarValue): string {
    return typeof value === 'string' ? sqlString(value) : value === null ? 'NULL' : String(value);
}

function fileUri(path: string): string {
    if (/^file:\/\//i.test(path)) {
        return path;
    }
    const normalized = path.replace(/\\/g, '/');
    const absolute = /^[A-Za-z]:\//.test(normalized) ? `/${normalized}` : normalized;
    return `file://${absolute.split('/').map((part, index) => {
        if (index === 0 && !part) {
            return '';
        }
        return index === 1 && /^[A-Za-z]:$/.test(part) ? part : encodeURIComponent(part);
    }).join('/')}`;
}
