import { ColumnType, Duration } from './spec';

export type SampleFormat = 'json' | 'csv';

export interface InferredField {
    name: string;
    type: ColumnType;
    confidence?: string;
}

export interface SampleInference {
    format: SampleFormat;
    fields: InferredField[];
    hasHeader?: boolean;
    delimiter?: string;
    timestampFormat?: 'SQL' | 'ISO-8601';
    eventTimeSuggestion?: { column: string; lateness: Duration };
    truncated: boolean;
    sampledBytes: number;
    sampledRows: number;
}

interface LimitedSample {
    text: string;
    sampledBytes: number;
    truncated: boolean;
}

interface ObservedValue {
    type: ColumnType | undefined;
    kind: string;
    timestampFormat?: 'SQL' | 'ISO-8601';
}

interface InferredSampleData {
    fields: InferredField[];
    hasHeader?: boolean;
    delimiter?: string;
    timestampFormat?: 'SQL' | 'ISO-8601';
    sampledRows: number;
}

const DEFAULT_MAX_BYTES = 64 * 1024;
const ISO_TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$/;
const SQL_TIMESTAMP = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/;
const DATE = /^\d{4}-\d{2}-\d{2}$/;

export function inferSample(
    input: string,
    format: SampleFormat,
    maxBytes = DEFAULT_MAX_BYTES,
    inputTruncated = false
): SampleInference {
    if (!Number.isInteger(maxBytes) || maxBytes < 1) {
        throw new RangeError('maxBytes must be a positive integer.');
    }
    const limited = limitSample(input, maxBytes);
    const lines = limited.text.replace(/^\uFEFF/, '').split(/\r?\n/);
    while (lines.length && !lines[lines.length - 1].trim()) {
        lines.pop();
    }
    if (!lines.length) {
        throw new Error('The sample file is empty.');
    }

    const inferred: InferredSampleData = format === 'json'
        ? inferJsonLines(lines)
        : inferCsv(lines);
    const timestampFields = inferred.fields.filter(field => field.type === 'TIMESTAMP(3)');
    const result: SampleInference = {
        format,
        fields: inferred.fields,
        ...(inferred.hasHeader === undefined ? {} : { hasHeader: inferred.hasHeader }),
        ...(inferred.delimiter === undefined ? {} : { delimiter: inferred.delimiter }),
        ...(timestampFields.length === 1 && inferred.timestampFormat
            ? { timestampFormat: inferred.timestampFormat }
            : {}),
        ...(timestampFields.length === 1
            ? { eventTimeSuggestion: { column: timestampFields[0].name, lateness: { value: 2, unit: 'SECOND' } } }
            : {}),
        truncated: limited.truncated || inputTruncated,
        sampledBytes: limited.sampledBytes,
        sampledRows: inferred.sampledRows
    };
    return result;
}

function limitSample(input: string, maxBytes: number): LimitedSample {
    const bytes = Buffer.from(input, 'utf8');
    if (bytes.length <= maxBytes) {
        return { text: input, sampledBytes: bytes.length, truncated: false };
    }
    let text = bytes.subarray(0, maxBytes).toString('utf8');
    const lastNewline = text.lastIndexOf('\n');
    text = lastNewline < 0 ? '' : text.slice(0, lastNewline + 1);
    return { text, sampledBytes: maxBytes, truncated: true };
}

function inferJsonLines(lines: string[]): {
    fields: InferredField[];
    timestampFormat?: 'SQL' | 'ISO-8601';
    sampledRows: number;
} {
    const objects: Array<Record<string, unknown>> = [];
    for (let index = 0; index < lines.length; index++) {
        const line = lines[index].trim();
        if (!line) {
            continue;
        }
        let value: unknown;
        try {
            value = JSON.parse(line) as unknown;
        } catch {
            throw new Error(`Line ${index + 1} is not valid JSON.`);
        }
        if (!isRecord(value)) {
            throw new Error(`Line ${index + 1} must contain a JSON object.`);
        }
        objects.push(value);
    }
    if (!objects.length) {
        throw new Error('The sample file contains no JSON objects.');
    }

    const keys: string[] = [];
    const seenKeys = new Set<string>();
    for (const object of objects) {
        for (const key of Object.keys(object)) {
            if (!seenKeys.has(key)) {
                seenKeys.add(key);
                keys.push(key);
            }
        }
    }
    const fields = keys.map(key => inferField(
        sanitizeName(key, `col_${keys.indexOf(key) + 1}`),
        objects.map(object => object[key]),
        objects.length
    ));
    const timestampFormats = new Set(
        fields.flatMap(field => field.type === 'TIMESTAMP(3)' ? [timestampFormatForName(field.name, objects, keys)] : [])
    );
    const timestampFormat = timestampFormats.size === 1
        ? [...timestampFormats][0]
        : undefined;
    return {
        fields: uniqueNames(fields),
        ...(timestampFormat ? { timestampFormat } : {}),
        sampledRows: objects.length
    };
}

function timestampFormatForName(
    sanitizedName: string,
    objects: Array<Record<string, unknown>>,
    keys: string[]
): 'SQL' | 'ISO-8601' | undefined {
    const index = keys.findIndex(key => sanitizeName(key, `col_${keys.indexOf(key) + 1}`) === sanitizedName);
    if (index < 0) {
        return undefined;
    }
    const originalName = keys[index];
    const formats = new Set<'SQL' | 'ISO-8601'>();
    for (const object of objects) {
        const observed = observe(object[originalName]);
        if (observed.timestampFormat) {
            formats.add(observed.timestampFormat);
        }
    }
    return formats.size === 1 ? [...formats][0] : undefined;
}

function inferCsv(lines: string[]): {
    fields: InferredField[];
    hasHeader: boolean;
    delimiter: string;
    timestampFormat?: 'SQL' | 'ISO-8601';
    sampledRows: number;
} {
    const delimiter = detectDelimiter(lines);
    const records = lines
        .filter(line => line.trim())
        .map((line, index) => parseCsvLine(line, delimiter, index + 1));
    if (!records.length || !records[0].length) {
        throw new Error('The sample file contains no CSV rows.');
    }
    const width = Math.max(...records.map(record => record.length));
    const hasHeader = detectHeader(records, width);
    const header = hasHeader ? records[0] : undefined;
    const dataRows = records.slice(hasHeader ? 1 : 0);
    if (!dataRows.length) {
        throw new Error('The CSV sample contains a header but no data rows.');
    }
    const fields = Array.from({ length: width }, (_unused, columnIndex) => {
        const fallbackName = `col_${columnIndex + 1}`;
        const name = header
            ? sanitizeName(header[columnIndex] ?? '', fallbackName)
            : fallbackName;
        return inferField(
            name,
            dataRows.map(row => row[columnIndex] ?? ''),
            dataRows.length,
            true
        );
    });
    const timestampFields = fields.filter(field => field.type === 'TIMESTAMP(3)');
    const timestampFormat = timestampFields.length === 1
        ? commonTimestampFormat(dataRows.map(row => row[fields.indexOf(timestampFields[0])] ?? ''))
        : undefined;
    return {
        fields: uniqueNames(fields),
        hasHeader,
        delimiter,
        ...(timestampFormat ? { timestampFormat } : {}),
        sampledRows: dataRows.length
    };
}

function inferField(
    name: string,
    values: unknown[],
    rowCount: number,
    csv = false
): InferredField {
    const observed = values
        .filter(value => value !== undefined && value !== null && !(csv && value === ''))
        .map(value => observe(value, csv));
    if (!observed.length) {
        return { name, type: 'STRING', confidence: 'No non-empty sample values; STRING is a placeholder.' };
    }
    const kinds = new Set(observed.map(value => value.kind));
    const types = new Set(observed.flatMap(value => value.type ? [value.type] : []));
    let type: ColumnType;
    if (kinds.size === 1) {
        type = observed[0].type ?? 'STRING';
    } else if (kinds.size === 2 && kinds.has('integer') && kinds.has('fractional')) {
        type = 'DOUBLE';
    } else {
        type = 'STRING';
    }
    const notes: string[] = [];
    if (kinds.size > 1) {
        notes.push(`Mixed sample values widened to ${type}.`);
    }
    const missing = rowCount - observed.length;
    if (missing > 0) {
        notes.push(`Missing in ${missing} sampled row${missing === 1 ? '' : 's'}; inferred from present values.`);
    }
    if (type === 'TIMESTAMP(3)' && new Set(observed.map(value => value.timestampFormat)).size > 1) {
        type = 'STRING';
        notes.push('Mixed timestamp formats widened to STRING.');
    }
    return {
        name,
        type,
        ...(notes.length ? { confidence: notes.join(' ') } : {})
    };
}

function observe(value: unknown, csv = false): ObservedValue {
    if (csv && typeof value === 'string') {
        const text = value.trim();
        if (!text) {
            return { type: undefined, kind: 'null' };
        }
        if (/^[+-]?\d+$/.test(text)) {
            return { type: 'BIGINT', kind: 'integer' };
        }
        if (/^[+-]?(?:\d+\.\d*|\d*\.\d+)(?:[eE][+-]?\d+)?$/.test(text)) {
            return { type: 'DOUBLE', kind: 'fractional' };
        }
        if (/^(true|false)$/i.test(text)) {
            return { type: 'BOOLEAN', kind: 'boolean' };
        }
        return observeString(text);
    }
    if (typeof value === 'number') {
        return Number.isInteger(value)
            ? { type: 'BIGINT', kind: 'integer' }
            : { type: 'DOUBLE', kind: 'fractional' };
    }
    if (typeof value === 'boolean') {
        return { type: 'BOOLEAN', kind: 'boolean' };
    }
    if (typeof value === 'string') {
        return observeString(value);
    }
    if (isRecord(value) || Array.isArray(value)) {
        return { type: 'STRING', kind: 'nested' };
    }
    return { type: undefined, kind: 'null' };
}

function observeString(value: string): ObservedValue {
    if (ISO_TIMESTAMP.test(value)) {
        return isValidDateTime(value)
            ? { type: 'TIMESTAMP(3)', kind: 'iso-timestamp', timestampFormat: 'ISO-8601' }
            : { type: 'STRING', kind: 'string' };
    }
    if (SQL_TIMESTAMP.test(value)) {
        return isValidDateTime(value.replace(' ', 'T'))
            ? { type: 'TIMESTAMP(3)', kind: 'sql-timestamp', timestampFormat: 'SQL' }
            : { type: 'STRING', kind: 'string' };
    }
    if (DATE.test(value)) {
        return isValidDateTime(`${value}T00:00:00`)
            ? { type: 'DATE', kind: 'date' }
            : { type: 'STRING', kind: 'string' };
    }
    return { type: 'STRING', kind: 'string' };
}

function commonTimestampFormat(values: string[]): 'SQL' | 'ISO-8601' | undefined {
    const formats = new Set(
        values.map(value => observeString(value).timestampFormat).filter(
            (format): format is 'SQL' | 'ISO-8601' => format !== undefined
        )
    );
    return formats.size === 1 ? [...formats][0] : undefined;
}

function detectDelimiter(lines: string[]): string {
    const candidates = [',', ';', '\t', '|'];
    const sample = lines.filter(line => line.trim()).slice(0, 20);
    const scores = candidates.map(delimiter => {
        const counts = sample.map(line => countDelimiter(line, delimiter));
        const positive = counts.filter(count => count > 0);
        if (!positive.length) {
            return { delimiter, score: 0 };
        }
        const consistency = positive.filter(count => count === positive[0]).length / positive.length;
        return { delimiter, score: positive[0] * consistency };
    });
    return scores.sort((left, right) => right.score - left.score)[0].delimiter;
}

function countDelimiter(line: string, delimiter: string): number {
    let count = 0;
    let quoted = false;
    for (let index = 0; index < line.length; index++) {
        if (line[index] === '"') {
            if (quoted && line[index + 1] === '"') {
                index++;
            } else {
                quoted = !quoted;
            }
        } else if (line[index] === delimiter && !quoted) {
            count++;
        }
    }
    return count;
}

function parseCsvLine(line: string, delimiter: string, lineNumber: number): string[] {
    const cells: string[] = [];
    let cell = '';
    let quoted = false;
    for (let index = 0; index < line.length; index++) {
        const character = line[index];
        if (character === '"') {
            if (quoted && line[index + 1] === '"') {
                cell += '"';
                index++;
            } else {
                quoted = !quoted;
            }
        } else if (character === delimiter && !quoted) {
            cells.push(cell.trim());
            cell = '';
        } else {
            cell += character;
        }
    }
    if (quoted) {
        throw new Error(`Line ${lineNumber} has an unterminated CSV quote.`);
    }
    cells.push(cell.trim());
    return cells;
}

function detectHeader(records: string[][], width: number): boolean {
    if (records.length < 2) {
        return false;
    }
    const first = records[0];
    if (first.length !== width || new Set(first.map(value => value.toLocaleLowerCase('en-US'))).size !== width) {
        return false;
    }
    const knownLabels = new Set([
        'id', 'user', 'name', 'amount', 'price', 'count', 'date', 'time',
        'timestamp', 'event_time', 'event-time', 'email', 'city', 'country'
    ]);
    if (first.some(value => knownLabels.has(value.trim().toLocaleLowerCase('en-US')))) {
        return true;
    }
    return first.some((value, column) => {
        const firstValue = observe(value, true);
        return firstValue.type === 'STRING'
            && records.slice(1).some(row => {
                const later = observe(row[column] ?? '', true);
                return later.type !== undefined && later.type !== 'STRING';
            });
    });
}

function sanitizeName(value: string, fallback: string): string {
    let name = value.trim().replace(/[^A-Za-z0-9_]/g, '_');
    if (!name) {
        name = fallback;
    }
    if (!/^[A-Za-z_]/.test(name)) {
        name = `_${name}`;
    }
    return name;
}

function uniqueNames(fields: InferredField[]): InferredField[] {
    const names = new Set<string>();
    return fields.map(field => {
        const base = field.name;
        let name = base;
        let suffix = 2;
        while (names.has(name.toLocaleLowerCase('en-US'))) {
            name = `${base}_${suffix++}`;
        }
        names.add(name.toLocaleLowerCase('en-US'));
        return { ...field, name };
    });
}

function isValidDateTime(value: string): boolean {
    const date = new Date(`${value}Z`);
    return Number.isFinite(date.getTime()) && date.toISOString().slice(0, 19) === value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}
