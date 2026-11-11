import { applyChangelog, PreviewRow } from '../preview.js';

interface Column {
    name: string;
    type: string;
}

interface JobSpec {
    version: 1;
    name: string;
    source: Record<string, unknown>;
    steps: Array<Record<string, unknown>>;
    sink: Record<string, unknown>;
    settings: Record<string, unknown>;
}

interface ValidationIssue {
    path: string;
    severity: 'error' | 'warning';
    message: string;
}

interface PipelineMetadata {
    schema: Column[];
    timeAttribute?: string;
    lostTimeAttributeAt?: string;
    changelogMode: 'APPEND-ONLY' | 'UPDATING';
}

interface PreviewColumn {
    name: string;
    type: string;
}

interface PreviewResult {
    target: 'source' | number;
    columns: PreviewColumn[];
    rows: Array<Record<string, unknown> & { fields?: unknown[]; kind?: string; rowKind?: string }>;
    rowCount: number;
    truncated: boolean;
    durationMs: number;
    changelogMode: 'APPEND-ONLY' | 'UPDATING';
    keyColumns?: string[];
    error?: string;
}

interface InferenceField {
    name: string;
    type: string;
    confidence?: string;
}

interface InferenceResult {
    format: 'json' | 'csv';
    fields: InferenceField[];
    delimiter?: string;
    timestampFormat?: 'SQL' | 'ISO-8601';
    eventTimeSuggestion?: { column: string; lateness: { value: number; unit: string } };
    truncated: boolean;
    sampledBytes: number;
    sampledRows: number;
}

type HostMessage =
    | { type: 'initialize'; spec: JobSpec; readOnly: boolean }
    | {
        type: 'update';
        issues: ValidationIssue[];
        sql: string[];
        sourceSchema?: Column[];
        schemaByStep?: Column[][];
        sourceMetadata?: PipelineMetadata;
        pipelineByStep?: PipelineMetadata[];
        changelogMode?: 'APPEND-ONLY' | 'UPDATING';
        schemaError?: string;
    }
    | { type: 'result'; action: 'validate' | 'submit'; message: string; jobID?: string }
    | { type: 'previewStarted'; target: 'source' | number }
    | { type: 'previewFinished'; target: 'source' | number }
    | { type: 'previewResult'; target: 'source' | number; columns: PreviewColumn[]; rows: PreviewResult['rows']; rowCount: number; truncated: boolean; durationMs: number; changelogMode: PreviewResult['changelogMode']; keyColumns?: string[]; error?: string }
    | { type: 'previewWarning'; target: 'source' | number; message: string }
    | { type: 'inference'; inference: InferenceResult; fileName: string }
    | { type: 'inferenceError'; message: string };

type WebviewMessage =
    | { type: 'ready' }
    | { type: 'change'; spec: JobSpec }
    | { type: 'validate' | 'submit' | 'save' | 'copySql'; spec: JobSpec }
    | { type: 'preview'; spec: JobSpec; target: 'source' | number }
    | { type: 'cancelPreview' }
    | { type: 'inferSample' }
    | { type: 'load' }
    | { type: 'openJob'; jobID: string };

declare function acquireVsCodeApi(): { postMessage(message: WebviewMessage): void };

const vscode = acquireVsCodeApi();
const root = document.querySelector<HTMLElement>('#app');
if (!root) {
    throw new Error('Job designer root element was not found.');
}

let spec: JobSpec;
let issues: ValidationIssue[] = [];
let sql: string[] = [];
let sourceSchema: Column[] = [];
let schemaByStep: Column[][] = [];
let sourceMetadata: PipelineMetadata | undefined;
let pipelineByStep: PipelineMetadata[] = [];
let changelogMode: 'APPEND-ONLY' | 'UPDATING' = 'APPEND-ONLY';
let schemaError = '';
let readOnly = false;
let resultMessage = '';
let resultJobId: string | undefined;
let previewRunning = false;
let previewResult: PreviewResult | undefined;
let previewWarning = '';
let showFinalState = false;
let inferenceResult: InferenceResult | undefined;
let inferenceFileName = '';
let inferenceFieldsJson = '';
let inferenceError = '';
let inferenceApplied = false;
let inferenceTimestampFormat: 'SQL' | 'ISO-8601' | undefined;
let activeBuilderTab = 'source';

window.addEventListener('message', event => {
    const message = event.data as HostMessage;
    switch (message.type) {
        case 'initialize':
            spec = message.spec;
            readOnly = message.readOnly;
            break;
        case 'update':
            issues = message.issues;
            sql = message.sql;
            sourceSchema = message.sourceSchema ?? [];
            schemaByStep = message.schemaByStep ?? [];
            sourceMetadata = message.sourceMetadata;
            pipelineByStep = message.pipelineByStep ?? [];
            changelogMode = message.changelogMode ?? pipelineByStep.at(-1)?.changelogMode ?? 'APPEND-ONLY';
            schemaError = message.schemaError ?? '';
            break;
        case 'result':
            resultMessage = message.message;
            resultJobId = message.jobID;
            break;
        case 'previewStarted':
            previewRunning = true;
            previewResult = undefined;
            previewWarning = '';
            showFinalState = false;
            activeBuilderTab = 'preview';
            break;
        case 'previewFinished':
            previewRunning = false;
            break;
        case 'previewResult':
            previewResult = message;
            showFinalState = false;
            break;
        case 'previewWarning':
            previewWarning = message.message;
            break;
        case 'inference':
            inferenceResult = message.inference;
            inferenceFileName = message.fileName;
            inferenceFieldsJson = JSON.stringify(
                message.inference.fields.map(({ name, type }) => ({ name, type })),
                null,
                2
            );
            inferenceError = '';
            inferenceApplied = false;
            inferenceTimestampFormat = message.inference.timestampFormat;
            break;
        case 'inferenceError':
            inferenceError = message.message;
            break;
    }
    render();
});
post({ type: 'ready' });

function render(): void {
    if (!spec || !root) {
        return;
    }
    root.replaceChildren();
    const builder = element('section', 'builder');
    builder.append(builderTabs());
    if (activeBuilderTab === 'source' || activeBuilderTab.startsWith('step:')) {
        builder.append(addStepControl());
    }
    builder.append(builderTabPanel());
    root.append(builder);
}

function builderTabs(): HTMLElement {
    const tabs = [
        { key: 'source', label: '1 · Source' },
        ...spec.steps.map((step, index) => ({
            key: `step:${index}`,
            label: `${index + 2} · ${stepTypeLabel(step['type'])}`
        })),
        { key: 'sink', label: `${spec.steps.length + 2} · Sink` },
        { key: 'run', label: `${spec.steps.length + 3} · Run` },
        { key: 'sql', label: 'Generated SQL' },
        { key: 'schema', label: 'Schema at this point' },
        { key: 'preview', label: 'Preview results' }
    ];
    if (!tabs.some(tab => tab.key === activeBuilderTab)) {
        activeBuilderTab = 'source';
    }

    const tabList = element('div', 'builder-tabs');
    tabList.setAttribute('role', 'tablist');
    tabList.setAttribute('aria-label', 'Job designer sections');
    for (const [index, tab] of tabs.entries()) {
        const control = element('button', 'builder-tab');
        control.type = 'button';
        control.id = tabId(tab.key);
        control.setAttribute('role', 'tab');
        control.setAttribute('aria-controls', 'job-stage-panel');
        control.setAttribute('aria-selected', String(tab.key === activeBuilderTab));
        control.tabIndex = tab.key === activeBuilderTab ? 0 : -1;
        control.textContent = tab.label;
        control.addEventListener('click', () => {
            activeBuilderTab = tab.key;
            render();
        });
        control.addEventListener('keydown', event => {
            let nextIndex: number | undefined;
            if (event.key === 'ArrowRight' || event.key === 'ArrowDown') {
                nextIndex = (index + 1) % tabs.length;
            } else if (event.key === 'ArrowLeft' || event.key === 'ArrowUp') {
                nextIndex = (index - 1 + tabs.length) % tabs.length;
            } else if (event.key === 'Home') {
                nextIndex = 0;
            } else if (event.key === 'End') {
                nextIndex = tabs.length - 1;
            }
            if (nextIndex === undefined) {
                return;
            }
            event.preventDefault();
            activeBuilderTab = tabs[nextIndex].key;
            render();
            root?.querySelector<HTMLButtonElement>('.builder-tab[aria-selected="true"]')?.focus();
        });
        tabList.append(control);
    }
    return tabList;
}

function addStepControl(): HTMLElement {
    const wrapper = element('div', 'add-step-control');
    const addStep = selectInput(
        ['filter', 'select', 'parse', 'aggregate', 'windowAggregate', 'topN', 'deduplicate'],
        '',
        value => {
            if (value) {
                spec.steps.push(defaultStep(value));
                activeBuilderTab = `step:${spec.steps.length - 1}`;
                changed();
            }
        },
        'Add a step...'
    );
    addStep.setAttribute('aria-label', 'Add a step');
    wrapper.append(addStep);
    return wrapper;
}

function builderTabPanel(): HTMLElement {
    const panel = element('section', 'builder-tab-panel');
    panel.id = 'job-stage-panel';
    panel.setAttribute('role', 'tabpanel');
    panel.setAttribute('aria-labelledby', tabId(activeBuilderTab));
    panel.tabIndex = 0;

    if (activeBuilderTab === 'source') {
        panel.append(heading('Source'), jobNameForm(), sourceForm());
    } else if (activeBuilderTab === 'sink') {
        panel.append(heading('Sink'), sinkForm());
    } else if (activeBuilderTab === 'run') {
        panel.append(heading('Run'), settingsForm(), actionButtons());
    } else if (activeBuilderTab === 'sql') {
        panel.append(
            heading('Generated SQL'),
            textBlock(sql.length ? sql.join(';\n\n') : 'Complete a valid source and steps to preview SQL.', 'sql'),
            issuesView()
        );
        if (resultMessage) {
            const status = element('p', 'result');
            status.textContent = resultMessage;
            panel.append(status);
        }
        if (resultJobId) {
            panel.append(button(`Open job ${resultJobId}`, () => post({ type: 'openJob', jobID: resultJobId! })));
        }
    } else if (activeBuilderTab === 'schema') {
        panel.append(heading('Schema at this point'), schemaTable());
    } else if (activeBuilderTab === 'preview') {
        panel.append(previewPanel());
    } else {
        const index = Number(activeBuilderTab.slice('step:'.length));
        const step = spec.steps[index];
        if (!step) {
            activeBuilderTab = 'source';
            return builderTabPanel();
        }
        const card = element('div', 'card');
        const controls = element('div', 'step-header');
        controls.append(
            heading(`Step ${index + 1}: ${stepTypeLabel(step['type'])}`),
            selectInput(
                ['filter', 'select', 'parse', 'aggregate', 'windowAggregate', 'topN', 'deduplicate'],
                String(step['type']),
                value => {
                    spec.steps[index] = defaultStep(value);
                    changed();
                }
            ),
            button('Up', () => moveStep(index, -1), index === 0),
            button('Down', () => moveStep(index, 1), index === spec.steps.length - 1),
            button('Delete step', () => deleteStep(index)),
            button('Preview step', () => post({ type: 'preview', spec, target: index }))
        );
        card.append(controls, stepForm(step, index));
        panel.append(card);
    }
    return panel;
}

function tabId(key: string): string {
    return `job-stage-tab-${key.replace(':', '-')}`;
}

function stepTypeLabel(value: unknown): string {
    const labels: Record<string, string> = {
        filter: 'Filter',
        select: 'Select',
        parse: 'Parse',
        aggregate: 'Aggregate',
        windowAggregate: 'Window aggregate',
        topN: 'Top-N',
        deduplicate: 'Deduplicate'
    };
    return labels[String(value)] ?? 'Step';
}

function jobNameForm(): HTMLElement {
    const wrapper = element('div', 'field');
    const input = textInput(spec.name, value => {
        spec.name = value;
        changed();
    });
    wrapper.append(labelled('Job name', input, 'name'));
    return wrapper;
}

function sourceForm(): HTMLElement {
    const wrapper = element('div', 'card');
    wrapper.append(labelled('Source type', selectInput(
        ['datagen', 'filesystem'],
        String(spec.source['type']),
        value => setSource(value)
    ), 'source.type'));

    if (spec.source['type'] === 'datagen') {
        wrapper.append(
            labelled('Rows per second', numberInput(spec.source['rowsPerSecond'], value => {
                spec.source['rowsPerSecond'] = value;
                changed();
            }), 'source.rowsPerSecond'),
            jsonField('Fields (JSON)', spec.source['fields'], value => {
                spec.source['fields'] = value;
                changed();
            }, 'source.fields')
        );
    } else {
        wrapper.append(
            labelled('Path', textInput(String(spec.source['path'] ?? ''), value => {
                spec.source['path'] = value;
                changed();
            }), 'source.path'),
            labelled('Format', selectInput(['raw', 'json', 'csv'], String(spec.source['format']), value => setSourceFormat(value)), 'source.format'),
            labelled('Watch directory', checkboxInput(Boolean(spec.source['watch']), value => {
                spec.source['watch'] = value;
                changed();
            }), 'source.watch')
        );
        if (spec.source['format'] === 'raw') {
            wrapper.append(labelled('Column name', textInput(String(spec.source['columnName'] ?? 'line'), value => {
                spec.source['columnName'] = value;
                changed();
            }), 'source.columnName'));
        } else {
            wrapper.append(jsonField('Fields (JSON)', spec.source['fields'], value => {
                spec.source['fields'] = value;
                changed();
            }, 'source.fields'));
            wrapper.append(jsonField('Options (JSON)', spec.source['options'] ?? {}, value => {
                spec.source['options'] = value;
                changed();
            }, 'source.options'));
        }
    }
    wrapper.append(sourceTimeAttributeForm());
    wrapper.append(button('Preview source', () => post({ type: 'preview', spec, target: 'source' })));
    if (spec.source['type'] === 'filesystem' && spec.source['format'] !== 'raw') {
        wrapper.append(button('Infer from sample file...', () => post({ type: 'inferSample' })));
    }
    if (inferenceError) {
        const error = element('p', 'error');
        error.textContent = inferenceError;
        wrapper.append(error);
    }
    if (inferenceResult) {
        wrapper.append(inferenceReviewForm());
    }
    return wrapper;
}

function sourceTimeAttributeForm(): HTMLElement {
    const wrapper = element('div', 'fields');
    const current = isRecord(spec.source['timeAttribute'])
        ? spec.source['timeAttribute'] as Record<string, unknown>
        : undefined;
    const kind = String(current?.['kind'] ?? 'none');
    wrapper.append(labelled('Time attribute', selectInput(
        ['none', 'processingTime', 'eventTime'],
        kind,
        value => {
            if (value === 'none') {
                delete spec.source['timeAttribute'];
            } else if (value === 'processingTime') {
                spec.source['timeAttribute'] = { kind: value };
            } else {
                const timestampColumns = sourceColumns().filter(column => column.type === 'TIMESTAMP(3)');
                const existing = isRecord(spec.source['timeAttribute'])
                    ? spec.source['timeAttribute'] as Record<string, unknown>
                    : undefined;
                spec.source['timeAttribute'] = {
                    kind: 'eventTime',
                    column: timestampColumns.some(column => column.name === existing?.['column'])
                        ? existing?.['column']
                        : timestampColumns[0]?.name ?? '',
                    maxOutOfOrderness: isRecord(existing?.['maxOutOfOrderness'])
                        ? existing?.['maxOutOfOrderness']
                        : { value: 2, unit: 'SECOND' }
                };
            }
            changed();
        }
    ), 'source.timeAttribute'));
    if (kind === 'eventTime') {
        const timestampColumns = sourceColumns().filter(column => column.type === 'TIMESTAMP(3)');
        wrapper.append(
            labelled('Event-time column', selectInput(
                timestampColumns.map(column => column.name),
                String(current?.['column'] ?? ''),
                value => {
                    current!['column'] = value;
                    changed();
                },
                'Choose a TIMESTAMP(3) column'
            ), 'source.timeAttribute.column'),
            durationInput('Maximum out-of-orderness', current?.['maxOutOfOrderness'], duration => {
                current!['maxOutOfOrderness'] = duration;
                changed();
            }, 'source.timeAttribute.maxOutOfOrderness')
        );
        const hint = element('p', 'muted');
        hint.textContent = 'Windows close only when newer data advances the watermark. For a watched folder, set an idle timeout so quiet sources do not hold windows open.';
        wrapper.append(hint);
    }
    return wrapper;
}

function sourceColumns(): Column[] {
    if (spec.source['type'] === 'filesystem' && spec.source['format'] === 'raw') {
        return [{ name: String(spec.source['columnName'] ?? 'line'), type: 'STRING' }];
    }
    if (!Array.isArray(spec.source['fields'])) {
        return [];
    }
    return (spec.source['fields'] as Array<Record<string, unknown>>)
        .filter(field => typeof field['name'] === 'string' && typeof field['type'] === 'string')
        .map(field => ({ name: String(field['name']), type: String(field['type']) }));
}

function inferenceReviewForm(): HTMLElement {
    const result = inferenceResult!;
    const card = element('section', 'card inference-review');
    const title = element('p', 'muted');
    title.textContent = `Review inferred fields from ${inferenceFileName} (${result.sampledRows} sampled rows${result.truncated ? ', sample truncated' : ''}).`;
    card.append(title);
    for (const field of result.fields) {
        const note = element('p', 'muted');
        note.textContent = `${field.name}: ${field.type}${field.confidence ? ` — ${field.confidence}` : ''}`;
        card.append(note);
    }
    const fields = element('textarea');
    fields.value = inferenceFieldsJson;
    fields.setAttribute('aria-label', 'Edit inferred field definitions as JSON');
    fields.addEventListener('input', () => {
        inferenceFieldsJson = fields.value;
    });
    card.append(labelled('Edit fields before applying', fields));
    if (result.timestampFormat) {
        card.append(labelled('Timestamp format', selectInput(
            ['SQL', 'ISO-8601'],
            inferenceTimestampFormat ?? result.timestampFormat,
            value => {
                inferenceTimestampFormat = value as 'SQL' | 'ISO-8601';
            }
        )));
    }
    const actions = element('div', 'actions');
    actions.append(button(
        inferenceApplied ? 'Fields applied' : 'Apply inferred fields',
        applyInferredFields,
        inferenceApplied
    ));
    if (result.eventTimeSuggestion) {
        const suggestion = result.eventTimeSuggestion;
        const available = sourceColumns().some(column =>
            column.name === suggestion.column && column.type === 'TIMESTAMP(3)'
        );
        actions.append(
            button(
                `Use ${suggestion.column} as event time (2 second lateness)`,
                () => {
                    spec.source['timeAttribute'] = {
                        kind: 'eventTime',
                        column: suggestion.column,
                        maxOutOfOrderness: suggestion.lateness
                    };
                    changed();
                },
                !available
            )
        );
        if (!available) {
            const hint = element('p', 'muted');
            hint.textContent = 'Apply the inferred TIMESTAMP(3) field first; the event-time suggestion is never applied automatically.';
            card.append(hint);
        }
    }
    card.append(actions);
    return card;
}

function applyInferredFields(): void {
    if (!inferenceResult) {
        return;
    }
    let fields: unknown;
    try {
        fields = JSON.parse(inferenceFieldsJson) as unknown;
    } catch {
        inferenceError = 'Enter a valid JSON field list before applying the inference.';
        render();
        return;
    }
    if (!Array.isArray(fields)) {
        inferenceError = 'Inferred fields must be a JSON array.';
        render();
        return;
    }
    const source = spec.source['type'] === 'filesystem'
        ? spec.source
        : { type: 'filesystem', path: '', watch: false };
    const next: Record<string, unknown> = {
        type: 'filesystem',
        path: String(source['path'] ?? ''),
        watch: Boolean(source['watch']),
        format: inferenceResult.format,
        fields
    };
    if (inferenceResult.format === 'json') {
        const oldOptions = isRecord(source['options']) ? source['options'] : {};
        next['options'] = {
            ...(oldOptions['ignoreParseErrors'] === undefined ? {} : { ignoreParseErrors: oldOptions['ignoreParseErrors'] }),
            ...(inferenceTimestampFormat ? { timestampFormat: inferenceTimestampFormat } : {})
        };
    } else {
        next['options'] = { delimiter: inferenceResult.delimiter ?? ',' };
    }
    spec.source = next;
    inferenceApplied = true;
    inferenceError = '';
    changed();
}

function stepForm(step: Record<string, unknown>, index: number): HTMLElement {
    const wrapper = element('div', 'fields');
    const columns = schemaAt(index);
    if (step['type'] === 'filter') {
        wrapper.append(labelled('Match', selectInput(['all', 'any'], String(step['mode']), value => {
            step['mode'] = value;
            changed();
        }), `steps[${index}].mode`));
        const conditions = Array.isArray(step['conditions']) ? step['conditions'] as Array<Record<string, unknown>> : [];
        conditions.forEach((condition, conditionIndex) => {
            const row = element('div', 'step-header');
            const column = columns.find(item => item.name === condition['column']);
            row.append(
                selectInput(columns.map(column => column.name), String(condition['column'] ?? ''), value => {
                    condition['column'] = value;
                    const selectedColumn = columns.find(item => item.name === value);
                    if (condition['op'] !== 'IS NULL' && condition['op'] !== 'IS NOT NULL') {
                        condition['value'] = defaultConditionValue(selectedColumn?.type);
                    }
                    changed();
                }, 'Column'),
                selectInput(['=', '<>', '<', '<=', '>', '>=', 'IS NULL', 'IS NOT NULL', 'LIKE', 'IN'], String(condition['op']), value => {
                    condition['op'] = value;
                    if (value === 'IS NULL' || value === 'IS NOT NULL') {
                        delete condition['value'];
                    } else if (condition['value'] === undefined) {
                        condition['value'] = defaultConditionValue(column?.type);
                    }
                    changed();
                })
            );
            if (condition['op'] !== 'IS NULL' && condition['op'] !== 'IS NOT NULL') {
                row.append(textInput(
                    Array.isArray(condition['value']) ? (condition['value'] as unknown[]).join(',') : String(condition['value'] ?? ''),
                    value => {
                        condition['value'] = typedConditionValue(value, column?.type, condition['op'] === 'IN');
                        changed();
                    },
                    condition['op'] === 'IN' ? 'Comma-separated values' : 'Value'
                ));
            }
            row.append(button('Remove condition', () => {
                conditions.splice(conditionIndex, 1);
                changed();
            }));
            wrapper.append(
                row,
                issueView(`steps[${index}].conditions[${conditionIndex}].column`),
                issueView(`steps[${index}].conditions[${conditionIndex}].value`)
            );
        });
        wrapper.append(button('Add condition', () => {
            conditions.push({ column: columns[0]?.name ?? '', op: '=', value: '' });
            step['conditions'] = conditions;
            changed();
        }));
    } else if (step['type'] === 'select') {
        const selected = Array.isArray(step['columns']) ? step['columns'] as Array<Record<string, unknown>> : [];
        columns.forEach(column => {
            const match = selected.find(item => item['name'] === column.name);
            const selection = labelled(column.name, checkboxInput(Boolean(match), checked => {
                if (checked && !match) {
                    selected.push({ name: column.name });
                } else if (!checked && match) {
                    selected.splice(selected.indexOf(match), 1);
                }
                step['columns'] = selected;
                changed();
            }), `steps[${index}].columns`);
            if (match) {
                selection.append(textInput(String(match['as'] ?? ''), value => {
                    if (value) {
                        match['as'] = value;
                    } else {
                        delete match['as'];
                    }
                    changed();
                }, 'Alias (optional)'));
                wrapper.append(issueView(`steps[${index}].columns[${selected.indexOf(match)}].as`));
            }
            wrapper.append(selection);
        });
        const computed = Array.isArray(step['computed']) ? step['computed'] as Array<Record<string, unknown>> : [];
        computed.forEach((item, computedIndex) => {
            const row = element('div', 'step-header');
            row.append(
                textInput(String(item['as'] ?? ''), value => {
                    item['as'] = value;
                    changed();
                }, 'Output name'),
                selectInput(['column', 'literal', 'binary', 'call', 'raw'], String((item['expr'] as Record<string, unknown>)?.['type'] ?? 'column'), value => {
                    item['expr'] = defaultExpression(value, columns);
                    changed();
                }, 'Expression')
            );
            row.append(...expressionFields(item, columns));
            row.append(button('Remove', () => {
                computed.splice(computedIndex, 1);
                changed();
            }));
            wrapper.append(row);
        });
        wrapper.append(button('Add computed column', () => {
            computed.push({ as: '', expr: defaultExpression('column', columns) });
            step['computed'] = computed;
            changed();
        }));
    } else if (step['type'] === 'parse') {
        wrapper.append(
            labelled('Parse type', selectInput(['jsonValue', 'splitToRows'], String(step['kind']), value => {
                step['kind'] = value;
                changed();
            }), `steps[${index}].kind`),
            labelled('Input column', selectInput(columns.map(column => column.name), String(step['column'] ?? ''), value => {
                step['column'] = value;
                changed();
            }), `steps[${index}].column`)
        );
        if (step['kind'] === 'splitToRows') {
            wrapper.append(
                labelled('Delimiter', textInput(String(step['delimiter'] ?? ' '), value => {
                    step['delimiter'] = value;
                    changed();
                }), `steps[${index}].delimiter`),
                labelled('Output column', textInput(String(step['as'] ?? ''), value => {
                    step['as'] = value;
                    changed();
                }), `steps[${index}].as`)
            );
        } else {
            wrapper.append(jsonField('JSON fields (JSON)', step['fields'], value => {
                step['fields'] = value;
                changed();
            }, `steps[${index}].fields`));
        }
    } else if (step['type'] === 'aggregate') {
        wrapper.append(
            ...groupByEditor(step, columns, index, 'groupBy'),
            ...aggregateEditor(step, columns, index)
        );
    } else if (step['type'] === 'windowAggregate') {
        wrapper.append(...windowAggregateForm(step, columns, index));
    } else if (step['type'] === 'topN') {
        const partitionBy = Array.isArray(step['partitionBy']) ? step['partitionBy'] as string[] : [];
        wrapper.append(...groupByEditor(step, columns, index, 'partitionBy', 'Partition by'));
        const orderBy = isRecord(step['orderBy']) ? step['orderBy'] as Record<string, unknown> : {};
        wrapper.append(
            labelled('Order column', selectInput(
                columns.map(column => column.name),
                String(orderBy['column'] ?? ''),
                value => {
                    step['orderBy'] = { ...orderBy, column: value };
                    changed();
                },
                'Choose a column'
            ), `steps[${index}].orderBy.column`),
            labelled('Direction', selectInput(['ASC', 'DESC'], String(orderBy['direction'] ?? 'DESC'), value => {
                step['orderBy'] = { ...orderBy, direction: value };
                changed();
            }), `steps[${index}].orderBy.direction`),
            labelled('Keep first N rows', numberInput(step['n'] ?? 2, value => {
                step['n'] = value;
                changed();
            }), `steps[${index}].n`),
            labelled('Rank column (optional)', textInput(String(step['rankColumn'] ?? ''), value => {
                step['rankColumn'] = value || undefined;
                changed();
            }), `steps[${index}].rankColumn`)
        );
        if (partitionBy.includes('window_start') && partitionBy.includes('window_end')) {
            const note = element('p', 'muted');
            note.textContent = 'Partitioning by both window boundaries makes this an append-only window Top-N.';
            wrapper.append(note);
        }
    } else if (step['type'] === 'deduplicate') {
        const inputMetadata = index === 0 ? sourceMetadata : pipelineByStep[index - 1];
        const timeColumn = inputMetadata?.timeAttribute;
        wrapper.append(
            ...groupByEditor(step, columns, index, 'keyColumns', 'Key'),
            labelled('Keep', selectInput(['first', 'last'], String(step['keep'] ?? 'first'), value => {
                step['keep'] = value;
                changed();
            }), `steps[${index}].keep`),
            labelled('Order by time attribute', selectInput(
                timeColumn ? [timeColumn] : [],
                String(step['orderColumn'] ?? timeColumn ?? ''),
                value => {
                    step['orderColumn'] = value;
                    changed();
                },
                timeColumn ? undefined : 'No time attribute is available'
            ), `steps[${index}].orderColumn`)
        );
    }
    return wrapper;
}

function groupByEditor(
    step: Record<string, unknown>,
    columns: Column[],
    index: number,
    property: string,
    labelPrefix = 'Group by'
): HTMLElement[] {
    const selected = Array.isArray(step[property]) ? step[property] as string[] : [];
    return columns.map((column, columnIndex) => {
        const field = labelled(`${labelPrefix} ${column.name}`, checkboxInput(
            selected.includes(column.name),
            checked => {
                step[property] = checked
                    ? [...selected, column.name]
                    : selected.filter(name => name !== column.name);
                changed();
            }
        ), `steps[${index}].${property}[${columnIndex}]`);
        return field;
    });
}

function aggregateEditor(step: Record<string, unknown>, columns: Column[], index: number): HTMLElement[] {
    const wrapper = element('div', 'fields');
    const aggregates = Array.isArray(step['aggregates'])
        ? step['aggregates'] as Array<Record<string, unknown>>
        : [];
    aggregates.forEach((aggregate, aggregateIndex) => {
        const row = element('div', 'step-header');
        row.append(
            selectInput(['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'], String(aggregate['fn']), value => {
                aggregate['fn'] = value;
                changed();
            }),
            selectInput(['*', ...columns.map(column => column.name)], String(aggregate['column'] ?? '*'), value => {
                aggregate['column'] = value === '*' ? undefined : value;
                changed();
            }, 'Column'),
            textInput(String(aggregate['as'] ?? ''), value => {
                aggregate['as'] = value;
                changed();
            }, 'Output name'),
            button('Remove', () => {
                aggregates.splice(aggregateIndex, 1);
                changed();
            })
        );
        wrapper.append(
            row,
            issueView(`steps[${index}].aggregates[${aggregateIndex}].column`),
            issueView(`steps[${index}].aggregates[${aggregateIndex}].as`)
        );
    });
    wrapper.append(button('Add aggregate', () => {
        aggregates.push({ fn: 'COUNT', as: '' });
        step['aggregates'] = aggregates;
        changed();
    }));
    return [wrapper];
}

function windowAggregateForm(step: Record<string, unknown>, columns: Column[], index: number): HTMLElement[] {
    const window = isRecord(step['window']) ? step['window'] as Record<string, unknown> : {};
    const kind = String(window['kind'] ?? 'tumble');
    const controls: HTMLElement[] = [
        labelled('Window type', selectInput(['tumble', 'hop', 'cumulate', 'session'], kind, value => {
            step['window'] = defaultWindow(value);
            changed();
        }), `steps[${index}].window.kind`)
    ];
    if (kind === 'tumble') {
        controls.push(durationInput('Window size', window['size'], value => {
            window['size'] = value;
            changed();
        }, `steps[${index}].window.size`));
    } else if (kind === 'hop') {
        controls.push(
            durationInput('Slide', window['slide'], value => {
                window['slide'] = value;
                changed();
            }, `steps[${index}].window.slide`),
            durationInput('Window size', window['size'], value => {
                window['size'] = value;
                changed();
            }, `steps[${index}].window.size`)
        );
    } else if (kind === 'cumulate') {
        controls.push(
            durationInput('Step', window['step'], value => {
                window['step'] = value;
                changed();
            }, `steps[${index}].window.step`),
            durationInput('Maximum size', window['maxSize'], value => {
                window['maxSize'] = value;
                changed();
            }, `steps[${index}].window.maxSize`)
        );
    } else {
        controls.push(durationInput('Session gap', window['gap'], value => {
            window['gap'] = value;
            changed();
        }, `steps[${index}].window.gap`));
    }
    controls.push(
        ...groupByEditor(step, columns, index, 'groupBy'),
        ...aggregateEditor(step, columns, index),
        labelled('Include window start', checkboxInput(Boolean(step['includeWindowStart']), value => {
            step['includeWindowStart'] = value;
            changed();
        }), `steps[${index}].includeWindowStart`),
        labelled('Include window end', checkboxInput(Boolean(step['includeWindowEnd']), value => {
            step['includeWindowEnd'] = value;
            changed();
        }), `steps[${index}].includeWindowEnd`),
        windowExplanation(kind, window),
        windowTimeline(kind, window)
    );
    return controls;
}

function windowExplanation(kind: string, window: Record<string, unknown>): HTMLElement {
    const note = element('p', 'muted');
    if (kind === 'tumble') {
        note.textContent = `Tumble emits one non-overlapping result every ${formatDuration(window['size'])}.`;
    } else if (kind === 'hop') {
        note.textContent = `Hop emits overlapping results every ${formatDuration(window['slide'])}, each covering ${formatDuration(window['size'])}.`;
    } else if (kind === 'cumulate') {
        note.textContent = `Cumulate grows a result by ${formatDuration(window['step'])} until it reaches ${formatDuration(window['maxSize'])}.`;
    } else {
        note.textContent = `Session groups nearby events until there is a gap of ${formatDuration(window['gap'])}.`;
    }
    return note;
}

function defaultWindow(kind: string): Record<string, unknown> {
    switch (kind) {
        case 'hop':
            return {
                kind,
                slide: { value: 30, unit: 'SECOND' },
                size: { value: 1, unit: 'MINUTE' }
            };
        case 'cumulate':
            return {
                kind,
                step: { value: 1, unit: 'MINUTE' },
                maxSize: { value: 1, unit: 'HOUR' }
            };
        case 'session':
            return { kind, gap: { value: 1, unit: 'MINUTE' } };
        default:
            return { kind: 'tumble', size: { value: 1, unit: 'MINUTE' } };
    }
}

function durationInput(
    title: string,
    raw: unknown,
    onChange: (duration: { value: number; unit: string }) => void,
    path: string
): HTMLElement {
    const duration = isRecord(raw) ? raw : { value: 1, unit: 'MINUTE' };
    const row = element('div', 'step-header');
    const amount = numberInput(duration['value'], value => {
        onChange({ value, unit: String(duration['unit'] ?? 'MINUTE') });
    });
    amount.min = '1';
    const unit = selectInput(
        ['SECOND', 'MINUTE', 'HOUR', 'DAY'],
        String(duration['unit'] ?? 'MINUTE'),
        value => onChange({ value: Number(duration['value'] ?? 1), unit: value })
    );
    row.append(amount, unit);
    return labelled(title, row, path);
}

function formatDuration(raw: unknown): string {
    if (!isRecord(raw)) {
        return 'the selected duration';
    }
    return `${String(raw['value'] ?? '?')} ${String(raw['unit'] ?? '')}`;
}

function windowTimeline(kind: string, window: Record<string, unknown>): HTMLElement {
    const namespace = 'http://www.w3.org/2000/svg';
    const svg = document.createElementNS(namespace, 'svg');
    svg.classList.add('timeline');
    svg.setAttribute('viewBox', '0 0 360 92');
    svg.setAttribute('role', 'img');
    svg.setAttribute('aria-label', `${kind} window timeline`);
    const axis = document.createElementNS(namespace, 'line');
    axis.setAttribute('x1', '16');
    axis.setAttribute('x2', '344');
    axis.setAttribute('y1', '70');
    axis.setAttribute('y2', '70');
    svg.append(axis);
    if (kind === 'hop') {
        addTimelineRect(svg, 30, 30, 160, 28, `size ${formatDuration(window['size'])}`);
        addTimelineRect(svg, 110, 30, 160, 28, `slide ${formatDuration(window['slide'])}`);
    } else if (kind === 'cumulate') {
        addTimelineRect(svg, 26, 42, 75, 16, formatDuration(window['step']));
        addTimelineRect(svg, 101, 30, 150, 28, formatDuration(window['maxSize']));
        addTimelineRect(svg, 251, 18, 83, 40, 'growing window');
    } else if (kind === 'session') {
        for (const x of [70, 105, 235, 270]) {
            const event = document.createElementNS(namespace, 'circle');
            event.setAttribute('cx', String(x));
            event.setAttribute('cy', '49');
            event.setAttribute('r', '7');
            event.setAttribute('fill', 'var(--vscode-button-background)');
            svg.append(event);
        }
        addTimelineRect(svg, 52, 29, 71, 8, `gap ${formatDuration(window['gap'])}`);
        addTimelineRect(svg, 217, 29, 71, 8, 'new session');
    } else {
        addTimelineRect(svg, 30, 30, 92, 28, formatDuration(window['size']));
        addTimelineRect(svg, 134, 30, 92, 28, formatDuration(window['size']));
        addTimelineRect(svg, 238, 30, 92, 28, formatDuration(window['size']));
    }
    const container = element('div', 'timeline-container');
    container.append(svg);
    return container;
}

function addTimelineRect(svg: SVGSVGElement, x: number, y: number, width: number, height: number, text: string): void {
    const namespace = 'http://www.w3.org/2000/svg';
    const rect = document.createElementNS(namespace, 'rect');
    rect.setAttribute('x', String(x));
    rect.setAttribute('y', String(y));
    rect.setAttribute('width', String(width));
    rect.setAttribute('height', String(height));
    rect.setAttribute('rx', '3');
    const label = document.createElementNS(namespace, 'text');
    label.setAttribute('x', String(x + width / 2));
    label.setAttribute('y', String(y + height / 2 + 4));
    label.setAttribute('text-anchor', 'middle');
    label.textContent = text;
    svg.append(rect, label);
}

function sinkForm(): HTMLElement {
    const wrapper = element('div', 'card');
    wrapper.append(labelled('Sink type', selectInput(['print', 'blackhole', 'filesystem'], String(spec.sink['type']), value => {
        spec.sink = value === 'filesystem'
            ? { type: value, path: '', format: 'json' }
            : { type: value };
        changed();
    }), 'sink.type'));
    if (spec.sink['type'] === 'filesystem') {
        wrapper.append(
            labelled('Path', textInput(String(spec.sink['path'] ?? ''), value => {
                spec.sink['path'] = value;
                changed();
            }), 'sink.path'),
            labelled('Format', selectInput(['json', 'csv'], String(spec.sink['format']), value => {
                spec.sink['format'] = value;
                changed();
            }), 'sink.format')
        );
    }
    return wrapper;
}

function settingsForm(): HTMLElement {
    const wrapper = element('div', 'card');
    for (const [key, title] of [
        ['parallelism', 'Parallelism'],
        ['checkpointIntervalSeconds', 'Checkpoint interval (seconds)'],
        ['stateTtlSeconds', 'State TTL (seconds)'],
        ['idleTimeoutSeconds', 'Source idle timeout (seconds)']
    ]) {
        wrapper.append(labelled(title, numberInput(spec.settings[key], value => {
            spec.settings[key] = value || undefined;
            changed();
        }, true), `settings.${key}`));
    }
    if (readOnly) {
        const warning = element('p', 'warning');
        warning.textContent = 'This profile is read-only. Job submission is disabled.';
        wrapper.append(warning);
    }
    return wrapper;
}

function actionButtons(): HTMLElement {
    const wrapper = element('div', 'actions');
    wrapper.append(
        button('Preview final result', () => post({
            type: 'preview',
            spec,
            target: spec.steps.length ? spec.steps.length - 1 : 'source'
        })),
        ...(previewRunning ? [button('Cancel preview', () => post({ type: 'cancelPreview' }))] : []),
        button('Validate', () => post({ type: 'validate', spec })),
        button('Submit', () => post({ type: 'submit', spec }), readOnly),
        button('Copy SQL', () => post({ type: 'copySql', spec })),
        button('Save Spec', () => post({ type: 'save', spec })),
        button('Load Spec', () => post({ type: 'load' }))
    );
    return wrapper;
}

function schemaTable(): HTMLElement {
    const section = element('div', 'card');
    if (schemaError) {
        const error = element('p', 'warning');
        error.textContent = schemaError;
        section.append(error);
    }
    const metadata = pipelineByStep.at(-1) ?? sourceMetadata;
    const schema = metadata?.schema ?? schemaByStep.at(-1) ?? sourceSchema;
    const timeLabel = element('p', 'muted');
    timeLabel.textContent = metadata?.timeAttribute
        ? `Time attribute: ${metadata.timeAttribute}`
        : 'Time attribute: none';
    section.append(timeLabel);
    const modeLabel = element('p', 'muted');
    modeLabel.textContent = `Result mode: ${metadata?.changelogMode ?? changelogMode}`;
    section.append(modeLabel);
    if (metadata?.lostTimeAttributeAt) {
        const lost = element('p', 'warning');
        lost.textContent = `The time attribute was lost at ${metadata.lostTimeAttributeAt}.`;
        section.append(lost);
    }
    if (spec.sink['type'] === 'filesystem' && (metadata?.changelogMode ?? changelogMode) === 'UPDATING') {
        const incompatible = element('p', 'error');
        incompatible.textContent = 'This filesystem sink requires append-only rows, but the current steps produce updates.';
        section.append(incompatible);
    }
    if (!schema.length) {
        const empty = element('p', 'muted');
        empty.textContent = 'Schema is resolved from Flink when the source and steps are valid.';
        section.append(empty);
        return section;
    }
    const table = element('table');
    const header = element('tr');
    for (const title of ['Column', 'Type', 'Time attribute']) {
        const cell = element('th');
        cell.textContent = title;
        header.append(cell);
    }
    const head = element('thead');
    head.append(header);
    const body = element('tbody');
    for (const column of schema) {
        const row = element('tr');
        const name = element('td');
        name.textContent = column.name;
        const type = element('td');
        type.textContent = column.type;
        const time = element('td');
        time.textContent = column.name === metadata?.timeAttribute ? 'Yes' : '';
        row.append(name, type, time);
        body.append(row);
    }
    table.append(head, body);
    section.append(table);
    return section;
}

function issuesView(): HTMLElement {
    const wrapper = element('div', 'issues');
    for (const item of issues) {
        const issue = element('p', item.severity);
        issue.textContent = `${item.path}: ${item.message}`;
        wrapper.append(issue);
    }
    return wrapper;
}

function previewPanel(): HTMLElement {
    const section = element('section', 'card preview-results');
    const title = element('h3');
    title.textContent = 'Preview results';
    section.append(title);
    if (previewRunning) {
        const status = element('p', 'muted');
        status.textContent = 'Preview is running…';
        section.append(status, button('Cancel preview', () => post({ type: 'cancelPreview' })));
    }
    if (previewWarning) {
        const warning = element('p', 'warning');
        warning.textContent = previewWarning;
        section.append(warning);
    }
    if (!previewResult) {
        if (!previewRunning) {
            const hint = element('p', 'muted');
            hint.textContent = 'Preview the source, a step, or the final result to see sample rows.';
            section.append(hint);
        }
        return section;
    }
    const result = previewResult;
    const summary = element('p', 'muted');
    summary.textContent = `${targetLabel(result.target)}: ${result.error ? 'failed' : 'completed'} in ${result.durationMs} ms; ${result.rowCount} row${result.rowCount === 1 ? '' : 's'} fetched.`;
    section.append(summary);
    if (result.error) {
        const error = element('pre', 'error');
        error.textContent = result.error;
        section.append(error);
        return section;
    }
    const originalRows = result.rows;
    const hasKinds = originalRows.some(row =>
        typeof row.kind === 'string' || typeof row.rowKind === 'string'
    );
    if (result.changelogMode === 'UPDATING' && result.keyColumns?.length) {
        const toggle = labelled(
            'Show final state',
            checkboxInput(showFinalState, value => {
                showFinalState = value;
                render();
            })
        );
        section.append(toggle);
    }
    const namedRows = originalRows.map(row => namedPreviewRow(row, result.columns));
    const displayedRows = showFinalState && result.keyColumns?.length
        ? applyChangelog(namedRows, result.keyColumns) ?? []
        : namedRows;
    const rowCount = element('p', 'muted');
    rowCount.textContent = `${displayedRows.length} row${displayedRows.length === 1 ? '' : 's'} shown${showFinalState ? ' (final state)' : ''}.`;
    section.append(rowCount);
    if (result.truncated) {
        const truncated = element('p', 'warning');
        truncated.textContent = `Truncated at ${originalRows.length} rows.`;
        section.append(truncated);
    }
    if (!displayedRows.length) {
        const hint = element('p', 'muted');
        hint.textContent = previewEmptyHint(result.target);
        section.append(hint);
    }
    const table = element('table');
    const header = element('tr');
    const showKind = !showFinalState
        && (result.changelogMode === 'UPDATING' || hasKinds);
    if (showKind) {
        const cell = element('th');
        cell.textContent = 'kind';
        header.append(cell);
    }
    for (const column of result.columns) {
        const cell = element('th');
        cell.textContent = `${column.name} (${column.type})`;
        header.append(cell);
    }
    const head = element('thead');
    head.append(header);
    const body = element('tbody');
    for (const row of displayedRows) {
        const tr = element('tr');
        if (showKind) {
            const cell = element('td');
            cell.textContent = String(row.$rowKind ?? '');
            tr.append(cell);
        }
        for (const column of result.columns) {
            const cell = element('td');
            cell.textContent = formatPreviewValue(row[column.name]);
            tr.append(cell);
        }
        body.append(tr);
    }
    table.append(head, body);
    section.append(table);
    return section;
}

function namedPreviewRow(
    row: PreviewResult['rows'][number],
    columns: PreviewColumn[]
): PreviewRow {
    const named: PreviewRow = {};
    const values = Array.isArray(row.fields)
        ? row.fields
        : columns.map(column => row[column.name]);
    columns.forEach((column, index) => {
        named[column.name] = values[index];
    });
    const kind = row.kind ?? row.rowKind;
    if (typeof kind === 'string') {
        named.$rowKind = kind;
    }
    return named;
}

function formatPreviewValue(value: unknown): string {
    if (value === null) {
        return 'NULL';
    }
    if (value === undefined) {
        return '';
    }
    return typeof value === 'string' ? value : JSON.stringify(value);
}

function previewEmptyHint(target: 'source' | number): string {
    const activeSteps = target === 'source' ? [] : spec.steps.slice(0, target + 1);
    const hasWindow = activeSteps.some(step => step.type === 'windowAggregate');
    if (hasWindow && isRecord(spec.source['timeAttribute'])) {
        const timeAttribute = spec.source['timeAttribute'];
        if (timeAttribute['kind'] === 'processingTime') {
            return 'Processing-time windows need real time to pass. Check that the source is producing rows and wait for the window interval.';
        }
        if (timeAttribute['kind'] === 'eventTime') {
            return 'Event-time windows close only when later data advances the watermark. Check that later event timestamps are present.';
        }
    }
    if (spec.source['type'] === 'filesystem') {
        return 'No rows were returned. The folder may be empty, or the source path may be wrong inside the Flink container.';
    }
    return 'No rows were returned. Check the source settings and whether earlier steps filter out all rows.';
}

function targetLabel(target: 'source' | number): string {
    return target === 'source' ? 'Source' : `Step ${target + 1}`;
}

function issueView(path: string): HTMLElement {
    const wrapper = element('span', 'inline-issue');
    wrapper.textContent = issues.filter(item => item.path === path).map(item => item.message).join(' ');
    return wrapper;
}

function jsonField(
    title: string,
    value: unknown,
    onValue: (value: unknown) => void,
    path: string
): HTMLElement {
    const wrapper = element('div', 'field');
    const textarea = element('textarea');
    textarea.value = JSON.stringify(value ?? [], null, 2);
    textarea.addEventListener('change', () => {
        try {
            onValue(JSON.parse(textarea.value) as unknown);
        } catch {
            const error = issueView(path);
            error.textContent = 'Enter valid JSON.';
            wrapper.append(error);
        }
    });
    wrapper.append(labelled(title, textarea, path), issueView(path));
    return wrapper;
}

function schemaAt(stepIndex: number): Column[] {
    return stepIndex === 0 ? sourceSchema : schemaByStep[stepIndex - 1] ?? [];
}

function setSource(type: string): void {
    spec.source = type === 'datagen'
        ? { type, rowsPerSecond: 1, fields: [{ name: 'line', type: 'STRING', kind: 'random' }] }
        : { type, path: '', format: 'raw', watch: false };
    changed();
}

function setSourceFormat(format: string): void {
    spec.source['format'] = format;
    if (format === 'raw') {
        delete spec.source['fields'];
        delete spec.source['options'];
    } else if (!Array.isArray(spec.source['fields'])) {
        spec.source['fields'] = [{ name: 'line', type: 'STRING' }];
    }
    changed();
}

function defaultStep(type: string): Record<string, unknown> {
    switch (type) {
        case 'filter':
            return { type, mode: 'all', conditions: [] };
        case 'select':
            return { type, columns: [], computed: [] };
        case 'parse':
            return { type, kind: 'splitToRows', column: '', delimiter: ' ', as: '' };
        case 'aggregate':
            return { type, groupBy: [], aggregates: [] };
        case 'windowAggregate':
            return {
                type,
                window: defaultWindow('tumble'),
                groupBy: [],
                aggregates: [],
                includeWindowStart: true,
                includeWindowEnd: true
            };
        case 'topN':
            return {
                type,
                partitionBy: [],
                orderBy: { column: '', direction: 'DESC' },
                n: 2
            };
        case 'deduplicate':
            return { type, keyColumns: [], keep: 'first', orderColumn: '' };
        default:
            return {};
    }
}

function defaultExpression(type: string, columns: Column[]): Record<string, unknown> {
    const columnName = columns[0]?.name ?? '';
    switch (type) {
        case 'literal':
            return { type, value: '' };
        case 'binary':
            return {
                type,
                op: '+',
                left: { type: 'column', name: columnName },
                right: { type: 'literal', value: 0 }
            };
        case 'call':
            return { type, fn: 'UPPER', args: [{ type: 'column', name: columnName }] };
        case 'raw':
            return { type, text: '' };
        default:
            return { type: 'column', name: columnName };
    }
}

function expressionFields(item: Record<string, unknown>, columns: Column[]): HTMLElement[] {
    const expr = item['expr'] as Record<string, unknown> | undefined;
    if (!expr) {
        return [];
    }
    if (expr['type'] === 'column') {
        return [selectInput(columns.map(column => column.name), String(expr['name'] ?? ''), value => {
            expr['name'] = value;
            changed();
        }, 'Column')];
    }
    if (expr['type'] === 'literal') {
        return [textInput(String(expr['value'] ?? ''), value => {
            expr['value'] = value;
            changed();
        }, 'String literal')];
    }
    if (expr['type'] === 'binary') {
        const left = expr['left'] as Record<string, unknown>;
        const right = expr['right'] as Record<string, unknown>;
        return [
            selectInput(['+', '-', '*', '/'], String(expr['op']), value => {
                expr['op'] = value;
                changed();
            }),
            selectInput(columns.map(column => column.name), String(left['name'] ?? ''), value => {
                expr['left'] = { type: 'column', name: value };
                changed();
            }, 'Left column'),
            selectInput(columns.map(column => column.name), String(right['name'] ?? ''), value => {
                expr['right'] = { type: 'column', name: value };
                changed();
            }, 'Right column')
        ];
    }
    if (expr['type'] === 'call') {
        const args = Array.isArray(expr['args']) ? expr['args'] as Array<Record<string, unknown>> : [];
        const firstArgument = args[0] ?? { type: 'column', name: columns[0]?.name ?? '' };
        const controls: HTMLElement[] = [
            selectInput(['UPPER', 'LOWER', 'TRIM', 'CONCAT', 'ABS', 'ROUND', 'CAST'], String(expr['fn']), value => {
                expr['fn'] = value;
                if (value === 'CAST') {
                    expr['to'] = 'STRING';
                } else {
                    delete expr['to'];
                }
                expr['args'] = value === 'CONCAT'
                    ? [
                        { type: 'column', name: columns[0]?.name ?? '' },
                        { type: 'column', name: columns[1]?.name ?? columns[0]?.name ?? '' }
                    ]
                    : [{ type: 'column', name: columns[0]?.name ?? '' }];
                changed();
            }),
            selectInput(columns.map(column => column.name), String(firstArgument['name'] ?? ''), value => {
                expr['args'] = [{ type: 'column', name: value }];
                changed();
            }, 'Argument column')
        ];
        if (expr['fn'] === 'CONCAT') {
            const secondArgument = args[1] ?? { type: 'column', name: columns[1]?.name ?? '' };
            controls.push(selectInput(
                columns.map(column => column.name),
                String(secondArgument['name'] ?? ''),
                value => {
                    expr['args'] = [firstArgument, { type: 'column', name: value }];
                    changed();
                },
                'Second argument'
            ));
        }
        if (expr['fn'] === 'CAST') {
            controls.push(selectInput(
                ['STRING', 'INT', 'BIGINT', 'DOUBLE', 'DECIMAL(10,2)', 'BOOLEAN', 'DATE', 'TIMESTAMP(3)'],
                String(expr['to'] ?? 'STRING'),
                value => {
                    expr['to'] = value;
                    changed();
                },
                'Cast type'
            ));
        }
        return controls;
    }
    if (expr['type'] === 'raw') {
        const textarea = element('textarea');
        textarea.value = String(expr['text'] ?? '');
        textarea.placeholder = 'Unchecked expression (review carefully)';
        textarea.addEventListener('change', () => {
            expr['text'] = textarea.value;
            changed();
        });
        return [textarea];
    }
    return [];
}

function defaultConditionValue(type: string | undefined): string | number | boolean {
    if (type === 'INT' || type === 'BIGINT' || type === 'DOUBLE' || type?.startsWith('DECIMAL(')) {
        return 0;
    }
    if (type === 'BOOLEAN') {
        return false;
    }
    return '';
}

function typedConditionValue(value: string, type: string | undefined, list: boolean): unknown {
    const convert = (entry: string): string | number | boolean => {
        if (type === 'INT' || type === 'BIGINT' || type === 'DOUBLE' || type?.startsWith('DECIMAL(')) {
            return entry.trim() === '' ? entry : Number(entry);
        }
        if (type === 'BOOLEAN') {
            if (entry.toLowerCase() === 'true') {
                return true;
            }
            if (entry.toLowerCase() === 'false') {
                return false;
            }
            return entry;
        }
        return entry;
    };
    return list ? value.split(',').map(convert) : convert(value);
}

function moveStep(index: number, offset: number): void {
    const target = index + offset;
    if (target < 0 || target >= spec.steps.length) {
        return;
    }
    const activeIndex = stepIndexFromTab(activeBuilderTab);
    if (activeIndex !== undefined) {
        if (activeIndex === index) {
            activeBuilderTab = `step:${target}`;
        } else if (index < activeIndex && target >= activeIndex) {
            activeBuilderTab = `step:${activeIndex - 1}`;
        } else if (index > activeIndex && target <= activeIndex) {
            activeBuilderTab = `step:${activeIndex + 1}`;
        }
    }
    const [step] = spec.steps.splice(index, 1);
    spec.steps.splice(target, 0, step);
    changed();
}

function deleteStep(index: number): void {
    const activeIndex = stepIndexFromTab(activeBuilderTab);
    spec.steps.splice(index, 1);
    if (activeIndex === index) {
        activeBuilderTab = spec.steps.length
            ? `step:${Math.min(index, spec.steps.length - 1)}`
            : 'source';
    } else if (activeIndex !== undefined && activeIndex > index) {
        activeBuilderTab = `step:${activeIndex - 1}`;
    }
    changed();
}

function stepIndexFromTab(tab: string): number | undefined {
    if (!tab.startsWith('step:')) {
        return undefined;
    }
    const index = Number(tab.slice('step:'.length));
    return Number.isInteger(index) ? index : undefined;
}

function changed(): void {
    resultMessage = '';
    resultJobId = undefined;
    previewRunning = false;
    previewResult = undefined;
    previewWarning = '';
    showFinalState = false;
    post({ type: 'change', spec });
}

function post(message: WebviewMessage): void {
    vscode.postMessage(message);
}

function labelled(title: string, input: HTMLElement, path?: string): HTMLElement {
    const wrapper = element('label', 'field');
    const titleNode = element('span');
    titleNode.textContent = title;
    wrapper.append(titleNode, input);
    if (path) {
        wrapper.append(issueView(path));
    }
    return wrapper;
}

function textInput(value: string, onChange: (value: string) => void, placeholder = ''): HTMLInputElement {
    const input = element('input');
    input.type = 'text';
    input.value = value;
    input.placeholder = placeholder;
    input.addEventListener('change', () => onChange(input.value));
    return input;
}

function numberInput(value: unknown, onChange: (value: number) => void, optional = false): HTMLInputElement {
    const input = element('input');
    input.type = 'number';
    input.value = value === undefined ? '' : String(value);
    input.min = '0';
    input.addEventListener('change', () => onChange(input.value === '' && optional ? 0 : Number(input.value)));
    return input;
}

function checkboxInput(value: boolean, onChange: (value: boolean) => void): HTMLInputElement {
    const input = element('input');
    input.type = 'checkbox';
    input.checked = value;
    input.addEventListener('change', () => onChange(input.checked));
    return input;
}

function selectInput(
    options: string[],
    value: string,
    onChange: (value: string) => void,
    placeholder?: string
): HTMLSelectElement {
    const select = element('select');
    if (placeholder) {
        const option = element('option');
        option.value = '';
        option.textContent = placeholder;
        select.append(option);
    }
    for (const optionValue of options) {
        const option = element('option');
        option.value = optionValue;
        option.textContent = optionValue;
        option.selected = value === optionValue;
        select.append(option);
    }
    select.addEventListener('change', () => onChange(select.value));
    return select;
}

function button(title: string, action: () => void, disabled = false): HTMLButtonElement {
    const control = element('button');
    control.textContent = title;
    control.disabled = disabled;
    control.addEventListener('click', action);
    return control;
}

function heading(title: string): HTMLElement {
    const titleElement = element('h2');
    titleElement.textContent = title;
    return titleElement;
}

function textBlock(text: string, className: string): HTMLElement {
    const pre = element('pre', className);
    pre.textContent = text;
    return pre;
}

function element<K extends keyof HTMLElementTagNameMap>(
    tag: K,
    className = ''
): HTMLElementTagNameMap[K] {
    const node = document.createElement(tag);
    if (className) {
        node.className = className;
    }
    return node;
}

function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}
