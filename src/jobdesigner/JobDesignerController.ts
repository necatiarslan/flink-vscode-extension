import * as vscode from 'vscode';
import { basename } from 'path';
import { open } from 'node:fs/promises';
import { ClusterStore } from '../flink/ClusterStore';
import { FlinkApiError } from '../flink/FlinkClient';
import { SqlGatewayClient } from '../flink/SqlGatewayClient';
import { classifyStatement, redactSqlLiterals } from '../flink/sqlStatements';
import { ClusterProfile } from '../flink/types';
import { compilePreview, compileSpec, validateSpec } from './compile';
import { inferSample, SampleFormat } from './infer';
import { isCompilerGeneratedPreviewSequence } from './preview';
import { JobDesignerPanel, JobDesignerWebviewMessage } from './JobDesignerPanel';
import { resolveJobSchema } from './schema';
import type { ResolvedJobSchema } from './schema';
import type { JobSpec, ValidationIssue } from './spec';
import type { FlinkTreeNode } from '../tree/FlinkTreeProvider';

interface PanelState {
    revision: number;
    timer?: NodeJS.Timeout;
    spec: unknown;
    sourceKey: string;
    schema?: ResolvedJobSchema;
    preview?: AbortController;
    previewRevision: number;
}

const SAMPLE_MAX_BYTES = 64 * 1024;
const SAMPLE_MAX_LINES = 200;

const TEMPLATE_FILES = [
    { label: 'Per-minute totals', description: 'Spec C · event-time tumble window', file: 'spec-c-window-totals.flinkjob.json' },
    { label: 'User sessions', description: 'Spec F · event-time session windows', file: 'spec-f-user-sessions.flinkjob.json' },
    { label: 'Top 2 users per window', description: 'Spec G · windowed Top-N', file: 'spec-g-window-top-two.flinkjob.json' },
    { label: 'Daily running total', description: 'Spec E · daily cumulate windows', file: 'spec-e-cumulate-daily.flinkjob.json' }
];

export class JobDesignerController {
    private readonly output = vscode.window.createOutputChannel('Flink');

    constructor(
        private readonly context: vscode.ExtensionContext,
        private readonly store: ClusterStore,
        private readonly refreshTree: () => void
    ) {
        context.subscriptions.push(
            this.output,
            vscode.commands.registerCommand('flink.newJob', (node?: FlinkTreeNode) => this.newJob(node)),
            vscode.commands.registerCommand('flink.openJobSpec', (uri?: vscode.Uri) => this.openJobSpec(uri))
        );
    }

    private async newJob(node?: FlinkTreeNode): Promise<void> {
        const profile = node?.profile?.sqlGatewayUrl
            ? node.profile
            : await this.chooseProfile();
        if (!profile) {
            return;
        }
        const selected = await vscode.window.showQuickPick([
            { label: 'Blank job', description: 'Start with an empty Flink job specification' },
            ...TEMPLATE_FILES
        ], { placeHolder: 'Start blank or choose a template' });
        if (!selected) {
            return;
        }
        if (!('file' in selected)) {
            this.openPanel(profile, defaultSpec());
            return;
        }
        try {
            const templateUri = vscode.Uri.joinPath(this.context.extensionUri, 'examples', 'jobs', selected.file);
            const template = JSON.parse(Buffer.from(await vscode.workspace.fs.readFile(templateUri)).toString('utf8')) as unknown;
            const errors = validateSpec(template).filter(item => item.severity === 'error');
            if (errors.length || !isJobSpec(template)) {
                throw new Error(formatIssues(errors.length ? errors : [{
                    path: '$',
                    severity: 'error',
                    message: 'The bundled template does not contain a valid JobSpec.'
                }]));
            }
            this.openPanel(profile, template, selected.label);
        } catch (error) {
            await vscode.window.showErrorMessage(`Could not load the ${selected.label} template: ${errorText(error)}`);
        }
    }

    private async openJobSpec(uri?: vscode.Uri): Promise<void> {
        const document = uri
            ? await vscode.workspace.openTextDocument(uri)
            : vscode.window.activeTextEditor?.document;
        if (!document || !document.fileName.toLowerCase().endsWith('.flinkjob.json')) {
            await vscode.window.showWarningMessage('Open a .flinkjob.json file to use this command.');
            return;
        }

        let parsed: unknown;
        try {
            parsed = JSON.parse(document.getText());
        } catch (error) {
            await vscode.window.showErrorMessage(`Could not read job spec JSON: ${errorText(error)}`);
            return;
        }
        const validation = validateSpec(parsed);
        const errors = validation.filter(issue => issue.severity === 'error');
        if (errors.length || !isJobSpec(parsed)) {
            await vscode.window.showErrorMessage(formatIssues(errors.length ? errors : [{
                path: '$',
                severity: 'error',
                message: 'The file does not contain a valid JobSpec.'
            }]));
            return;
        }
        const profile = await this.chooseProfile();
        if (profile) {
            this.openPanel(profile, parsed, basename(document.fileName));
        }
    }

    private openPanel(profile: ClusterProfile, initialSpec: JobSpec, title = `New Flink Job: ${profile.name}`): void {
        let designer: JobDesignerPanel;
        const state: PanelState = {
            revision: 0,
            spec: initialSpec,
            sourceKey: JSON.stringify(initialSpec.source),
            previewRevision: 0
        };
        designer = new JobDesignerPanel(
            this.context,
            title,
            message => {
                void this.handleMessage(designer, profile, state, message).catch(error => {
                    designer.postMessage({
                        type: 'result',
                        action: 'validate',
                        message: formatGatewayError(error)
                    });
                });
            }
        );
        designer.panel.onDidDispose(() => {
            if (state.timer) {
                clearTimeout(state.timer);
            }
            state.preview?.abort();
        });
        designer.showInitialState(initialSpec, profile.readOnly ?? false);
    }

    private async handleMessage(
        panel: JobDesignerPanel,
        profile: ClusterProfile,
        state: PanelState,
        message: JobDesignerWebviewMessage
    ): Promise<void> {
        switch (message.type) {
            case 'ready':
                if (isJobSpec(state.spec)) {
                    panel.showInitialState(state.spec, profile.readOnly ?? false);
                }
                await this.updateSpec(panel, profile, state, state.spec);
                return;
            case 'change':
                state.preview?.abort();
                state.preview = undefined;
                state.previewRevision++;
                state.spec = message.spec;
                await this.updateSpec(panel, profile, state, message.spec);
                return;
            case 'validate':
                await this.validateJob(panel, profile, message.spec);
                return;
            case 'submit':
                await this.submitJob(panel, profile, message.spec);
                return;
            case 'save':
                await this.saveSpec(message.spec);
                return;
            case 'load':
                await this.loadSpec(panel, profile, state);
                return;
            case 'copySql':
                await this.copySql(message.spec);
                return;
            case 'preview':
                await this.preview(panel, profile, state, message.spec, message.target);
                return;
            case 'cancelPreview':
                state.preview?.abort();
                return;
            case 'inferSample':
                await this.inferSample(panel);
                return;
            case 'openJob':
                await vscode.commands.executeCommand('flink.openSqlJob', profile.id, message.jobID);
                return;
        }
    }

    private async updateSpec(
        panel: JobDesignerPanel,
        profile: ClusterProfile,
        state: PanelState,
        input: unknown
    ): Promise<void> {
        const revision = ++state.revision;
        const issues = validateSpec(input);
        const errors = issues.filter(issue => issue.severity === 'error');
        let statements: string[] = [];
        let schemaError = '';
        let compiledPipeline: ReturnType<typeof compileSpec> | undefined;
        const spec = isJobSpec(input) ? input : undefined;
        const nextSourceKey = spec ? JSON.stringify(spec.source) : '';
        if (state.sourceKey !== nextSourceKey) {
            state.schema = undefined;
        }
        state.sourceKey = nextSourceKey;
        if (!errors.length && spec) {
            try {
                compiledPipeline = compileSpec(spec);
                statements = compiledPipeline.statements;
            } catch (error) {
                schemaError = errorText(error);
            }
        }
        panel.postMessage({
            type: 'update',
            issues,
            sql: statements,
            ...(compiledPipeline ? {
                sourceMetadata: compiledPipeline.sourceMetadata,
                pipelineByStep: compiledPipeline.pipelineByStep,
                changelogMode: compiledPipeline.changelogMode
            } : {}),
            ...(state.schema ? { sourceSchema: state.schema.source, schemaByStep: state.schema.schemaByStep } : {}),
            schemaError
        });

        if (state.timer) {
            clearTimeout(state.timer);
            state.timer = undefined;
        }
        if (!spec || !profile.sqlGatewayUrl) {
            return;
        }
        if (profile.readOnly) {
            panel.postMessage({
                type: 'update',
                issues,
                sql: statements,
                ...(state.schema ? { sourceSchema: state.schema.source, schemaByStep: state.schema.schemaByStep } : {}),
                schemaError: 'Live schema resolution requires temporary DDL, which is not allowed for this read-only profile.'
            });
            return;
        }
        const appendOnlyConflict = spec.sink.type === 'filesystem'
            && spec.steps.some(step => step.type === 'aggregate');
        const specForSchema = appendOnlyConflict
            ? undefined
            : errors.length
                ? longestValidPrefix(spec)
                : spec;
        if (!specForSchema) {
            return;
        }
        state.timer = setTimeout(() => {
            void resolveJobSchema(new SqlGatewayClient(profile), specForSchema)
                .then(schema => {
                    if (state.revision === revision) {
                        state.schema = schema;
                        const resolvedPipeline = !errors.length && schema.schemaByStep.length === spec.steps.length
                            ? compileSpec(spec, schema)
                            : undefined;
                        const resolvedSql = resolvedPipeline?.statements ?? statements;
                        panel.postMessage({
                            type: 'update',
                            issues,
                            sql: resolvedSql,
                            ...(resolvedPipeline ? {
                                sourceMetadata: resolvedPipeline.sourceMetadata,
                                pipelineByStep: resolvedPipeline.pipelineByStep,
                                changelogMode: resolvedPipeline.changelogMode
                            } : {}),
                            sourceSchema: schema.source,
                            schemaByStep: schema.schemaByStep
                        });
                    }
                })
                .catch(error => {
                    if (state.revision === revision) {
                        panel.postMessage({
                            type: 'update',
                            issues,
                            sql: statements,
                            schemaError: formatGatewayError(error)
                        });
                    }
                });
        }, 500);
    }

    private async preview(
        panel: JobDesignerPanel,
        profile: ClusterProfile,
        state: PanelState,
        input: unknown,
        target: 'source' | number
    ): Promise<void> {
        state.preview?.abort();
        const revision = ++state.previewRevision;
        const controller = new AbortController();
        state.preview = controller;
        const startedAt = Date.now();
        let timedOut = false;
        let filesystemSource = false;
        let timeout: NodeJS.Timeout | undefined;
        let session: Awaited<ReturnType<SqlGatewayClient['openSession']>> | undefined;
        let client: SqlGatewayClient | undefined;
        panel.postMessage({ type: 'previewStarted', target });

        try {
            if (!profile.sqlGatewayUrl) {
                throw new Error('This cluster profile does not have a SQL Gateway URL.');
            }
            if (!isJobSpec(input)) {
                throw new Error('The preview requires a valid JobSpec.');
            }
            filesystemSource = input.source.type === 'filesystem';
            const configuration = vscode.workspace.getConfiguration('flink.preview');
            const maxRows = settingInteger(
                configuration.get<number>('maxRows', 50),
                'flink.preview.maxRows',
                1
            );
            const timeoutSeconds = settingInteger(
                configuration.get<number>('timeoutSeconds', 30),
                'flink.preview.timeoutSeconds',
                1
            );
            const compiled = compilePreview(input, target, { maxRows });
            if (!isCompilerGeneratedPreviewSequence(compiled.statements)) {
                throw new Error('The preview compiler produced a statement outside the safe preview-only sequence.');
            }
            client = new SqlGatewayClient(profile);
            timeout = setTimeout(() => {
                timedOut = true;
                controller.abort();
            }, timeoutSeconds * 1000);
            session = await client.openSession();
            let result: Awaited<ReturnType<SqlGatewayClient['runStatement']>> | undefined;
            for (const [index, statement] of compiled.statements.entries()) {
                if (controller.signal.aborted) {
                    throw new Error('Preview cancelled.');
                }
                const statementStartedAt = Date.now();
                try {
                    result = await client.runStatement(session, statement, {
                        maxRows: index === compiled.statements.length - 1 ? maxRows : 1_000,
                        signal: controller.signal
                    });
                    if (result.status !== 'FINISHED') {
                        throw new Error(
                            result.error ?? `SQL Gateway operation ended with status ${result.status}.`
                        );
                    }
                    this.logPreviewStatement(
                        target,
                        classifyStatement(statement),
                        Date.now() - statementStartedAt
                    );
                } catch (error) {
                    this.logPreviewError(
                        target,
                        classifyStatement(statement),
                        Date.now() - statementStartedAt,
                        errorText(error)
                    );
                    throw error;
                }
            }
            if (!result) {
                throw new Error('The compiled preview did not include a SELECT statement.');
            }
            if (controller.signal.aborted) {
                throw new Error('Preview cancelled.');
            }
            if (revision === state.previewRevision) {
                panel.postMessage({
                    type: 'previewResult',
                    target,
                    columns: compiled.columns,
                    rows: result.rows,
                    rowCount: result.rows.length,
                    truncated: result.truncated,
                    durationMs: Date.now() - startedAt,
                    changelogMode: compiled.changelogMode,
                    ...(compiled.keyColumns ? { keyColumns: compiled.keyColumns } : {})
                });
            }
        } catch (error) {
            const message = timedOut
                ? filesystemSource
                    ? `Preview timed out after ${vscode.workspace.getConfiguration('flink.preview').get<number>('timeoutSeconds', 30)} seconds. The folder may be empty, or the source path may be wrong inside the Flink container.`
                    : `Preview timed out after ${vscode.workspace.getConfiguration('flink.preview').get<number>('timeoutSeconds', 30)} seconds.`
                : controller.signal.aborted
                    ? 'Preview cancelled.'
                    : formatGatewayError(error);
            if (revision === state.previewRevision) {
                panel.postMessage({
                    type: 'previewResult',
                    target,
                    columns: [],
                    rows: [],
                    rowCount: 0,
                    truncated: false,
                    durationMs: Date.now() - startedAt,
                    changelogMode: 'APPEND-ONLY',
                    error: message
                });
            }
            this.output.appendLine(
                `${new Date().toISOString()} preview ${targetLabel(target)} error after ${Date.now() - startedAt}ms: ${message}`
            );
        } finally {
            if (timeout) {
                clearTimeout(timeout);
            }
            if (session && client) {
                try {
                    await client.closeSession(session.sessionHandle);
                } catch (error) {
                    const message = `Could not close the preview SQL Gateway session: ${errorText(error)}`;
                    this.output.appendLine(`${new Date().toISOString()} preview ${targetLabel(target)} session close error: ${message}`);
                    if (revision === state.previewRevision) {
                        panel.postMessage({ type: 'previewWarning', target, message });
                    }
                }
            }
            if (revision === state.previewRevision) {
                state.preview = undefined;
                panel.postMessage({ type: 'previewFinished', target });
            }
        }
    }

    private async inferSample(panel: JobDesignerPanel): Promise<void> {
        const selected = await vscode.window.showOpenDialog({
            canSelectMany: false,
            openLabel: 'Infer Source Schema',
            filters: { 'Sample files': ['jsonl', 'ndjson', 'json', 'csv', 'txt'] }
        });
        const uri = selected?.[0];
        if (!uri) {
            return;
        }
        if (uri.scheme !== 'file') {
            panel.postMessage({
                type: 'inferenceError',
                message: 'Choose a local file; sample contents are not sent to Flink.'
            });
            return;
        }
        try {
            let format: SampleFormat;
            const extension = uri.fsPath.split('.').pop()?.toLowerCase();
            if (extension === 'csv') {
                format = 'csv';
            } else if (['json', 'jsonl', 'ndjson'].includes(extension ?? '')) {
                format = 'json';
            } else {
                const choice = await vscode.window.showQuickPick([
                    { label: 'JSON Lines', format: 'json' as const },
                    { label: 'CSV', format: 'csv' as const }
                ], { placeHolder: 'Choose the sample file format' });
                if (!choice) {
                    return;
                }
                format = choice.format;
            }
            const sample = await readLocalSample(uri.fsPath);
            const inference = inferSample(sample.text, format, SAMPLE_MAX_BYTES, sample.truncated);
            panel.postMessage({
                type: 'inference',
                inference,
                fileName: basename(uri.fsPath)
            });
        } catch (error) {
            panel.postMessage({
                type: 'inferenceError',
                message: errorText(error)
            });
        }
    }

    private logPreviewStatement(target: 'source' | number, kind: string, durationMs: number): void {
        this.output.appendLine(
            `${new Date().toISOString()} preview ${targetLabel(target)} ${kind}: ${durationMs}ms`
        );
    }

    private logPreviewError(
        target: 'source' | number,
        kind: string,
        durationMs: number,
        message: string
    ): void {
        this.output.appendLine(
            `${new Date().toISOString()} preview ${targetLabel(target)} ${kind} error after ${durationMs}ms: ${message}`
        );
    }

    private async validateJob(panel: JobDesignerPanel, profile: ClusterProfile, input: unknown): Promise<void> {
        const spec = this.requireValidSpec(input);
        if (!spec) {
            panel.postMessage({
                type: 'result',
                action: 'validate',
                message: 'Fix the validation errors before validating with Flink.'
            });
            return;
        }
        if (profile.readOnly) {
            panel.postMessage({
                type: 'result',
                action: 'validate',
                message: 'Gateway validation requires temporary DDL, which is not allowed for this read-only profile.'
            });
            return;
        }

        const client = new SqlGatewayClient(profile);
        const schema = await resolveJobSchema(client, spec);
        const compiled = compileSpec(spec, schema);
        const session = await client.openSession();
        let validationMessage = 'Flink validation did not complete.';
        try {
            for (const statement of compiled.statements.slice(0, -1)) {
                await requireFinished(client, session, statement);
            }
            const result = await client.runStatement(session, `EXPLAIN ${compiled.statements.at(-1)}`, {
                maxRows: 10_000
            });
            if (result.status !== 'FINISHED' || !result.rows.length) {
                throw new Error(`EXPLAIN INSERT finished with status ${result.status} and returned no plan.`);
            }
            validationMessage = 'Flink validation succeeded. EXPLAIN INSERT returned a plan.';
        } catch (error) {
            validationMessage = formatGatewayError(error);
        } finally {
            try {
                await client.closeSession(session.sessionHandle);
            } catch (error) {
                validationMessage += `\nCould not close the SQL Gateway session: ${errorText(error)}`;
            }
        }
        panel.postMessage({ type: 'result', action: 'validate', message: validationMessage });
    }

    private async submitJob(panel: JobDesignerPanel, profile: ClusterProfile, input: unknown): Promise<void> {
        const spec = this.requireValidSpec(input);
        if (!spec) {
            panel.postMessage({
                type: 'result',
                action: 'submit',
                message: 'Fix the validation errors before submitting.'
            });
            return;
        }
        if (profile.readOnly) {
            panel.postMessage({
                type: 'result',
                action: 'submit',
                message: 'This cluster profile is read-only. Job submission is disabled.'
            });
            await vscode.window.showErrorMessage('This cluster profile is read-only. Job submission is disabled.');
            return;
        }
        const confirmation = await vscode.window.showWarningMessage(
            `Submit Flink job "${spec.name}" to ${profile.name}?`,
            { modal: true },
            'Submit Job'
        );
        if (confirmation !== 'Submit Job') {
            panel.postMessage({ type: 'result', action: 'submit', message: 'Submission cancelled. No statements were sent.' });
            return;
        }

        const client = new SqlGatewayClient(profile);
        let session: Awaited<ReturnType<SqlGatewayClient['openSession']>> | undefined;
        let jobID: string | undefined;
        try {
            const schema = await resolveJobSchema(client, spec);
            const compiled = compileSpec(spec, schema);
            session = await client.openSession();
            for (let index = 0; index < compiled.statements.length; index++) {
                const statement = compiled.statements[index];
                try {
                    const result = await client.runStatement(session, statement, { maxRows: 100 });
                    if (result.status !== 'FINISHED') {
                        throw new Error(`Statement ended with status ${result.status}.`);
                    }
                    this.logJobStatement(spec.name, index, statement, result.status.toLowerCase());
                    if (index === compiled.statements.length - 1) {
                        jobID = result.jobID;
                    }
                } catch (error) {
                    this.logJobStatement(spec.name, index, statement, 'failed');
                    for (let skipped = index + 1; skipped < compiled.statements.length; skipped++) {
                        this.logJobStatement(
                            spec.name,
                            skipped,
                            compiled.statements[skipped],
                            'not run (previous statement failed)'
                        );
                    }
                    throw new Error(`Statement ${index + 1} failed: ${errorText(error)}`);
                }
            }
            if (!jobID) {
                throw new Error('Flink did not return a job ID for the submitted INSERT.');
            }
            panel.postMessage({
                type: 'result',
                action: 'submit',
                message: `Job submitted successfully. Job ID: ${jobID}`,
                jobID
            });
            this.refreshTree();
        } catch (error) {
            const message = formatGatewayError(error);
            this.output.appendLine(`${new Date().toISOString()} ${spec.name}: submission failed`);
            panel.postMessage({ type: 'result', action: 'submit', message });
            await vscode.window.showErrorMessage(`Could not submit Flink job: ${message}`);
        } finally {
            if (session) {
                try {
                    await client.closeSession(session.sessionHandle);
                } catch (error) {
                    const warning = `Could not close the SQL Gateway session: ${errorText(error)}`;
                    this.output.appendLine(`${new Date().toISOString()} ${spec.name}: SQL Gateway session close failed`);
                    panel.postMessage({
                        type: 'result',
                        action: 'submit',
                        message: `${jobID ? `Job submitted successfully. Job ID: ${jobID}\n` : ''}${warning}`,
                        ...(jobID ? { jobID } : {})
                    });
                    await vscode.window.showWarningMessage(warning);
                }
            }
        }
    }

    private async saveSpec(input: unknown): Promise<void> {
        const errors = validateSpec(input).filter(issue => issue.severity === 'error');
        if (errors.length || !isJobSpec(input)) {
            await vscode.window.showErrorMessage(`Could not save invalid job spec:\n${formatIssues(errors)}`);
            return;
        }
        const workspaceUri = vscode.workspace.workspaceFolders?.[0]?.uri;
        const uri = await vscode.window.showSaveDialog({
            ...(workspaceUri ? { defaultUri: vscode.Uri.joinPath(workspaceUri, 'job.flinkjob.json') } : {}),
            filters: { 'Flink Job Spec': ['flinkjob.json'] }
        });
        if (!uri) {
            return;
        }
        const target = uri.fsPath.toLowerCase().endsWith('.flinkjob.json')
            ? uri
            : vscode.Uri.file(`${uri.fsPath}.flinkjob.json`);
        try {
            await vscode.workspace.fs.writeFile(target, Buffer.from(`${JSON.stringify(input, null, 2)}\n`, 'utf8'));
            await vscode.window.showInformationMessage(`Saved Flink job spec to ${target.fsPath}.`);
        } catch (error) {
            await vscode.window.showErrorMessage(`Could not save job spec: ${errorText(error)}`);
        }
    }

    private async loadSpec(
        panel: JobDesignerPanel,
        profile: ClusterProfile,
        state: PanelState
    ): Promise<void> {
        const selected = await vscode.window.showOpenDialog({
            canSelectMany: false,
            openLabel: 'Load Job Spec',
            filters: { 'Flink Job Spec': ['flinkjob.json'] }
        });
        if (!selected?.[0]) {
            return;
        }
        try {
            const text = Buffer.from(await vscode.workspace.fs.readFile(selected[0])).toString('utf8');
            const spec: unknown = JSON.parse(text);
            const errors = validateSpec(spec).filter(issue => issue.severity === 'error');
            if (errors.length || !isJobSpec(spec)) {
                panel.postMessage({
                    type: 'result',
                    action: 'validate',
                    message: `Could not load spec:\n${formatIssues(errors)}`
                });
                return;
            }
            state.spec = spec;
            panel.showInitialState(spec, profile.readOnly ?? false);
            await this.updateSpec(panel, profile, state, spec);
        } catch (error) {
            await vscode.window.showErrorMessage(`Could not load job spec: ${errorText(error)}`);
        }
    }

    private async copySql(input: unknown): Promise<void> {
        const spec = this.requireValidSpec(input);
        if (!spec) {
            await vscode.window.showErrorMessage('Fix the validation errors before copying SQL.');
            return;
        }
        try {
            await vscode.env.clipboard.writeText(compileSpec(spec).statements.join(';\n\n'));
            await vscode.window.showInformationMessage('Generated SQL copied to the clipboard.');
        } catch (error) {
            await vscode.window.showErrorMessage(`Could not copy SQL: ${errorText(error)}`);
        }
    }

    private requireValidSpec(input: unknown): JobSpec | undefined {
        const errors = validateSpec(input).filter(issue => issue.severity === 'error');
        return !errors.length && isJobSpec(input) ? input : undefined;
    }

    private logJobStatement(name: string, index: number, statement: string, outcome: string): void {
        this.output.appendLine(
            `${new Date().toISOString()} ${name} statement ${index + 1} (${classifyStatement(statement)}): ${outcome} | ${redactSqlLiterals(statement)}`
        );
    }

    private async chooseProfile(): Promise<ClusterProfile | undefined> {
        const profiles = this.store.getAll().filter(profile => Boolean(profile.sqlGatewayUrl));
        if (!profiles.length) {
            const action = await vscode.window.showInformationMessage(
                'No Flink cluster has a SQL Gateway URL configured.',
                'Add Cluster'
            );
            if (action === 'Add Cluster') {
                await vscode.commands.executeCommand('ApacheFlink.AddCluster');
            }
            return undefined;
        }
        if (profiles.length === 1) {
            return profiles[0];
        }
        const selected = await vscode.window.showQuickPick(
            profiles.map(profile => ({ label: profile.name, description: profile.baseUrl, profile })),
            { placeHolder: 'Select a Flink SQL Gateway' }
        );
        return selected?.profile;
    }
}

function defaultSpec(): JobSpec {
    return {
        version: 1,
        name: 'new_flink_job',
        source: {
            type: 'datagen',
            rowsPerSecond: 1,
            fields: [{ name: 'line', type: 'STRING', kind: 'random' }]
        },
        steps: [],
        sink: { type: 'print' },
        settings: { checkpointIntervalSeconds: 10 }
    };
}

function longestValidPrefix(spec: JobSpec): JobSpec | undefined {
    for (let count = spec.steps.length; count >= 0; count--) {
        const candidate: JobSpec = {
            ...spec,
            name: 'schema_validation',
            steps: spec.steps.slice(0, count),
            sink: { type: 'print' },
            settings: {}
        };
        if (validateSpec(candidate).every(issue => issue.severity !== 'error')) {
            return candidate;
        }
    }
    return undefined;
}

function isJobSpec(value: unknown): value is JobSpec {
    return typeof value === 'object'
        && value !== null
        && 'version' in value
        && 'name' in value
        && 'source' in value
        && 'steps' in value
        && 'sink' in value
        && 'settings' in value;
}

async function requireFinished(
    client: SqlGatewayClient,
    session: Awaited<ReturnType<SqlGatewayClient['openSession']>>,
    statement: string
): Promise<void> {
    const result = await client.runStatement(session, statement, { maxRows: 10_000 });
    if (result.status !== 'FINISHED') {
        throw new Error(`Statement ended with status ${result.status}.`);
    }
}

function errorText(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
}

function settingInteger(value: number, name: string, minimum: number): number {
    if (!Number.isSafeInteger(value) || value < minimum) {
        throw new Error(`${name} must be an integer greater than or equal to ${minimum}.`);
    }
    return value;
}

function targetLabel(target: 'source' | number): string {
    return target === 'source' ? 'source' : `step ${target + 1}`;
}

async function readLocalSample(path: string): Promise<{ text: string; truncated: boolean }> {
    const file = await open(path, 'r');
    try {
        const stats = await file.stat();
        const buffer = Buffer.alloc(SAMPLE_MAX_BYTES);
        let length = 0;
        let lineCount = 0;
        let reachedLineLimit = false;
        while (length < SAMPLE_MAX_BYTES && !reachedLineLimit) {
            const readLength = Math.min(8 * 1024, SAMPLE_MAX_BYTES - length);
            const { bytesRead } = await file.read(buffer, length, readLength, length);
            if (!bytesRead) {
                break;
            }
            const chunkStart = length;
            length += bytesRead;
            for (let index = chunkStart; index < length; index++) {
                if (buffer[index] === 0x0a && ++lineCount === SAMPLE_MAX_LINES) {
                    length = index + 1;
                    reachedLineLimit = true;
                    break;
                }
            }
        }
        const truncated = stats.size > length;
        if (truncated && !reachedLineLimit) {
            const lastNewline = buffer.lastIndexOf(0x0a, length - 1);
            length = lastNewline + 1;
        }
        return {
            text: buffer.subarray(0, length).toString('utf8'),
            truncated
        };
    } finally {
        await file.close();
    }
}

function formatGatewayError(error: unknown): string {
    const message = error instanceof FlinkApiError ? error.message : errorText(error);
    if (/no factory|factory for identifier|could not find.*connector|connector.*not found/i.test(message)) {
        return `${message}\nHint: add the required Flink connector JAR to the cluster and SQL Gateway.`;
    }
    return message;
}

function formatIssues(issues: ValidationIssue[]): string {
    return issues.map(issue => `${issue.path}: ${issue.message}`).join('\n') || 'The spec is invalid.';
}
