import * as vscode from 'vscode';
import { basename, extname } from 'path';
import { ClusterStore } from './ClusterStore';
import { FlinkClient } from './FlinkClient';
import { FlinkDetailPanel } from './FlinkDetailPanel';
import { SqlGatewayClient } from './SqlGatewayClient';
import { SqlResultsPanel, SqlStatementPanelResult } from './SqlResultsPanel';
import {
    classifyStatement,
    isReadOnlySqlAllowed,
    redactSqlLiterals,
    splitSqlStatements,
    SqlStatementKind
} from './sqlStatements';
import { ClusterProfile, SqlGatewayOperationStatus, SqlGatewayStatementResult } from './types';

export class SqlRunController {
    private readonly output = vscode.window.createOutputChannel('Flink');

    constructor(
        context: vscode.ExtensionContext,
        private readonly store: ClusterStore
    ) {
        context.subscriptions.push(
            this.output,
            vscode.commands.registerCommand('flink.runSql', (editor?: vscode.TextEditor) => this.runSql(editor)),
            vscode.commands.registerCommand(
                'flink.openSqlJob',
                (profileId: unknown, jobId: unknown) => this.openJob(profileId, jobId)
            )
        );
    }

    private async runSql(editorArgument?: vscode.TextEditor): Promise<void> {
        const editor = editorArgument ?? vscode.window.activeTextEditor;
        if (!editor || extname(editor.document.fileName).toLowerCase() !== '.sql') {
            await vscode.window.showWarningMessage('Open a .sql file to run SQL.');
            return;
        }
        const source = editor.document.getText(editor.selection.isEmpty ? undefined : editor.selection);
        const statements = splitSqlStatements(source);
        if (!statements.length) {
            await vscode.window.showInformationMessage('No SQL statements were found.');
            return;
        }

        const profile = await this.chooseProfile();
        if (!profile) {
            return;
        }
        if (profile.readOnly) {
            const blocked = statements.find(statement => !isReadOnlySqlAllowed(classifyStatement(statement)));
            if (blocked) {
                await vscode.window.showErrorMessage(
                    `This cluster profile is read-only. Only query, show, explain, and set statements are allowed; blocked: ${preview(blocked)}`
                );
                return;
            }
        }

        const fileName = basename(editor.document.fileName);
        const configuration = vscode.workspace.getConfiguration('flink.sql');
        let maxRows: number;
        let statementTimeoutSeconds: number;
        try {
            maxRows = settingInteger(configuration.get<number>('maxRows', 100), 'flink.sql.maxRows', 0);
            statementTimeoutSeconds = settingInteger(
                configuration.get<number>('statementTimeoutSeconds', 60),
                'flink.sql.statementTimeoutSeconds',
                1
            );
        } catch (error) {
            await vscode.window.showErrorMessage(`Invalid Flink SQL setting: ${errorText(error)}`);
            return;
        }
        const results: SqlStatementPanelResult[] = [];
        this.output.show(true);

        try {
            await vscode.window.withProgress({
                location: vscode.ProgressLocation.Notification,
                title: `Running SQL in ${fileName}`,
                cancellable: true
            }, async (_progress, cancellationToken) => {
                const controller = new AbortController();
                const cancellation = cancellationToken.onCancellationRequested(() => controller.abort());
                let session: Awaited<ReturnType<SqlGatewayClient['openSession']>> | undefined;
                let stopped = false;
                try {
                    const client = new SqlGatewayClient(profile);
                    session = await client.openSession();
                    for (let index = 0; index < statements.length; index++) {
                        const statement = statements[index];
                        const kind = classifyStatement(statement);
                        if (stopped || cancellationToken.isCancellationRequested) {
                            results.push({
                                statement,
                                kind,
                                status: 'SKIPPED',
                                durationMs: 0,
                                error: 'Statement was not run because execution stopped.'
                            });
                            this.logStatement(fileName, index, kind, statement, 'not run (execution stopped)');
                            continue;
                        }

                        if (!await this.confirmStatement(kind, statement)) {
                            const result = cancelledResult(statement, kind);
                            results.push(result);
                            this.logStatement(fileName, index, kind, statement, 'cancelled by user');
                            stopped = true;
                            continue;
                        }

                        const statementStartedAt = Date.now();
                        try {
                            const result = await client.runStatement(session, statement, {
                                maxRows,
                                pollingIntervalMs: 250,
                                executionConfig: { 'pipeline.name': fileName },
                                executionTimeout: statementTimeoutSeconds,
                                signal: controller.signal
                            });
                            const outcome = this.operationOutcome(result.status, result.truncated);
                            results.push({ statement, kind, status: result.status, result });
                            this.logStatement(fileName, index, kind, statement, outcome);
                            if (result.status !== 'FINISHED' && result.status !== 'CANCELED') {
                                results[results.length - 1].error = `SQL Gateway operation ended with status ${result.status}.`;
                                stopped = true;
                            } else if (result.status === 'CANCELED' || cancellationToken.isCancellationRequested) {
                                stopped = true;
                            }
                        } catch (error) {
                            const errorMessage = errorText(error);
                            results.push({
                                statement,
                                kind,
                                status: 'ERROR',
                                durationMs: Date.now() - statementStartedAt,
                                error: errorMessage
                            });
                            this.logStatement(fileName, index, kind, statement, 'failed');
                            await vscode.window.showErrorMessage(`SQL statement ${index + 1} failed: ${errorMessage}`);
                            stopped = true;
                        }
                    }
                } finally {
                    cancellation.dispose();
                    if (session) {
                        try {
                            await new SqlGatewayClient(profile).closeSession(session.sessionHandle);
                        } catch (error) {
                            await vscode.window.showErrorMessage(`Could not close the SQL Gateway session: ${errorText(error)}`);
                        }
                    }
                }
            });
        } catch (error) {
            await vscode.window.showErrorMessage(`Could not run SQL: ${errorText(error)}`);
            return;
        }

        SqlResultsPanel.open(`SQL Results: ${fileName}`, profile.id, results);
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
            profiles.map(profile => ({ label: profile.name, profile })),
            { placeHolder: 'Select a Flink SQL Gateway' }
        );
        return selected?.profile;
    }

    private async confirmStatement(kind: SqlStatementKind, statement: string): Promise<boolean> {
        const normalized = firstKeyword(statement);
        if (kind === 'insert') {
            return this.confirm(`Run this INSERT statement?\n\n${preview(statement)}`, 'Run INSERT');
        }
        if (normalized === 'DROP' || normalized === 'ALTER') {
            return this.confirm(`Run this ${normalized} statement?\n\n${preview(statement)}`, `Run ${normalized}`);
        }
        return true;
    }

    private async confirm(message: string, action: string): Promise<boolean> {
        return await vscode.window.showWarningMessage(message, { modal: true }, action) === action;
    }

    private operationOutcome(status: SqlGatewayOperationStatus['status'], truncated: boolean): string {
        return truncated ? `finished (truncated at configured row limit; status ${status})` : status.toLowerCase();
    }

    private logStatement(
        fileName: string,
        index: number,
        kind: SqlStatementKind,
        statement: string,
        outcome: string
    ): void {
        this.output.appendLine(
            `${new Date().toISOString()} ${fileName} statement ${index + 1} (${kind}): ${outcome} | ${redactSqlLiterals(statement)}`
        );
    }

    private async openJob(profileId: unknown, jobId: unknown): Promise<void> {
        if (typeof profileId !== 'string' || typeof jobId !== 'string' || !/^[0-9a-f]{32}$/i.test(jobId)) {
            await vscode.window.showErrorMessage('The SQL results panel provided an invalid job reference.');
            return;
        }
        const profile = this.store.get(profileId);
        if (!profile) {
            await vscode.window.showErrorMessage('The cluster for this SQL result no longer exists.');
            return;
        }
        try {
            const path = `/jobs/${encodeURIComponent(jobId)}`;
            const job = await new FlinkClient(profile).get<unknown>(path);
            await FlinkDetailPanel.open(profile.id, path, 'Job Overview', 'job', job);
        } catch (error) {
            await vscode.window.showErrorMessage(`Could not open Flink job ${jobId}: ${errorText(error)}`);
        }
    }
}

function cancelledResult(statement: string, kind: SqlStatementKind): SqlStatementPanelResult {
    return {
        statement,
        kind,
        status: 'CANCELED',
        durationMs: 0,
        error: 'Statement was not run because confirmation was declined.'
    };
}

function settingInteger(value: number, name: string, minimum: number): number {
    if (!Number.isInteger(value) || value < minimum) {
        throw new Error(`${name} must be an integer greater than or equal to ${minimum}.`);
    }
    return value;
}

function preview(statement: string): string {
    const normalized = statement.replace(/\s+/g, ' ').trim();
    return normalized.length > 240 ? `${normalized.slice(0, 237)}...` : normalized;
}

function firstKeyword(statement: string): string {
    return /^\s*(?:--[^\r\n]*(?:\r?\n|$)\s*|\/\*[\s\S]*?\*\/\s*)*([A-Za-z]+)/.exec(statement)?.[1]?.toUpperCase() ?? '';
}

function errorText(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
}
