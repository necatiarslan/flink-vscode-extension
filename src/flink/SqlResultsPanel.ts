import * as vscode from 'vscode';
import { SqlGatewayStatementResult } from './types';

export interface SqlStatementPanelResult {
    statement: string;
    status: string;
    kind: string;
    durationMs?: number;
    result?: SqlGatewayStatementResult;
    error?: string;
}

export class SqlResultsPanel {
    public static open(
        title: string,
        profileId: string,
        statements: SqlStatementPanelResult[]
    ): void {
        const panel = vscode.window.createWebviewPanel(
            'flinkSqlResults',
            title,
            vscode.ViewColumn.One,
            {
                enableScripts: false,
                enableCommandUris: ['flink.openSqlJob'],
                localResourceRoots: []
            }
        );
        panel.webview.html = renderHtml(title, profileId, statements);
    }
}

function renderHtml(title: string, profileId: string, statements: SqlStatementPanelResult[]): string {
    return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline';">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${escapeHtml(title)}</title>
<style>
body{padding:24px;color:var(--vscode-foreground);background:var(--vscode-editor-background);font-family:var(--vscode-font-family);font-size:var(--vscode-font-size)}
h1{font-size:1.55em;font-weight:500;margin:0 0 18px;overflow-wrap:anywhere}
h2{font-size:1.12em;font-weight:500;margin:0 0 10px}
.section{border:1px solid var(--vscode-panel-border);border-radius:4px;padding:14px;background:var(--vscode-sideBar-background);margin:12px 0;overflow:auto}
.meta{display:flex;flex-wrap:wrap;gap:14px;margin-bottom:12px;color:var(--vscode-descriptionForeground)}
.error{color:var(--vscode-errorForeground);white-space:pre-wrap;overflow-wrap:anywhere;margin:10px 0}
.statement{white-space:pre-wrap;overflow-wrap:anywhere;font-family:var(--vscode-editor-font-family);margin:12px 0}
.muted{color:var(--vscode-descriptionForeground)}
table{width:100%;border-collapse:collapse}
th,td{text-align:left;padding:8px 10px;border-bottom:1px solid var(--vscode-panel-border);vertical-align:top;overflow-wrap:anywhere}
th{color:var(--vscode-descriptionForeground);font-weight:500}
tr:last-child td{border-bottom:0}
.button{display:inline-block;padding:6px 10px;border-radius:2px;background:var(--vscode-button-background);color:var(--vscode-button-foreground);text-decoration:none}
.button:hover{background:var(--vscode-button-hoverBackground)}
</style>
</head>
<body>
<main><h1>${escapeHtml(title)}</h1>${statements.map((statement, index) =>
    renderStatement(statement, index, profileId)
).join('')}</main>
</body>
</html>`;
}

function renderStatement(
    statement: SqlStatementPanelResult,
    index: number,
    profileId: string
): string {
    const result = statement.result;
    const rows = result?.rows ?? [];
    const columns = result?.columns.length ? result.columns : inferredColumns(rows);
    const hasKinds = rows.some(row => typeof row.kind === 'string' || typeof row.rowKind === 'string');
    const headers = [...(hasKinds ? ['kind'] : []), ...columns];
    const table = headers.length
        ? `<table><thead><tr>${headers.map(header => `<th>${escapeHtml(header)}</th>`).join('')}</tr></thead><tbody>${
            rows.map(row => renderRow(row, columns, hasKinds)).join('')
        }</tbody></table>`
        : `<p class="muted">${rows.length ? 'Result rows have no column metadata.' : 'No rows returned.'}</p>`;
    const job = statement.kind === 'insert' && result?.jobID
        ? `<p><a class="button" href="${escapeHtml(jobCommandUri(profileId, result.jobID))}">Open job ${escapeHtml(result.jobID)}</a></p>`
        : '';
    const truncation = result?.truncated
        ? `<p class="muted">Truncated at ${result.rows.length} rows.</p>`
        : '';

    return `<section class="section">
<h2>Statement ${index + 1} - ${escapeHtml(statement.status)}</h2>
<div class="meta"><span>Kind: ${escapeHtml(statement.kind)}</span>${
        `<span>Duration: ${escapeHtml(String(result?.durationMs ?? statement.durationMs ?? 0))} ms</span>`
    }</div>
<pre class="statement">${escapeHtml(statement.statement)}</pre>
${statement.error ? `<p class="error">${escapeHtml(statement.error)}</p>` : ''}
${job}${table}${truncation}
</section>`;
}

function inferredColumns(rows: SqlGatewayStatementResult['rows']): string[] {
    const count = rows.reduce((maximum, row) => {
        const rowCount = Array.isArray(row.fields)
            ? row.fields.length
            : Object.keys(row).filter(key => key !== 'kind' && key !== 'rowKind').length;
        return Math.max(maximum, rowCount);
    }, 0);
    return Array.from({ length: count }, (_value, index) => `Column ${index + 1}`);
}

function renderRow(
    row: SqlGatewayStatementResult['rows'][number],
    columns: string[],
    hasKinds: boolean
): string {
    const values = Array.isArray(row.fields)
        ? row.fields
        : columns.map(column => row[column]);
    const cells = [
        ...(hasKinds ? [row.kind ?? row.rowKind ?? ''] : []),
        ...columns.map((_, index) => values[index])
    ];
    return `<tr>${cells.map(cell => `<td>${escapeHtml(formatValue(cell))}</td>`).join('')}</tr>`;
}

function formatValue(value: unknown): string {
    if (value === null) {
        return 'NULL';
    }
    if (value === undefined) {
        return '';
    }
    return typeof value === 'string' ? value : JSON.stringify(value);
}

function jobCommandUri(profileId: string, jobId: string): string {
    return `command:flink.openSqlJob?${encodeURIComponent(JSON.stringify([profileId, jobId]))}`;
}

function escapeHtml(value: string): string {
    return value.replace(/[&<>"']/g, character => {
        switch (character) {
            case '&': return '&amp;';
            case '<': return '&lt;';
            case '>': return '&gt;';
            case '"': return '&quot;';
            default: return '&#39;';
        }
    });
}
