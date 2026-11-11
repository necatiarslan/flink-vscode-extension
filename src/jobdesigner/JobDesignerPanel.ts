import { randomBytes } from 'crypto';
import * as vscode from 'vscode';
import type { JobSpec } from './spec';

export type JobDesignerWebviewMessage =
    | { type: 'ready' }
    | { type: 'change'; spec: unknown }
    | { type: 'validate' | 'submit' | 'save' | 'copySql'; spec: unknown }
    | { type: 'preview'; spec: unknown; target: 'source' | number }
    | { type: 'cancelPreview' }
    | { type: 'inferSample' }
    | { type: 'load' }
    | { type: 'openJob'; jobID: string };

export class JobDesignerPanel {
    public readonly panel: vscode.WebviewPanel;

    constructor(
        context: vscode.ExtensionContext,
        title: string,
        private readonly onMessage: (message: JobDesignerWebviewMessage) => void
    ) {
        const scriptRoot = vscode.Uri.joinPath(context.extensionUri, 'out', 'webview');
        this.panel = vscode.window.createWebviewPanel(
            'flinkJobDesigner',
            title,
            vscode.ViewColumn.One,
            {
                enableScripts: true,
                localResourceRoots: [scriptRoot]
            }
        );
        const nonce = randomBytes(16).toString('base64');
        const script = this.panel.webview.asWebviewUri(
            vscode.Uri.joinPath(scriptRoot, 'jobdesigner', 'webview', 'main.js')
        );
        this.panel.webview.html = renderHtml(this.panel.webview, title, nonce, script);
        this.panel.webview.onDidReceiveMessage(message => {
            if (isMessage(message)) {
                this.onMessage(message);
            }
        });
    }

    public showInitialState(spec: JobSpec, readOnly: boolean): void {
        void this.panel.webview.postMessage({ type: 'initialize', spec, readOnly });
    }

    public postMessage(message: Record<string, unknown>): void {
        void this.panel.webview.postMessage(message);
    }
}

function renderHtml(webview: vscode.Webview, title: string, nonce: string, script: vscode.Uri): string {
    return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src 'nonce-${nonce}' ${webview.cspSource}; style-src 'nonce-${nonce}';">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${escapeHtml(title)}</title>
<style nonce="${nonce}">
body{padding:18px;color:var(--vscode-foreground);background:var(--vscode-editor-background);font-family:var(--vscode-font-family);font-size:var(--vscode-font-size)}
h1{font-size:1.4em;font-weight:500;margin:0 0 16px}
h2{font-size:1.1em;font-weight:500;margin:18px 0 8px}
.builder{min-width:0}
.card{border:1px solid var(--vscode-panel-border);border-radius:4px;padding:12px;background:var(--vscode-sideBar-background);margin:8px 0}
.field{display:flex;flex-direction:column;gap:4px;margin:8px 0}
.fields{margin-top:10px}
.step-header,.actions{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.step-header h2{flex:1;margin:0}
.step-header select{flex:1 1 180px;width:auto}
.builder-tabs{display:flex;gap:2px;overflow-x:auto;border-bottom:1px solid var(--vscode-panel-border);margin:8px 0}
.builder-tab{flex:0 0 auto;padding:8px 12px;color:var(--vscode-foreground);background:transparent;border-bottom:2px solid transparent;white-space:nowrap}
.builder-tab:hover{background:var(--vscode-toolbar-hoverBackground)}
.builder-tab[aria-selected=true]{color:var(--vscode-tab-activeForeground);background:var(--vscode-tab-activeBackground);border-bottom-color:var(--vscode-focusBorder)}
.builder-tab:focus-visible{outline:1px solid var(--vscode-focusBorder);outline-offset:-2px}
.builder-tab-panel{min-width:0;outline:none}
.add-step-control{display:flex;justify-content:flex-end;margin:8px 0}
.add-step-control select{max-width:240px}
input,select,textarea{box-sizing:border-box;min-width:0;width:100%;padding:5px;color:var(--vscode-input-foreground);background:var(--vscode-input-background);border:1px solid var(--vscode-input-border);font:inherit}
input[type=checkbox]{width:auto}
textarea{min-height:75px;font-family:var(--vscode-editor-font-family)}
button{padding:5px 9px;color:var(--vscode-button-foreground);background:var(--vscode-button-background);border:0;border-radius:2px;cursor:pointer}
button:hover{background:var(--vscode-button-hoverBackground)}
button:disabled{opacity:.55;cursor:default}
.sql{white-space:pre-wrap;overflow-wrap:anywhere;font-family:var(--vscode-editor-font-family);border:1px solid var(--vscode-panel-border);padding:12px;max-height:55vh;overflow:auto}
table{width:100%;border-collapse:collapse}
th,td{text-align:left;padding:7px;border-bottom:1px solid var(--vscode-panel-border);overflow-wrap:anywhere}
svg.timeline{display:block;width:min(100%,360px);height:92px;margin:12px auto;color:var(--vscode-foreground);background:var(--vscode-editor-background);border:1px solid var(--vscode-panel-border)}
svg.timeline rect{fill:var(--vscode-button-background);stroke:var(--vscode-focusBorder);stroke-width:1}
svg.timeline line{stroke:var(--vscode-descriptionForeground);stroke-width:2}
svg.timeline text{fill:var(--vscode-foreground);font:12px var(--vscode-font-family)}
.error,.warning{color:var(--vscode-errorForeground)}
.warning{color:var(--vscode-editorWarning-foreground)}
.inline-issue{color:var(--vscode-errorForeground);font-size:.9em}
.muted{color:var(--vscode-descriptionForeground)}
.result{white-space:pre-wrap;color:var(--vscode-descriptionForeground)}
</style>
</head>
<body>
<h1>${escapeHtml(title)}</h1>
<div id="app"></div>
<script nonce="${nonce}" type="module" src="${webview.asWebviewUri(script)}"></script>
</body>
</html>`;
}

function isMessage(value: unknown): value is JobDesignerWebviewMessage {
    if (typeof value !== 'object' || value === null || !('type' in value)) {
        return false;
    }
    const type = (value as { type?: unknown }).type;
    return [
        'ready',
        'change',
        'validate',
        'submit',
        'save',
        'load',
        'copySql',
        'openJob',
        'preview',
        'cancelPreview',
        'inferSample'
    ].includes(String(type));
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
