import * as vscode from 'vscode';
import { randomBytes } from 'crypto';
import { FlinkClient } from './FlinkClient';
import { FlinkMetric } from './types';

export class MetricChartPanel {
    private static readonly panels = new Map<string, MetricChartPanel>();
    private readonly panel: vscode.WebviewPanel;
    private readonly disposables: vscode.Disposable[] = [];
    private timer: NodeJS.Timeout | undefined;
    private selectedMetric: string;
    private sampling = false;

    private constructor(
        private readonly key: string,
        private readonly client: FlinkClient,
        private readonly metricsPath: string,
        private readonly metrics: FlinkMetric[]
    ) {
        this.selectedMetric = metrics[0]?.id ?? '';
        this.panel = vscode.window.createWebviewPanel(
            'flinkMetrics',
            'Flink Metrics',
            vscode.ViewColumn.One,
            { enableScripts: true, localResourceRoots: [] }
        );
        this.panel.webview.html = this.getHtml();
        this.disposables.push(
            this.panel.onDidDispose(() => this.dispose()),
            this.panel.onDidChangeViewState(() => this.updatePolling()),
            this.panel.webview.onDidReceiveMessage(message => {
                if (typeof message?.metricId === 'string' && this.metrics.some(metric => metric.id === message.metricId)) {
                    this.selectedMetric = message.metricId;
                    void this.sample();
                }
            })
        );
        this.updatePolling();
    }

    public static open(clusterId: string, client: FlinkClient, metricsPath: string, metrics: FlinkMetric[]): void {
        const key = `${clusterId}:${metricsPath}`;
        this.panels.get(key)?.panel.reveal(vscode.ViewColumn.One);
        if (!this.panels.has(key)) {
            this.panels.set(key, new MetricChartPanel(key, client, metricsPath, metrics));
        }
    }

    private updatePolling(): void {
        if (!this.panel.visible) {
            if (this.timer) {
                clearInterval(this.timer);
                this.timer = undefined;
            }
            return;
        }
        if (!this.timer) {
            void this.sample();
            this.timer = setInterval(() => void this.sample(), 5_000);
        }
    }

    private async sample(): Promise<void> {
        if (!this.selectedMetric || !this.panel.visible || this.sampling) {
            return;
        }
        this.sampling = true;
        try {
            const query = new URLSearchParams({ get: this.selectedMetric });
            const result = await this.client.get<FlinkMetric[]>(`${this.metricsPath}?${query}`);
            const value = Number(result.find(metric => metric.id === this.selectedMetric)?.value);
            if (!Number.isFinite(value)) {
                throw new Error(`Metric "${this.selectedMetric}" did not return a numeric value.`);
            }
            await this.panel.webview.postMessage({ type: 'sample', metricId: this.selectedMetric, value });
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            await this.panel.webview.postMessage({ type: 'error', message });
        } finally {
            this.sampling = false;
        }
    }

    private getHtml(): string {
        const nonce = randomBytes(16).toString('base64');
        const metricsJson = JSON.stringify(this.metrics.map(metric => metric.id)).replace(/</g, '\\u003c');
        return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src 'nonce-${nonce}'; style-src 'nonce-${nonce}';">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Flink Metrics</title>
<style nonce="${nonce}">
body{font-family:var(--vscode-font-family);color:var(--vscode-foreground);padding:16px}
select{max-width:100%;padding:6px;color:var(--vscode-dropdown-foreground);background:var(--vscode-dropdown-background)}
#chart{width:100%;height:280px;margin-top:24px;border-bottom:1px solid var(--vscode-panel-border)}
polyline{fill:none;stroke:var(--vscode-charts-blue);stroke-width:2}
.muted{color:var(--vscode-descriptionForeground)}
</style>
</head>
<body>
<label for="metric">Metric </label><select id="metric"></select>
<p id="value" class="muted">Waiting for samples...</p>
<svg id="chart" viewBox="0 0 800 280" role="img" aria-label="Metric samples over time"><polyline id="line"></polyline></svg>
<script nonce="${nonce}">
const metrics=${metricsJson};
const select=document.getElementById('metric');
for(const id of metrics){const option=document.createElement('option');option.value=id;option.textContent=id;select.append(option)}
const value=document.getElementById('value'),line=document.getElementById('line'),history=[];
select.addEventListener('change',()=>{history.length=0;value.textContent='Waiting for samples...';line.setAttribute('points','');vscode.postMessage({metricId:select.value})});
window.addEventListener('message',event=>{const message=event.data;if(message.type==='sample'){history.push(message.value);if(history.length>60)history.shift();const low=Math.min(...history),high=Math.max(...history),span=high-low||1;const points=history.map((item,index)=>{const x=history.length===1?400:index*800/(history.length-1);const y=260-(item-low)*240/span;return x+','+y}).join(' ');line.setAttribute('points',points);value.textContent=message.metricId+': '+message.value}else if(message.type==='error'){value.textContent=message.message}});
const vscode=acquireVsCodeApi();
</script>
</body>
</html>`;
    }

    private dispose(): void {
        if (this.timer) {
            clearInterval(this.timer);
        }
        for (const disposable of this.disposables) {
            disposable.dispose();
        }
        MetricChartPanel.panels.delete(this.key);
    }
}
