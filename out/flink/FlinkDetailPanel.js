"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.FlinkDetailPanel = void 0;
const vscode = __importStar(require("vscode"));
class FlinkDetailPanel {
    static panels = new Map();
    static async open(clusterId, resourceId, title, type, data) {
        const key = `${clusterId}:${resourceId}`;
        const existing = this.panels.get(key);
        if (existing) {
            existing.title = title;
            existing.webview.html = this.html(title, type, data);
            existing.reveal(vscode.ViewColumn.One);
            return;
        }
        const panel = vscode.window.createWebviewPanel('flinkResource', title, vscode.ViewColumn.One, { enableScripts: false, localResourceRoots: [] });
        panel.webview.html = this.html(title, type, data);
        this.panels.set(key, panel);
        panel.onDidDispose(() => this.panels.delete(key));
    }
    static html(title, type, data) {
        const body = this.render(type, data);
        return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline';">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${escapeHtml(title)}</title>
<style>
body{padding:24px;color:var(--vscode-foreground);background:var(--vscode-editor-background);font-family:var(--vscode-font-family);font-size:var(--vscode-font-size)}
h1{font-size:1.55em;font-weight:500;margin:0 0 8px}
h2{font-size:1.12em;font-weight:500;margin:26px 0 10px}
.subtitle,.muted{color:var(--vscode-descriptionForeground)}
.subtitle{margin:0 0 22px;overflow-wrap:anywhere}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin:18px 0}
.card,.section{border:1px solid var(--vscode-panel-border);border-radius:4px;padding:14px;background:var(--vscode-sideBar-background)}
.card-label{color:var(--vscode-descriptionForeground);font-size:.85em}
.card-value{font-size:1.35em;margin-top:8px;overflow-wrap:anywhere}
.section{margin:12px 0;overflow:auto}
table{width:100%;border-collapse:collapse}
th,td{text-align:left;padding:8px 10px;border-bottom:1px solid var(--vscode-panel-border);vertical-align:top;overflow-wrap:anywhere}
th{color:var(--vscode-descriptionForeground);font-weight:500}
tr:last-child td{border-bottom:0}
code,pre{font-family:var(--vscode-editor-font-family);font-size:var(--vscode-editor-font-size)}
code{overflow-wrap:anywhere}
pre{white-space:pre-wrap;overflow-wrap:anywhere;margin:0}
.badge{display:inline-block;padding:3px 8px;border-radius:10px;background:var(--vscode-badge-background);color:var(--vscode-badge-foreground)}
.error{color:var(--vscode-errorForeground)}
.empty{padding:18px;color:var(--vscode-descriptionForeground);border:1px dashed var(--vscode-panel-border)}
.bar{height:8px;background:var(--vscode-progressBar-background);min-width:2px}
</style>
</head>
<body>
<main><h1>${escapeHtml(title)}</h1><p class="subtitle">${escapeHtml(typeLabel(type))}</p>${body}</main>
</body>
</html>`;
    }
    static render(type, data) {
        switch (type) {
            case 'overview':
                return this.renderOverview(data);
            case 'job':
                return this.renderJob(data);
            case 'plan':
            case 'jarPlan':
                return this.renderPlan(data);
            case 'exceptions':
                return this.renderExceptions(data);
            case 'configuration':
                return this.renderConfiguration(data, 'Configuration properties');
            case 'environment':
                return this.renderConfiguration(data, 'Runtime environment');
            case 'checkpoints':
                return this.renderCheckpoints(data);
            case 'checkpoint':
            case 'subtasks':
                return this.renderCheckpointDetails(data);
            case 'vertex':
                return this.renderVertex(data);
            case 'backpressure':
                return this.renderBackpressure(data);
            case 'flamegraph':
                return this.renderFlamegraph(data);
            case 'watermarks':
                return this.renderWatermarks(data);
            case 'taskManagers':
                return this.renderTaskManagers(data);
            case 'taskManager':
                return this.renderTaskManager(data);
            case 'jobManager':
                return this.renderJobManager(data);
            case 'logs':
                return this.renderLogs(data);
            case 'threadDump':
                return this.renderThreadDump(data);
            case 'savepoint':
                return this.renderSavepoint(data);
            default:
                return this.renderDetails(data);
        }
    }
    static renderOverview(data) {
        const record = asRecord(data);
        const overview = asRecord(record['overview'] ?? data);
        const config = asRecord(record['config']);
        const metrics = record['metrics'];
        const cards = [
            ['Flink version', value(overview['flink-version'] ?? config['flink-version'])],
            ['Task Managers', value(overview['taskmanagers'])],
            ['Total slots', value(overview['slots-total'])],
            ['Available slots', value(overview['slots-available'])],
            ['Running jobs', value(overview['jobs-running'])],
            ['Finished jobs', value(overview['jobs-finished'])],
            ['Failed jobs', value(overview['jobs-failed'])],
            ['Canceled jobs', value(overview['jobs-cancelled'] ?? overview['jobs-canceled'])]
        ];
        return this.cards(cards) + this.section('Cluster overview', this.keyValueTable(overview))
            + this.section('JobManager configuration', this.table(record['jobManagerConfig']))
            + this.section('JobManager metrics', this.metricTable(metrics));
    }
    static renderJob(data) {
        const job = asRecord(data);
        const tasks = asRecord(job['tasks']);
        return this.cards([
            ['Status', value(job['state'])],
            ['Job ID', value(job['jid'])],
            ['Parallelism', value(job['status-counts'] ? undefined : job['parallelism'])],
            ['Duration', duration(job['duration'])],
            ['Started', timestamp(job['start-time'] ?? job['startTime'])],
            ['Tasks running', value(tasks['running'])],
            ['Tasks finished', value(tasks['finished'])],
            ['Tasks failed', value(tasks['failed'])]
        ]) + this.section('Job details', this.keyValueTable(job, new Set(['vertices', 'tasks'])))
            + this.section('Task state counts', this.keyValueTable(tasks))
            + this.section('Vertices', this.table(job['vertices']));
    }
    static renderPlan(data) {
        const plan = asRecord(data);
        const planData = plan['plan'] ?? data;
        const nodes = asRecord(planData)['nodes'];
        return this.section('Execution plan', this.table(nodes))
            + this.section('Plan metadata', this.keyValueTable(plan, new Set(['plan'])));
    }
    static renderExceptions(data) {
        const record = asRecord(data);
        const exceptions = record['all-exceptions'] ?? record['exceptions'] ?? record['root-exception'] ?? data;
        return this.section('Failure summary', this.keyValueTable(record, new Set(['all-exceptions', 'exceptions'])))
            + this.section('Exceptions', this.listCards(exceptions, ['exception', 'task', 'location', 'timestamp']));
    }
    static renderConfiguration(data, title) {
        const entries = Array.isArray(data)
            ? data
            : Array.isArray(asRecord(data)['environment'])
                ? asRecord(data)['environment']
                : Object.entries(asRecord(data)).map(([key, item]) => ({ key, value: item }));
        return this.section(title, this.table(entries));
    }
    static renderCheckpoints(data) {
        const record = asRecord(data);
        return this.cards([
            ['Total', value(asRecord(record['counts'])['total'])],
            ['Completed', value(asRecord(record['counts'])['completed'])],
            ['Failed', value(asRecord(record['counts'])['failed'])],
            ['In progress', value(asRecord(record['counts'])['in_progress'] ?? asRecord(record['counts'])['in-progress'])]
        ]) + this.section('Checkpoint history', this.table(record['history']))
            + this.section('Latest checkpoints', this.keyValueTable(asRecord(record['latest'])));
    }
    static renderCheckpointDetails(data) {
        const details = asRecord(data);
        return this.cards([
            ['Checkpoint', value(details['id'])],
            ['Status', value(details['status'])],
            ['Trigger timestamp', timestamp(details['trigger_timestamp'] ?? details['triggerTimestamp'])],
            ['Duration', duration(details['end_to_end_duration'] ?? details['endToEndDuration'])],
            ['State size', bytes(details['state_size'] ?? details['stateSize'])],
            ['Acknowledged subtasks', value(details['num_acknowledged_subtasks'] ?? details['numAcknowledgedSubtasks'])]
        ]) + this.section('Checkpoint details', this.keyValueTable(details))
            + this.section('Subtasks', this.table(details['subtasks']));
    }
    static renderVertex(data) {
        const vertex = asRecord(data);
        return this.cards([
            ['Vertex', value(vertex['name'])],
            ['Status', value(vertex['status'])],
            ['Parallelism', value(vertex['parallelism'])],
            ['Running', value(vertex['number-of-running-subtasks'] ?? vertex['running'])],
            ['Finished', value(vertex['number-of-finished-subtasks'] ?? vertex['finished'])],
            ['Failed', value(vertex['number-of-failed-subtasks'] ?? vertex['failed'])]
        ]) + this.section('Vertex information', this.keyValueTable(vertex, new Set(['subtasks'])))
            + this.section('Subtasks', this.table(vertex['subtasks']));
    }
    static renderBackpressure(data) {
        const result = asRecord(data);
        const detail = asRecord(result['result']);
        const subtasks = array(result['subtasks'] ?? detail['subtasks']);
        const rows = subtasks.map(item => {
            const subtask = asRecord(item);
            const ratio = Number(subtask['ratio'] ?? subtask['backpressure-level'] ?? subtask['backpressureLevel'] ?? 0);
            return {
                ...subtask,
                'backpressure ratio': Number.isFinite(ratio) ? `${Math.round(ratio * 100)}%` : '—'
            };
        });
        return this.cards([
            ['Status', value(result['status'] ?? detail['status'])],
            ['Sample duration', duration(result['sample-duration'] ?? result['sampleDuration'] ?? detail['sample-duration'])],
            ['Subtasks', String(rows.length)]
        ]) + this.section('Backpressure by subtask', this.table(rows));
    }
    static renderFlamegraph(data) {
        const record = asRecord(data);
        const samples = record['end'] ?? record['samples'] ?? record['root'] ?? data;
        return this.section('Flamegraph samples', this.listCards(samples, ['name', 'value', 'self', 'total', 'count']));
    }
    static renderWatermarks(data) {
        const record = asRecord(data);
        return this.section('Watermarks by subtask', this.table(record['watermarks'] ?? record['subtasks'] ?? data));
    }
    static renderTaskManagers(data) {
        const record = asRecord(data);
        return this.section('Task Managers', this.table(record['taskmanagers'] ?? record['taskManagers'] ?? data));
    }
    static renderTaskManager(data) {
        const manager = asRecord(data);
        const hardware = asRecord(manager['hardware']);
        const resources = asRecord(manager['totalResource'] ?? manager['total-resource']);
        return this.cards([
            ['Task Manager ID', value(manager['id'])],
            ['Slots', value(manager['slotsNumber'] ?? manager['slots-number'])],
            ['Free slots', value(manager['freeSlots'] ?? manager['free-slots'])],
            ['CPU cores', value(hardware['cpuCores'] ?? resources['cpuCores'])],
            ['Last heartbeat', timestamp(manager['timeSinceLastHeartbeat'] ?? manager['time-since-last-heartbeat'])],
            ['Data port', value(manager['dataPort'] ?? manager['data-port'])]
        ]) + this.section('Task Manager details', this.keyValueTable(manager));
    }
    static renderJobManager(data) {
        const record = asRecord(data);
        return this.cards([
            ['Flink version', value(record['flink-version'])],
            ['Address', value(record['address'])],
            ['Task Managers', value(record['taskmanagers'])],
            ['Total slots', value(record['slots-total'])],
            ['Available slots', value(record['slots-available'])]
        ]) + this.section('JobManager details', this.keyValueTable(record));
    }
    static renderLogs(data) {
        if (typeof data === 'string') {
            return `<section class="section"><pre>${escapeHtml(data)}</pre></section>`;
        }
        const record = asRecord(data);
        return this.section('Log files', this.table(record['logs'] ?? data));
    }
    static renderThreadDump(data) {
        const record = asRecord(data);
        const threads = array(record['threadInfos'] ?? record['thread-infos']);
        return threads.length
            ? this.section('Thread dump', this.listCards(threads, ['threadName', 'thread-name', 'threadId', 'thread-id', 'threadState', 'thread-state', 'stringifiedThreadInfo']))
            : this.section('Thread dump', this.keyValueTable(record));
    }
    static renderSavepoint(data) {
        const record = asRecord(data);
        return this.cards([
            ['Job', value(record['jobName'])],
            ['Created', timestamp(record['createdAt'])],
            ['Savepoint path', value(record['path'])]
        ]) + this.section('Savepoint', this.keyValueTable(record));
    }
    static renderDetails(data) {
        if (typeof data === 'string') {
            return `<section class="section"><pre>${escapeHtml(data)}</pre></section>`;
        }
        if (Array.isArray(data)) {
            return this.section('Details', this.table(data));
        }
        return this.section('Details', this.keyValueTable(asRecord(data)));
    }
    static cards(items) {
        const visible = items.filter(([, content]) => content !== '—');
        if (!visible.length) {
            return '';
        }
        return `<div class="cards">${visible.map(([label, content]) => (`<article class="card"><div class="card-label">${escapeHtml(label)}</div><div class="card-value">${escapeHtml(content)}</div></article>`)).join('')}</div>`;
    }
    static section(title, content) {
        return `<h2>${escapeHtml(title)}</h2>${content || '<div class="empty">No data available</div>'}`;
    }
    static keyValueTable(record, omitted = new Set()) {
        const entries = Object.entries(record).filter(([key, item]) => !omitted.has(key) && item !== undefined);
        if (!entries.length) {
            return '<div class="empty">No data available</div>';
        }
        return `<div class="section"><table><tbody>${entries.map(([key, item]) => (`<tr><th>${escapeHtml(humanize(key))}</th><td>${formatValue(item)}</td></tr>`)).join('')}</tbody></table></div>`;
    }
    static table(input) {
        const rows = Array.isArray(input) ? input : input === undefined || input === null ? [] : [input];
        if (!rows.length) {
            return '<div class="empty">No data available</div>';
        }
        if (rows.every(row => typeof row !== 'object' || row === null)) {
            return `<div class="section"><table><tbody>${rows.map((row, index) => (`<tr><th>${index + 1}</th><td>${formatValue(row)}</td></tr>`)).join('')}</tbody></table></div>`;
        }
        const objects = rows.map(asRecord);
        const columns = [...new Set(objects.flatMap(row => Object.keys(row)))];
        if (!columns.length) {
            return '<div class="empty">No data available</div>';
        }
        return `<div class="section"><table><thead><tr>${columns.map(column => `<th>${escapeHtml(humanize(column))}</th>`).join('')}</tr></thead><tbody>${objects.map(row => (`<tr>${columns.map(column => `<td>${formatValue(row[column])}</td>`).join('')}</tr>`)).join('')}</tbody></table></div>`;
    }
    static metricTable(input) {
        return this.table(input);
    }
    static listCards(input, preferredFields) {
        const items = Array.isArray(input) ? input : input === undefined || input === null ? [] : [input];
        if (!items.length) {
            return '<div class="empty">No entries</div>';
        }
        return `<div class="cards">${items.map((item, index) => {
            const record = asRecord(item);
            if (typeof item !== 'object' || item === null) {
                return `<article class="card"><div class="card-value">${escapeHtml(value(item))}</div></article>`;
            }
            const titleKey = preferredFields.find(key => record[key] !== undefined);
            const title = titleKey ? value(record[titleKey]) : `Entry ${index + 1}`;
            const body = Object.entries(record)
                .filter(([key]) => key !== titleKey)
                .map(([key, content]) => `<div><span class="muted">${escapeHtml(humanize(key))}:</span> ${formatValue(content)}</div>`)
                .join('');
            return `<article class="card"><div class="card-value">${escapeHtml(title)}</div>${body}</article>`;
        }).join('')}</div>`;
    }
}
exports.FlinkDetailPanel = FlinkDetailPanel;
function asRecord(value) {
    if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
        return value;
    }
    return {};
}
function array(value) {
    return Array.isArray(value) ? value : [];
}
function value(input) {
    if (input === undefined || input === null || input === '') {
        return '—';
    }
    if (typeof input === 'object') {
        return JSON.stringify(input);
    }
    return String(input);
}
function formatValue(input) {
    if (typeof input === 'string' && (input.includes('\n') || input.length > 180)) {
        return `<pre>${escapeHtml(input)}</pre>`;
    }
    return escapeHtml(value(input));
}
function humanize(input) {
    return input.replace(/([a-z])([A-Z])/g, '$1 $2').replace(/[-_]/g, ' ');
}
function typeLabel(type) {
    return humanize(type).replace(/^./, letter => letter.toUpperCase());
}
function duration(input) {
    const milliseconds = Number(input);
    if (!Number.isFinite(milliseconds)) {
        return value(input);
    }
    if (milliseconds < 1000) {
        return `${milliseconds} ms`;
    }
    const seconds = Math.floor(milliseconds / 1000);
    const minutes = Math.floor(seconds / 60);
    return minutes ? `${minutes}m ${seconds % 60}s` : `${seconds}s`;
}
function timestamp(input) {
    const number = Number(input);
    if (!Number.isFinite(number) || number <= 0) {
        return value(input);
    }
    return new Date(number).toLocaleString();
}
function bytes(input) {
    const number = Number(input);
    if (!Number.isFinite(number)) {
        return value(input);
    }
    if (number < 1024) {
        return `${number} B`;
    }
    const units = ['KB', 'MB', 'GB', 'TB'];
    const exponent = Math.min(Math.floor(Math.log(number) / Math.log(1024)) - 1, units.length - 1);
    return `${(number / (1024 ** (exponent + 1))).toFixed(2)} ${units[exponent]}`;
}
function escapeHtml(input) {
    return input.replace(/[&<>"']/g, character => {
        switch (character) {
            case '&': return '&amp;';
            case '<': return '&lt;';
            case '>': return '&gt;';
            case '"': return '&quot;';
            default: return '&#39;';
        }
    });
}
//# sourceMappingURL=FlinkDetailPanel.js.map