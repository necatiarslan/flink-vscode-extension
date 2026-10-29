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
exports.FlinkTreeProvider = exports.FlinkTreeNode = void 0;
const vscode = __importStar(require("vscode"));
const FlinkClient_1 = require("../flink/FlinkClient");
class FlinkTreeNode extends vscode.TreeItem {
    nodeType;
    profile;
    resource;
    jobId;
    taskManagerId;
    childNodes;
    constructor(label, collapsibleState, nodeType, profile, resource, description, jobId, taskManagerId, childNodes) {
        super(label, collapsibleState);
        this.nodeType = nodeType;
        this.profile = profile;
        this.resource = resource;
        this.jobId = jobId;
        this.taskManagerId = taskManagerId;
        this.childNodes = childNodes;
        this.description = description;
    }
}
exports.FlinkTreeNode = FlinkTreeNode;
class FlinkTreeProvider {
    context;
    store;
    changes = new vscode.EventEmitter();
    onDidChangeTreeData = this.changes.event;
    constructor(context, store) {
        this.context = context;
        this.store = store;
        context.subscriptions.push(this.changes);
        context.subscriptions.push(vscode.commands.registerCommand('ApacheFlink.AddCluster', () => this.addCluster()), vscode.commands.registerCommand('ApacheFlink.EditCluster', (node) => this.editCluster(node)), vscode.commands.registerCommand('ApacheFlink.RemoveCluster', (node) => this.removeCluster(node)), vscode.commands.registerCommand('ApacheFlink.TestConnection', (node) => this.testConnection(node)), vscode.commands.registerCommand('ApacheFlink.Refresh', () => this.refresh()), vscode.commands.registerCommand('ApacheFlink.OpenResource', (node) => this.openResource(node)));
    }
    getTreeItem(element) {
        return element;
    }
    async getChildren(element) {
        if (!element) {
            return this.store.getAll().map(profile => {
                const item = new FlinkTreeNode(profile.name, vscode.TreeItemCollapsibleState.Collapsed, 'cluster', profile, undefined, profile.baseUrl);
                item.contextValue = 'flinkCluster';
                return item;
            });
        }
        try {
            return await this.getNodeChildren(element);
        }
        catch (error) {
            const detail = error instanceof Error ? error.message : String(error);
            return [new FlinkTreeNode(`Unable to load: ${detail}`, vscode.TreeItemCollapsibleState.None, 'error')];
        }
    }
    refresh() {
        this.changes.fire();
    }
    async getNodeChildren(node) {
        const profile = node.profile;
        if (!profile) {
            return [];
        }
        switch (node.nodeType) {
            case 'cluster':
                return [
                    this.actionNode('Overview', profile, { kind: 'overview', profile }),
                    new FlinkTreeNode('Jobs', vscode.TreeItemCollapsibleState.Collapsed, 'jobs', profile),
                    new FlinkTreeNode('Task Managers', vscode.TreeItemCollapsibleState.Collapsed, 'taskManagers', profile),
                    new FlinkTreeNode('Job Manager', vscode.TreeItemCollapsibleState.Collapsed, 'jobManager', profile)
                ];
            case 'jobs':
                return this.getJobGroups(profile);
            case 'jobGroup':
                return node.childNodes ?? [];
            case 'job':
                if (!node.jobId) {
                    return [];
                }
                return [
                    this.actionNode('Overview', profile, { kind: 'job', profile, jobId: node.jobId, resource: 'overview' }),
                    this.actionNode('Plan', profile, { kind: 'job', profile, jobId: node.jobId, resource: 'plan' }),
                    this.actionNode('Exceptions', profile, { kind: 'job', profile, jobId: node.jobId, resource: 'exceptions' }),
                    this.actionNode('Configuration', profile, { kind: 'job', profile, jobId: node.jobId, resource: 'config' })
                ];
            case 'taskManagers':
                return this.getTaskManagers(profile);
            case 'taskManager':
                if (!node.taskManagerId) {
                    return [];
                }
                return [
                    this.actionNode('Details', profile, {
                        kind: 'taskManager',
                        profile,
                        taskManagerId: node.taskManagerId,
                        resource: 'overview'
                    }),
                    this.actionNode('Logs', profile, {
                        kind: 'taskManager',
                        profile,
                        taskManagerId: node.taskManagerId,
                        resource: 'logs'
                    })
                ];
            case 'jobManager':
                return [
                    this.actionNode('Configuration', profile, { kind: 'jobManager', profile, resource: 'config' }),
                    this.actionNode('Logs', profile, { kind: 'jobManager', profile, resource: 'logs' })
                ];
            default:
                return [];
        }
    }
    async getJobGroups(profile) {
        const result = await new FlinkClient_1.FlinkClient(profile).get('/jobs/overview');
        const jobs = result.jobs ?? [];
        if (jobs.length === 0) {
            return [new FlinkTreeNode('No jobs', vscode.TreeItemCollapsibleState.None, 'empty')];
        }
        const groups = new Map();
        for (const job of jobs) {
            const status = (job.state || 'UNKNOWN').toUpperCase();
            const group = groups.get(status) ?? [];
            const item = new FlinkTreeNode(job.name || job.jid, vscode.TreeItemCollapsibleState.Collapsed, 'job', profile, undefined, status, job.jid);
            item.id = `${profile.id}:job:${job.jid}`;
            item.tooltip = `Job ID: ${job.jid}`;
            item.contextValue = `flinkJob.${status.toLowerCase()}`;
            group.push(item);
            groups.set(status, group);
        }
        return [...groups.entries()].map(([status, groupedJobs]) => {
            const groupNode = new FlinkTreeNode(`${status} (${groupedJobs.length})`, vscode.TreeItemCollapsibleState.Expanded, 'jobGroup', profile, undefined, undefined, undefined, undefined, groupedJobs);
            return groupNode;
        });
    }
    async getTaskManagers(profile) {
        const result = await new FlinkClient_1.FlinkClient(profile).get('/taskmanagers');
        const managers = result.taskmanagers ?? [];
        if (managers.length === 0) {
            return [new FlinkTreeNode('No task managers', vscode.TreeItemCollapsibleState.None, 'empty')];
        }
        return managers.map(manager => {
            const item = new FlinkTreeNode(manager.id, vscode.TreeItemCollapsibleState.Collapsed, 'taskManager', profile, undefined, `${Math.max(0, (manager.slotsNumber ?? 0) - (manager.freeSlots ?? 0))}/${manager.slotsNumber ?? 0} slots`, undefined, manager.id);
            item.contextValue = 'flinkTaskManager';
            return item;
        });
    }
    async addCluster() {
        const name = await vscode.window.showInputBox({
            prompt: 'Name this Flink cluster',
            placeHolder: 'local-dev',
            validateInput: value => value.trim() ? undefined : 'A cluster name is required.'
        });
        if (name === undefined) {
            return;
        }
        const baseUrl = await vscode.window.showInputBox({
            prompt: 'Flink REST API URL',
            placeHolder: 'http://localhost:8081',
            value: 'http://localhost:8081',
            validateInput: value => this.validateUrl(value)
        });
        if (baseUrl === undefined) {
            return;
        }
        try {
            await this.store.add(name.trim(), baseUrl.trim());
            this.refresh();
        }
        catch (error) {
            this.showError('Could not add Flink cluster', error);
        }
    }
    async editCluster(node) {
        const profile = node.profile;
        if (!profile) {
            return;
        }
        const name = await vscode.window.showInputBox({
            prompt: 'Cluster name',
            value: profile.name,
            validateInput: value => value.trim() ? undefined : 'A cluster name is required.'
        });
        if (name === undefined) {
            return;
        }
        const baseUrl = await vscode.window.showInputBox({
            prompt: 'Flink REST API URL',
            value: profile.baseUrl,
            validateInput: value => this.validateUrl(value)
        });
        if (baseUrl === undefined) {
            return;
        }
        try {
            await this.store.update(profile.id, name.trim(), baseUrl.trim());
            this.refresh();
        }
        catch (error) {
            this.showError('Could not update Flink cluster', error);
        }
    }
    async removeCluster(node) {
        const profile = node.profile;
        if (!profile) {
            return;
        }
        const confirmation = await vscode.window.showWarningMessage(`Remove Flink cluster "${profile.name}"?`, { modal: true }, 'Remove');
        if (confirmation !== 'Remove') {
            return;
        }
        try {
            await this.store.remove(profile.id);
            this.refresh();
        }
        catch (error) {
            this.showError(`Could not remove "${profile.name}"`, error);
        }
    }
    async testConnection(node) {
        const profile = node.profile;
        if (!profile) {
            return;
        }
        try {
            const result = await new FlinkClient_1.FlinkClient(profile).testConnection();
            await vscode.window.showInformationMessage(`Connected to Flink ${result.version} at ${profile.baseUrl}.`);
        }
        catch (error) {
            this.showError(`Could not connect to "${profile.name}"`, error);
        }
    }
    async openResource(node) {
        const resource = node.resource;
        if (!resource) {
            return;
        }
        try {
            const client = new FlinkClient_1.FlinkClient(resource.profile);
            const data = await this.fetchResource(client, resource);
            const content = JSON.stringify(data, null, 2) ?? String(data);
            const document = await vscode.workspace.openTextDocument({ language: 'json', content });
            await vscode.window.showTextDocument(document, vscode.ViewColumn.One);
        }
        catch (error) {
            this.showError(`Could not load ${node.label}`, error);
        }
    }
    async fetchResource(client, resource) {
        switch (resource.kind) {
            case 'overview': {
                const [overview, config] = await Promise.all([
                    client.get('/overview'),
                    client.get('/config')
                ]);
                return { overview, config };
            }
            case 'job': {
                const prefix = `/jobs/${encodeURIComponent(resource.jobId)}`;
                const endpoint = resource.resource === 'overview'
                    ? prefix
                    : `${prefix}/${resource.resource}`;
                return client.get(endpoint);
            }
            case 'taskManager': {
                const prefix = `/taskmanagers/${encodeURIComponent(resource.taskManagerId)}`;
                return client.get(resource.resource === 'overview' ? prefix : `${prefix}/logs`);
            }
            case 'jobManager':
                return client.get(`/jobmanager/${resource.resource}`);
        }
    }
    actionNode(label, profile, resource) {
        const node = new FlinkTreeNode(label, vscode.TreeItemCollapsibleState.None, 'resource', profile, resource);
        node.command = {
            command: 'ApacheFlink.OpenResource',
            title: `Open ${label}`,
            arguments: [node]
        };
        return node;
    }
    validateUrl(value) {
        try {
            const url = new URL(value);
            if (url.protocol !== 'http:' && url.protocol !== 'https:') {
                return 'The URL must use HTTP or HTTPS.';
            }
            if (url.username || url.password || url.search || url.hash) {
                return 'Do not include credentials, query parameters, or fragments.';
            }
            return undefined;
        }
        catch {
            return 'Enter a valid Flink REST URL, for example http://localhost:8081.';
        }
    }
    showError(message, error) {
        const detail = error instanceof Error ? error.message : String(error);
        void vscode.window.showErrorMessage(`${message}: ${detail}`);
    }
}
exports.FlinkTreeProvider = FlinkTreeProvider;
//# sourceMappingURL=FlinkTreeProvider.js.map