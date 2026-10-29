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
const crypto_1 = require("crypto");
const AsyncOperationTracker_1 = require("../flink/AsyncOperationTracker");
const FlinkClient_1 = require("../flink/FlinkClient");
const MetricChartPanel_1 = require("../flink/MetricChartPanel");
class FlinkTreeNode extends vscode.TreeItem {
    nodeType;
    profile;
    resource;
    operation;
    jobId;
    jobName;
    vertexId;
    taskManagerId;
    jarId;
    childNodes;
    checkpointId;
    savepointPath;
    jarName;
    constructor(label, collapsibleState, nodeType, profile, resource, operation, jobId, jobName, vertexId, taskManagerId, jarId, childNodes, checkpointId, savepointPath, jarName) {
        super(label, collapsibleState);
        this.nodeType = nodeType;
        this.profile = profile;
        this.resource = resource;
        this.operation = operation;
        this.jobId = jobId;
        this.jobName = jobName;
        this.vertexId = vertexId;
        this.taskManagerId = taskManagerId;
        this.jarId = jarId;
        this.childNodes = childNodes;
        this.checkpointId = checkpointId;
        this.savepointPath = savepointPath;
        this.jarName = jarName;
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
        context.subscriptions.push(vscode.commands.registerCommand('ApacheFlink.AddCluster', () => this.addCluster()), vscode.commands.registerCommand('ApacheFlink.EditCluster', (node) => this.editCluster(node)), vscode.commands.registerCommand('ApacheFlink.RemoveCluster', (node) => this.removeCluster(node)), vscode.commands.registerCommand('ApacheFlink.TestConnection', (node) => this.testConnection(node)), vscode.commands.registerCommand('ApacheFlink.Refresh', () => this.refresh()), vscode.commands.registerCommand('ApacheFlink.OpenResource', (node) => this.openResource(node)), vscode.commands.registerCommand('ApacheFlink.RunOperation', (node) => this.runOperation(node)));
    }
    getTreeItem(element) {
        return element;
    }
    async getChildren(element) {
        if (!element) {
            return this.store.getAll().map(profile => {
                const readOnly = profile.readOnly ?? true;
                const item = new FlinkTreeNode(profile.name, vscode.TreeItemCollapsibleState.Collapsed, 'cluster', profile);
                item.description = `${profile.baseUrl}${readOnly ? '  (read-only)' : ''}`;
                item.contextValue = 'flinkCluster';
                item.iconPath = new vscode.ThemeIcon(readOnly ? 'lock' : 'server');
                return item;
            });
        }
        try {
            return await this.getNodeChildren(element);
        }
        catch (error) {
            return [this.messageNode(`Unable to load: ${this.errorMessage(error)}`)];
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
                return this.clusterChildren(profile);
            case 'jobs':
                return this.getJobGroups(profile);
            case 'jobGroup':
                return node.childNodes ?? [];
            case 'job':
                return this.getJobChildren(node);
            case 'checkpoints':
                return this.getCheckpointChildren(node);
            case 'vertices':
                return this.getVertexChildren(node);
            case 'vertex':
                return this.getVertexDiagnostics(node);
            case 'taskManagers':
                return this.getTaskManagers(profile);
            case 'taskManager':
                return this.getTaskManagerChildren(node);
            case 'jobManager':
                return this.getJobManagerChildren(profile);
            case 'jars':
                return this.getJars(profile);
            case 'jar':
                return this.jarActions(node);
            case 'savepoints':
                return this.getSavepoints(profile);
            case 'checkpointDetails':
                return this.getCheckpointDetails(node);
            default:
                return [];
        }
    }
    async clusterChildren(profile) {
        const client = new FlinkClient_1.FlinkClient(profile);
        let includeJars = true;
        try {
            await client.get('/jars');
        }
        catch (error) {
            if (error instanceof FlinkClient_1.FlinkApiError && error.statusCode === 404) {
                includeJars = false;
            }
            else {
                throw error;
            }
        }
        const children = [
            this.resourceNode('Overview', profile, '/overview', 'Cluster Overview', 'clusterOverview'),
            this.node('Jobs', 'jobs', profile),
            this.node('Task Managers', 'taskManagers', profile),
            this.node('Job Manager', 'jobManager', profile),
            this.node('Savepoints', 'savepoints', profile)
        ];
        if (includeJars) {
            children.push(this.node('Jars', 'jars', profile));
        }
        return children;
    }
    async getJobGroups(profile) {
        const result = await new FlinkClient_1.FlinkClient(profile).get('/jobs/overview');
        const jobs = result.jobs ?? [];
        if (jobs.length === 0) {
            return [this.messageNode('No jobs')];
        }
        const groups = new Map();
        for (const job of jobs) {
            const state = (job.state || 'UNKNOWN').toUpperCase();
            const jobName = job.name || job.jid;
            const item = this.node(jobName, 'job', profile, {
                jobId: job.jid,
                jobName
            });
            item.id = `${profile.id}:job:${job.jid}`;
            item.description = state;
            item.tooltip = `Job ID: ${job.jid}`;
            item.contextValue = `flinkJob.${state.toLowerCase()}`;
            item.iconPath = this.stateIcon(state);
            const group = groups.get(state) ?? [];
            group.push(item);
            groups.set(state, group);
        }
        return [...groups.entries()].map(([state, groupedJobs]) => {
            const group = this.node(`${state} (${groupedJobs.length})`, 'jobGroup', profile);
            group.collapsibleState = vscode.TreeItemCollapsibleState.Expanded;
            Object.defineProperty(group, 'childNodes', { value: groupedJobs });
            return group;
        });
    }
    async getJobChildren(node) {
        const profile = this.requireProfile(node);
        const jobId = this.requireJobId(node);
        const jobName = node.jobName ?? jobId;
        const prefix = `/jobs/${encodeURIComponent(jobId)}`;
        const detail = await new FlinkClient_1.FlinkClient(profile).get(prefix);
        const children = [
            this.resourceNode('Overview', profile, prefix, 'Job Overview'),
            this.resourceNode('Plan', profile, `${prefix}/plan`, 'Execution Plan'),
            this.resourceNode('Exceptions', profile, `${prefix}/exceptions`, 'Job Exceptions'),
            this.resourceNode('Configuration', profile, `${prefix}/config`, 'Job Configuration'),
            this.resourceNode('Metrics Chart', profile, `${prefix}/metrics`, 'Job Metrics', 'metrics'),
            this.node('Checkpoints', 'checkpoints', profile, { jobId, jobName }),
            this.node('Vertices', 'vertices', profile, { jobId, jobName })
        ];
        if (!this.isReadOnly(profile) && this.isJobRunning(node.description)) {
            children.push(this.operationNode('Cancel Job', profile, { type: 'cancelJob', jobId, jobName }), this.operationNode('Trigger Savepoint', profile, { type: 'savepoint', jobId, jobName }), this.operationNode('Stop with Savepoint', profile, { type: 'savepoint', jobId, jobName, stop: true }), this.operationNode('Trigger Checkpoint', profile, { type: 'checkpoint', jobId, jobName }), this.operationNode('Rescale Job', profile, { type: 'rescale', jobId, jobName }));
        }
        if (detail.vertices?.length) {
            const vertexNodes = detail.vertices.map(vertex => {
                const item = this.node(vertex.name || vertex.id, 'vertex', profile, {
                    jobId,
                    jobName,
                    vertexId: vertex.id
                });
                item.description = vertex.status;
                return item;
            });
            const vertexGroup = this.node('Vertices', 'vertices', profile, { jobId, jobName });
            Object.defineProperty(vertexGroup, 'childNodes', { value: vertexNodes });
            return children.map(child => child.nodeType === 'vertices' ? vertexGroup : child);
        }
        return children;
    }
    async getCheckpointChildren(node) {
        const profile = this.requireProfile(node);
        const jobId = this.requireJobId(node);
        const result = await new FlinkClient_1.FlinkClient(profile).get(`/jobs/${encodeURIComponent(jobId)}/checkpoints`);
        const records = result.history ?? Object.values(result.latest ?? {}).filter(value => typeof value === 'object' && value !== null && 'id' in value);
        if (!records.length) {
            return [this.messageNode('No checkpoint history')];
        }
        return records.map(checkpoint => {
            const item = this.node(`Checkpoint ${checkpoint.id}`, 'checkpointDetails', profile, {
                jobId,
                jobName: node.jobName,
                checkpointId: checkpoint.id
            });
            item.description = checkpoint.status;
            return item;
        });
    }
    async getCheckpointDetails(node) {
        const profile = this.requireProfile(node);
        const jobId = this.requireJobId(node);
        const checkpointId = node.checkpointId;
        const id = Number.isFinite(checkpointId) ? checkpointId : undefined;
        if (id === undefined) {
            return [];
        }
        return [
            this.resourceNode('Details', profile, `/jobs/${encodeURIComponent(jobId)}/checkpoints/details/${id}`, 'Checkpoint Details'),
            this.resourceNode('Configuration', profile, `/jobs/${encodeURIComponent(jobId)}/checkpoints/config`, 'Checkpoint Configuration'),
            ...await this.getCheckpointVertexSubtasks(profile, jobId, id)
        ];
    }
    async getCheckpointVertexSubtasks(profile, jobId, checkpointId) {
        const job = await new FlinkClient_1.FlinkClient(profile).get(`/jobs/${encodeURIComponent(jobId)}`);
        return (job.vertices ?? []).map(vertex => this.resourceNode(`Subtasks: ${vertex.name || vertex.id}`, profile, `/jobs/${encodeURIComponent(jobId)}/checkpoints/details/${checkpointId}/subtasks/${encodeURIComponent(vertex.id)}`, `Checkpoint ${checkpointId} Subtasks`));
    }
    async getVertexChildren(node) {
        if (node.childNodes) {
            return node.childNodes;
        }
        const profile = this.requireProfile(node);
        const jobId = this.requireJobId(node);
        const job = await new FlinkClient_1.FlinkClient(profile).get(`/jobs/${encodeURIComponent(jobId)}`);
        return (job.vertices ?? []).map(vertex => {
            const item = this.node(vertex.name || vertex.id, 'vertex', profile, {
                jobId,
                jobName: node.jobName,
                vertexId: vertex.id
            });
            item.description = vertex.status;
            return item;
        });
    }
    getVertexDiagnostics(node) {
        const profile = this.requireProfile(node);
        const jobId = this.requireJobId(node);
        const vertexId = node.vertexId;
        if (!vertexId) {
            return [];
        }
        const prefix = `/jobs/${encodeURIComponent(jobId)}/vertices/${encodeURIComponent(vertexId)}`;
        return [
            this.resourceNode('Details', profile, prefix, 'Vertex Details'),
            this.resourceNode('Backpressure', profile, `${prefix}/backpressure`, 'Backpressure'),
            this.resourceNode('Flamegraph', profile, `${prefix}/flamegraph`, 'Flamegraph'),
            this.resourceNode('Watermarks', profile, `${prefix}/watermarks`, 'Watermarks'),
            this.resourceNode('Subtask Times', profile, `${prefix}/subtasktimes`, 'Subtask Times'),
            this.resourceNode('Task Managers', profile, `${prefix}/taskmanagers`, 'Vertex Task Managers'),
            this.resourceNode('Metrics Chart', profile, `${prefix}/metrics`, 'Vertex Metrics', 'metrics')
        ];
    }
    async getTaskManagers(profile) {
        const result = await new FlinkClient_1.FlinkClient(profile).get('/taskmanagers');
        if (!result.taskmanagers?.length) {
            return [this.messageNode('No task managers')];
        }
        return result.taskmanagers.map(manager => {
            const item = this.node(manager.id, 'taskManager', profile, { taskManagerId: manager.id });
            const used = Math.max(0, (manager.slotsNumber ?? 0) - (manager.freeSlots ?? 0));
            item.description = `${used}/${manager.slotsNumber ?? 0} slots`;
            item.iconPath = new vscode.ThemeIcon('server');
            return item;
        });
    }
    getTaskManagerChildren(node) {
        const profile = this.requireProfile(node);
        const managerId = node.taskManagerId;
        if (!managerId) {
            return [];
        }
        const prefix = `/taskmanagers/${encodeURIComponent(managerId)}`;
        return [
            this.resourceNode('Details', profile, prefix, 'Task Manager Details'),
            this.resourceNode('Logs', profile, `${prefix}/logs`, 'Task Manager Logs', 'logList'),
            this.resourceNode('Metrics Chart', profile, `${prefix}/metrics`, 'Task Manager Metrics', 'metrics'),
            this.resourceNode('Thread Dump', profile, `${prefix}/thread-dump`, 'Task Manager Thread Dump')
        ];
    }
    getJobManagerChildren(profile) {
        return [
            this.resourceNode('Configuration', profile, '/jobmanager/config', 'JobManager Configuration'),
            this.resourceNode('Environment', profile, '/jobmanager/environment', 'JobManager Environment'),
            this.resourceNode('Logs', profile, '/jobmanager/logs', 'JobManager Logs', 'logList'),
            this.resourceNode('Metrics Chart', profile, '/jobmanager/metrics', 'JobManager Metrics', 'metrics'),
            this.resourceNode('Thread Dump', profile, '/jobmanager/thread-dump', 'JobManager Thread Dump')
        ];
    }
    async getJars(profile) {
        const result = await new FlinkClient_1.FlinkClient(profile).get('/jars');
        const jars = result.files ?? [];
        const nodes = jars.map(jar => {
            const item = this.node(jar.name || jar.id, 'jar', profile, {
                jarId: jar.id,
                jarName: jar.name || jar.id
            });
            item.description = jar.entry;
            item.tooltip = `Jar ID: ${jar.id}`;
            return item;
        });
        if (!this.isReadOnly(profile)) {
            nodes.unshift(this.operationNode('Upload JAR', profile, { type: 'uploadJar' }));
        }
        return nodes.length ? nodes : [this.messageNode('No uploaded JARs')];
    }
    jarActions(node) {
        const profile = this.requireProfile(node);
        const jarId = node.jarId;
        if (!jarId) {
            return [];
        }
        const children = [this.resourceNode('Plan', profile, `/jars/${encodeURIComponent(jarId)}/plan`, 'JAR Plan')];
        if (!this.isReadOnly(profile)) {
            children.push(this.operationNode('Submit JAR', profile, { type: 'submitJar', jarId }), this.operationNode('Delete JAR', profile, {
                type: 'deleteJar',
                jarId,
                jarName: String(node.label)
            }));
        }
        return children;
    }
    getSavepoints(profile) {
        const savepoints = this.context.globalState.get(this.savepointKey(profile), []);
        const children = savepoints.map(savepoint => {
            const item = this.node(savepoint.jobName, 'savepoint', profile, {
                savepointPath: savepoint.path
            });
            item.description = savepoint.path;
            item.tooltip = new Date(savepoint.createdAt).toLocaleString();
            item.contextValue = 'flinkSavepoint';
            return item;
        });
        if (!this.isReadOnly(profile)) {
            children.unshift(this.operationNode('Dispose Savepoint...', profile, { type: 'disposeSavepoint' }));
        }
        if (!savepoints.length) {
            children.push(this.messageNode('No tracked savepoints'));
        }
        return children;
    }
    async openResource(node) {
        const profile = node.profile;
        const resource = node.resource;
        if (!profile || !resource) {
            return;
        }
        try {
            if (resource.kind === 'clusterOverview') {
                const client = new FlinkClient_1.FlinkClient(profile);
                const [overview, config, jobManagerConfig, metrics] = await Promise.all([
                    client.get('/overview'),
                    client.get('/config'),
                    client.get('/jobmanager/config'),
                    client.get('/jobmanager/metrics')
                ]);
                await this.showJson(resource.title, { overview, config, jobManagerConfig, metrics });
                return;
            }
            if (resource.kind === 'metrics') {
                await this.openMetrics(profile, resource.path);
                return;
            }
            if (!resource.path) {
                return;
            }
            const client = new FlinkClient_1.FlinkClient(profile);
            const data = await client.get(resource.path);
            if (resource.kind === 'logList') {
                await this.openLog(profile, resource.path, data);
            }
            else {
                await this.showJson(resource.title, data);
            }
        }
        catch (error) {
            this.showError(`Could not load ${resource.title}`, error);
        }
    }
    async openMetrics(profile, path) {
        const metrics = await new FlinkClient_1.FlinkClient(profile).get(path);
        if (!metrics.length) {
            await vscode.window.showInformationMessage('No metrics are available for this resource.');
            return;
        }
        MetricChartPanel_1.MetricChartPanel.open(profile.id, new FlinkClient_1.FlinkClient(profile), path, metrics);
    }
    async openLog(profile, path, data) {
        const logs = Array.isArray(data)
            ? data
            : typeof data === 'object' && data !== null && Array.isArray(data.logs)
                ? data.logs
                : [];
        if (!logs.length) {
            await this.showJson('Log Files', data);
            return;
        }
        const entries = logs.map(log => typeof log === 'string' ? log : JSON.stringify(log));
        const selected = await vscode.window.showQuickPick(entries, { placeHolder: 'Select a log file to view' });
        if (!selected) {
            return;
        }
        const selectedValue = logs[entries.indexOf(selected)];
        const fileName = typeof selectedValue === 'string'
            ? selectedValue
            : String(selectedValue['name'] ?? selectedValue['id'] ?? '');
        if (!fileName) {
            throw new Error('The selected log entry did not include a file name.');
        }
        const content = await new FlinkClient_1.FlinkClient(profile).get(`${path}/${encodeURIComponent(fileName)}`);
        await this.showJson(fileName, content);
    }
    async runOperation(node) {
        const savedProfile = node.profile ? this.store.get(node.profile.id) : undefined;
        const operation = node.operation;
        if (!savedProfile || !operation) {
            if (node.profile && !savedProfile) {
                this.showError('Cluster no longer exists', new Error('Refresh the Flink view and try again.'));
            }
            return;
        }
        const profile = savedProfile;
        if (this.isReadOnly(profile)) {
            this.showError('Operations are disabled for this read-only cluster', new Error('Edit the cluster and enable operations to proceed.'));
            return;
        }
        try {
            await this.executeOperation(profile, operation);
            this.refresh();
        }
        catch (error) {
            this.showError(`Could not ${String(operation.type).replace(/[A-Z]/g, letter => ` ${letter.toLowerCase()}`)}`, error);
        }
    }
    async executeOperation(profile, operation) {
        const client = new FlinkClient_1.FlinkClient(profile);
        switch (operation.type) {
            case 'cancelJob':
                if (await this.confirm(`Cancel job "${operation.jobName}"?`, 'Cancel Job', true)) {
                    await client.patch(`/jobs/${encodeURIComponent(operation.jobId)}?mode=cancel`);
                    await vscode.window.showInformationMessage(`Cancellation requested for "${operation.jobName}".`);
                }
                return;
            case 'savepoint':
                await this.triggerSavepoint(client, profile, operation);
                return;
            case 'checkpoint':
                if (await this.confirm(`Trigger a checkpoint for "${operation.jobName}"?`, 'Trigger', true)) {
                    const prefix = `/jobs/${encodeURIComponent(operation.jobId)}`;
                    const result = await client.post(`${prefix}/checkpoints`, {});
                    const triggerId = AsyncOperationTracker_1.AsyncOperationTracker.getTriggerId(result);
                    await new AsyncOperationTracker_1.AsyncOperationTracker(client).track('Triggering checkpoint', `${prefix}/checkpoints/${encodeURIComponent(triggerId)}`);
                    await vscode.window.showInformationMessage('Checkpoint completed.');
                }
                return;
            case 'rescale':
                await this.rescaleJob(client, operation);
                return;
            case 'uploadJar':
                await this.uploadJar(client);
                return;
            case 'submitJar':
                await this.submitJar(client, operation.jarId);
                return;
            case 'deleteJar':
                if (await this.confirm(`Delete JAR "${operation.jarName}"?`, 'Delete JAR', true)) {
                    await client.delete(`/jars/${encodeURIComponent(operation.jarId)}`);
                    await vscode.window.showInformationMessage(`Deleted JAR "${operation.jarName}".`);
                }
                return;
            case 'disposeSavepoint':
                await this.disposeSavepoint(client, profile, operation.path);
        }
    }
    async triggerSavepoint(client, profile, operation) {
        const destination = await vscode.window.showInputBox({
            prompt: 'Target directory (leave blank to use the cluster default)',
            placeHolder: 'file:///path/to/savepoints'
        });
        if (destination === undefined) {
            return;
        }
        const label = operation.stop ? 'Stop job with savepoint' : 'Trigger savepoint';
        if (!await this.confirm(`${label} for "${operation.jobName}"?`, 'Continue', operation.stop ?? false)) {
            return;
        }
        const jobPath = `/jobs/${encodeURIComponent(operation.jobId)}`;
        const triggerId = (0, crypto_1.randomUUID)();
        const requestBody = {
            triggerId,
            cancelJob: false,
            drain: operation.stop
        };
        if (destination.trim()) {
            requestBody['targetDirectory'] = destination.trim();
        }
        const response = await client.post(`${jobPath}/${operation.stop ? 'stop' : 'savepoints'}`, requestBody);
        const returnedId = this.triggerIdFromResult(response, triggerId);
        const result = await new AsyncOperationTracker_1.AsyncOperationTracker(client).track(operation.stop ? 'Stopping job with savepoint' : 'Creating savepoint', `${jobPath}/savepoints/${encodeURIComponent(returnedId)}`);
        const savepointPath = this.findSavepointPath(result);
        if (savepointPath) {
            await this.rememberSavepoint(profile, operation.jobName, savepointPath);
            await vscode.window.showInformationMessage(`Savepoint created: ${savepointPath}`);
        }
        else {
            await this.showJson('Savepoint Result', result);
        }
    }
    async rescaleJob(client, operation) {
        const value = await vscode.window.showInputBox({
            prompt: `New parallelism for "${operation.jobName}"`,
            validateInput: input => /^\d+$/.test(input) && Number(input) > 0
                ? undefined
                : 'Enter a positive whole number.'
        });
        if (value === undefined || !await this.confirm(`Rescale "${operation.jobName}" to parallelism ${value}?`, 'Rescale', true)) {
            return;
        }
        const prefix = `/jobs/${encodeURIComponent(operation.jobId)}/rescaling`;
        const response = await client.patchQuery(prefix, { parallelism: value });
        const triggerId = AsyncOperationTracker_1.AsyncOperationTracker.getTriggerId(response);
        const result = await new AsyncOperationTracker_1.AsyncOperationTracker(client).track('Rescaling job', `${prefix}/${encodeURIComponent(triggerId)}`);
        await this.showJson('Rescale Result', result);
    }
    async uploadJar(client) {
        const files = await vscode.window.showOpenDialog({
            canSelectFiles: true,
            canSelectFolders: false,
            canSelectMany: false,
            filters: { 'Flink JAR': ['jar'] },
            openLabel: 'Upload JAR'
        });
        if (!files?.length) {
            return;
        }
        const fileName = files[0].path.split(/[\\/]/).pop() || 'job.jar';
        const content = await vscode.workspace.fs.readFile(files[0]);
        const result = await client.uploadJar(fileName, content);
        await vscode.window.showInformationMessage('JAR uploaded.');
        await this.showJson('Uploaded JAR', result);
    }
    async submitJar(client, jarId) {
        const entryClass = await vscode.window.showInputBox({ prompt: 'Entry class (optional)' });
        if (entryClass === undefined) {
            return;
        }
        const programArgs = await vscode.window.showInputBox({ prompt: 'Program arguments (optional)' });
        if (programArgs === undefined) {
            return;
        }
        const parallelism = await vscode.window.showInputBox({
            prompt: 'Parallelism (optional)',
            validateInput: input => !input || (/^\d+$/.test(input) && Number(input) > 0)
                ? undefined
                : 'Enter a positive whole number.'
        });
        if (parallelism === undefined) {
            return;
        }
        const savepointPath = await vscode.window.showInputBox({ prompt: 'Restore from savepoint (optional)' });
        if (savepointPath === undefined) {
            return;
        }
        const allowNonRestoredState = await vscode.window.showQuickPick(['No', 'Yes'], { placeHolder: 'Allow non-restored state?' });
        if (!allowNonRestoredState) {
            return;
        }
        if (!await this.confirm(`Submit JAR "${jarId}"?`, 'Submit', true)) {
            return;
        }
        const params = {};
        if (entryClass.trim()) {
            params['entry-class'] = entryClass.trim();
        }
        if (programArgs.trim()) {
            params['program-args'] = programArgs.trim();
        }
        if (parallelism.trim()) {
            params['parallelism'] = parallelism.trim();
        }
        if (savepointPath.trim()) {
            params['savepointPath'] = savepointPath.trim();
        }
        params['allowNonRestoredState'] = String(allowNonRestoredState === 'Yes');
        const result = await client.postQuery(`/jars/${encodeURIComponent(jarId)}/run`, params);
        await vscode.window.showInformationMessage('Job submitted.');
        await this.showJson('Submitted Job', result);
    }
    async disposeSavepoint(client, profile, existingPath) {
        const savepoints = this.context.globalState.get(this.savepointKey(profile), []);
        const path = existingPath ?? await vscode.window.showQuickPick([...new Set(savepoints.map(savepoint => savepoint.path)), 'Enter another path...'], { placeHolder: 'Select a savepoint to dispose' });
        if (!path) {
            return;
        }
        let targetPath = path;
        if (path === 'Enter another path...') {
            const entered = await vscode.window.showInputBox({ prompt: 'Savepoint path to dispose' });
            if (!entered?.trim()) {
                return;
            }
            targetPath = entered.trim();
        }
        if (!await this.confirm(`Permanently dispose savepoint "${targetPath}"?`, 'Dispose', true)) {
            return;
        }
        const triggerId = (0, crypto_1.randomUUID)();
        const response = await client.post('/savepoint-disposal', {
            triggerId,
            savepointPath: targetPath
        });
        const returnedId = this.triggerIdFromResult(response, triggerId);
        const result = await new AsyncOperationTracker_1.AsyncOperationTracker(client).track('Disposing savepoint', `/savepoint-disposal/${encodeURIComponent(returnedId)}`);
        await this.context.globalState.update(this.savepointKey(profile), savepoints.filter(savepoint => savepoint.path !== targetPath));
        await this.showJson('Savepoint Disposal Result', result);
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
        const readOnly = await this.chooseReadOnly(true);
        if (readOnly === undefined) {
            return;
        }
        try {
            await this.store.add(name.trim(), baseUrl.trim(), readOnly);
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
        const readOnly = await this.chooseReadOnly(profile.readOnly ?? true);
        if (readOnly === undefined) {
            return;
        }
        try {
            await this.store.update(profile.id, name.trim(), baseUrl.trim(), readOnly);
            this.refresh();
        }
        catch (error) {
            this.showError('Could not update Flink cluster', error);
        }
    }
    async chooseReadOnly(defaultReadOnly = true) {
        const selected = await vscode.window.showQuickPick([
            { label: 'Read-only', description: 'Block all cluster mutations', readOnly: true },
            { label: 'Allow operations', description: 'Enable job, checkpoint, and JAR operations', readOnly: false }
        ], {
            placeHolder: `Choose cluster access mode (default: ${defaultReadOnly ? 'Read-only' : 'Allow operations'})`,
            matchOnDescription: true
        });
        return selected?.readOnly ?? (selected ? false : undefined);
    }
    async removeCluster(node) {
        const profile = node.profile;
        if (!profile || !await this.confirm(`Remove cluster "${profile.name}"?`, 'Remove', true)) {
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
    async confirm(message, action, modal = false) {
        const selected = modal
            ? await vscode.window.showWarningMessage(message, { modal: true }, action)
            : await vscode.window.showWarningMessage(message, action);
        return selected === action;
    }
    async showJson(title, data) {
        const content = typeof data === 'string' ? data : JSON.stringify(data, null, 2) ?? String(data);
        const document = await vscode.workspace.openTextDocument({ language: 'json', content });
        await vscode.window.showTextDocument(document, { preview: false, viewColumn: vscode.ViewColumn.One });
        void title;
    }
    async rememberSavepoint(profile, jobName, path) {
        const key = this.savepointKey(profile);
        const existing = this.context.globalState.get(key, []);
        await this.context.globalState.update(key, [
            { path, jobName, createdAt: Date.now() },
            ...existing.filter(savepoint => savepoint.path !== path)
        ].slice(0, 100));
    }
    findSavepointPath(result) {
        const findPath = (value) => {
            if (Array.isArray(value)) {
                for (const entry of value) {
                    const match = findPath(entry);
                    if (match) {
                        return match;
                    }
                }
                return undefined;
            }
            if (typeof value !== 'object' || value === null) {
                return undefined;
            }
            const record = value;
            for (const key of ['location', 'savepoint-path', 'savepointPath', 'external-path', 'externalPath']) {
                if (typeof record[key] === 'string') {
                    return record[key];
                }
            }
            for (const child of Object.values(record)) {
                const match = findPath(child);
                if (match) {
                    return match;
                }
            }
            return undefined;
        };
        return findPath(result);
    }
    triggerIdFromResult(result, fallback) {
        try {
            return AsyncOperationTracker_1.AsyncOperationTracker.getTriggerId(result);
        }
        catch {
            return fallback;
        }
    }
    node(label, nodeType, profile, values = {}) {
        const item = new FlinkTreeNode(label, vscode.TreeItemCollapsibleState.Collapsed, nodeType, profile, undefined, undefined, values.jobId, values.jobName, values.vertexId, values.taskManagerId, values.jarId, undefined, values.checkpointId, values.savepointPath, values.jarName);
        if (values.savepointPath) {
            item.command = {
                command: 'ApacheFlink.RunOperation',
                title: 'Dispose Savepoint',
                arguments: [this.operationNode('Dispose Savepoint', profile, {
                        type: 'disposeSavepoint',
                        path: values.savepointPath
                    })]
            };
        }
        return item;
    }
    resourceNode(label, profile, path, title, kind, resourceOverride) {
        const resource = resourceOverride ?? { path: path ?? '', title, kind };
        const item = new FlinkTreeNode(label, vscode.TreeItemCollapsibleState.None, 'resource', profile, resource);
        item.command = {
            command: 'ApacheFlink.OpenResource',
            title: `Open ${label}`,
            arguments: [item]
        };
        item.iconPath = new vscode.ThemeIcon(kind === 'metrics' ? 'graph' : 'file');
        return item;
    }
    operationNode(label, profile, operation) {
        const item = new FlinkTreeNode(label, vscode.TreeItemCollapsibleState.None, 'operation', profile, undefined, operation);
        item.command = {
            command: 'ApacheFlink.RunOperation',
            title: label,
            arguments: [item]
        };
        item.iconPath = new vscode.ThemeIcon('play');
        return item;
    }
    messageNode(message) {
        return new FlinkTreeNode(message, vscode.TreeItemCollapsibleState.None, 'message');
    }
    async validateUrl(value) {
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
    isReadOnly(profile) {
        return profile.readOnly ?? true;
    }
    requireProfile(node) {
        if (!node.profile) {
            throw new Error('Cluster profile not found.');
        }
        return node.profile;
    }
    requireJobId(node) {
        if (!node.jobId) {
            throw new Error('Job ID not found.');
        }
        return node.jobId;
    }
    stateIcon(state) {
        if (state === 'RUNNING') {
            return new vscode.ThemeIcon('play', new vscode.ThemeColor('charts.green'));
        }
        if (state === 'FAILED') {
            return new vscode.ThemeIcon('error', new vscode.ThemeColor('charts.red'));
        }
        if (state === 'CANCELED' || state === 'CANCELLING') {
            return new vscode.ThemeIcon('circle-slash');
        }
        return new vscode.ThemeIcon('circle-outline');
    }
    isJobRunning(description) {
        return description === 'RUNNING';
    }
    savepointKey(profile) {
        return `flink.savepoints.${profile.id}`;
    }
    errorMessage(error) {
        return error instanceof Error ? error.message : String(error);
    }
    showError(message, error) {
        void vscode.window.showErrorMessage(`${message}: ${this.errorMessage(error)}`);
    }
}
exports.FlinkTreeProvider = FlinkTreeProvider;
//# sourceMappingURL=FlinkTreeProvider.js.map