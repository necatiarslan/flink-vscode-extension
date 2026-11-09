import * as vscode from 'vscode';
import { randomUUID } from 'crypto';
import { AsyncOperationTracker } from '../flink/AsyncOperationTracker';
import { ClusterStore } from '../flink/ClusterStore';
import { FlinkApiError, FlinkClient } from '../flink/FlinkClient';
import { FlinkDetailPanel, FlinkPanelType } from '../flink/FlinkDetailPanel';
import { MetricChartPanel } from '../flink/MetricChartPanel';
import {
    ClusterProfile,
    FlinkAsyncStatus,
    FlinkCheckpoints,
    FlinkJobDetail,
    FlinkJobsOverview,
    FlinkJars,
    FlinkMetric,
    FlinkTaskManagers
} from '../flink/types';

type Resource = {
    path: string;
    title: string;
    kind?: 'metrics' | 'logList' | 'clusterOverview';
    viewType: FlinkPanelType;
};

type Operation =
    | { type: 'cancelJob'; jobId: string; jobName: string }
    | { type: 'savepoint'; jobId: string; jobName: string; stop?: boolean }
    | { type: 'restartFromSavepoint'; savepointPath: string }
    | { type: 'checkpoint'; jobId: string; jobName: string }
    | { type: 'rescale'; jobId: string; jobName: string }
    | { type: 'uploadJar' }
    | { type: 'planJar'; jarId: string }
    | { type: 'submitJar'; jarId: string }
    | { type: 'submitApplication'; jarId: string }
    | { type: 'deleteJar'; jarId: string; jarName: string }
    | { type: 'disposeSavepoint'; path?: string }
    | { type: 'updateResourceRequirements'; jobId: string; jobName: string }
    | { type: 'rescaleDetails'; jobId: string; jobName: string }
    | { type: 'jobTaskManagerLogUrl'; jobId: string; jobName: string }
    | {
        type: 'subtaskRequest';
        jobId: string;
        vertexId: string;
        vertexName: string;
        request: 'details' | 'metrics' | 'attempt' | 'attemptAccumulators';
    }
    | {
        type: 'metricsRequest';
        path: string;
        title: string;
        dimension?: 'jobs' | 'taskmanagers' | 'subtasks';
    };

interface SavedSavepoint {
    path: string;
    jobName: string;
    createdAt: number;
}

export class FlinkTreeNode extends vscode.TreeItem {
    constructor(
        label: string,
        collapsibleState: vscode.TreeItemCollapsibleState,
        public readonly nodeType: string,
        public readonly profile?: ClusterProfile,
        public readonly resource?: Resource,
        public readonly operation?: Operation,
        public readonly jobId?: string,
        public readonly jobName?: string,
        public readonly vertexId?: string,
        public readonly taskManagerId?: string,
        public readonly jarId?: string,
        public readonly childNodes?: FlinkTreeNode[],
        public readonly checkpointId?: number,
        public readonly savepointPath?: string,
        public readonly jarName?: string,
        public refreshNode?: FlinkTreeNode
    ) {
        super(label, collapsibleState);
    }
}

export class FlinkTreeProvider implements vscode.TreeDataProvider<FlinkTreeNode> {
    private readonly changes = new vscode.EventEmitter<FlinkTreeNode | undefined | null | void>();
    public readonly onDidChangeTreeData = this.changes.event;

    constructor(
        private readonly context: vscode.ExtensionContext,
        private readonly store: ClusterStore
    ) {
        context.subscriptions.push(this.changes);
        context.subscriptions.push(
            vscode.commands.registerCommand('ApacheFlink.AddCluster', () => this.addCluster()),
            vscode.commands.registerCommand('ApacheFlink.EditCluster', (node: FlinkTreeNode) => this.editCluster(node)),
            vscode.commands.registerCommand('ApacheFlink.RemoveCluster', (node: FlinkTreeNode) => this.removeCluster(node)),
            vscode.commands.registerCommand('ApacheFlink.TestConnection', (node: FlinkTreeNode) => this.testConnection(node)),
            vscode.commands.registerCommand('ApacheFlink.Refresh', () => this.refresh()),
            vscode.commands.registerCommand('ApacheFlink.OpenResource', (node: FlinkTreeNode) => this.openResource(node)),
            vscode.commands.registerCommand('ApacheFlink.RunOperation', (node: FlinkTreeNode) => this.runOperation(node)),
            vscode.commands.registerCommand(
                '_ApacheFlink.UploadJar#sideBar',
                (node: FlinkTreeNode) => this.uploadJarFromNode(node)
            )
        );
    }

    public getTreeItem(element: FlinkTreeNode): vscode.TreeItem {
        return element;
    }

    public async getChildren(element?: FlinkTreeNode): Promise<FlinkTreeNode[]> {
        if (!element) {
            return this.store.getAll().map(profile => {
                const item = new FlinkTreeNode(profile.name, vscode.TreeItemCollapsibleState.Collapsed, 'cluster', profile);
                item.description = profile.baseUrl;
                item.contextValue = 'flinkCluster';
                item.iconPath = new vscode.ThemeIcon('server');
                return item;
            });
        }

        try {
            return await this.getNodeChildren(element);
        } catch (error) {
            return [this.messageNode(`Unable to load: ${this.errorMessage(error)}`)];
        }
    }

    public refresh(node?: FlinkTreeNode): void {
        this.changes.fire(node);
    }

    private async getNodeChildren(node: FlinkTreeNode): Promise<FlinkTreeNode[]> {
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
                return this.getJars(profile, node);
            case 'jar':
                return this.jarActions(node);
            case 'savepoints':
                return this.getSavepoints(profile);
            case 'savepoint':
                return this.savepointActions(node);
            case 'checkpointDetails':
                return this.getCheckpointDetails(node);
            default:
                return [];
        }
    }

    private async clusterChildren(profile: ClusterProfile): Promise<FlinkTreeNode[]> {
        const client = new FlinkClient(profile);
        let includeJars = true;
        try {
            await client.get('/jars');
        } catch (error) {
            if (error instanceof FlinkApiError && error.statusCode === 404) {
                includeJars = false;
            } else {
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
            const jarsNode = this.node('Jars', 'jars', profile);
            jarsNode.contextValue = 'flinkJarsWritable';
            children.push(jarsNode);
        }
        return children;
    }

    private async getJobGroups(profile: ClusterProfile): Promise<FlinkTreeNode[]> {
        const result = await new FlinkClient(profile).get<FlinkJobsOverview>('/jobs/overview');
        const jobs = result.jobs ?? [];
        if (jobs.length === 0) {
            return [
                this.messageNode('No jobs'),
                this.resourceNode('Job IDs and statuses', profile, '/jobs', 'Job IDs and Statuses'),
                this.operationNode('Aggregate Job Metrics...', profile, {
                    type: 'metricsRequest',
                    path: '/jobs/metrics',
                    title: 'Aggregate Job Metrics',
                    dimension: 'jobs'
                })
            ];
        }

        const groups = new Map<string, FlinkTreeNode[]>();
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

        return [
            ...[...groups.entries()].map(([state, groupedJobs]) => {
                const group = this.node(`${state} (${groupedJobs.length})`, 'jobGroup', profile);
                group.collapsibleState = vscode.TreeItemCollapsibleState.Expanded;
                Object.defineProperty(group, 'childNodes', { value: groupedJobs });
                return group;
            }),
            this.resourceNode('Job IDs and statuses', profile, '/jobs', 'Job IDs and Statuses'),
            this.operationNode('Aggregate Job Metrics...', profile, {
                type: 'metricsRequest',
                path: '/jobs/metrics',
                title: 'Aggregate Job Metrics',
                dimension: 'jobs'
            })
        ];
    }

    private async getJobChildren(node: FlinkTreeNode): Promise<FlinkTreeNode[]> {
        const profile = this.requireProfile(node);
        const jobId = this.requireJobId(node);
        const jobName = node.jobName ?? jobId;
        const prefix = `/jobs/${encodeURIComponent(jobId)}`;
        const detail = await new FlinkClient(profile).get<FlinkJobDetail>(prefix);
        const children = [
            this.resourceNode('Overview', profile, prefix, 'Job Overview'),
            this.resourceNode('Plan', profile, `${prefix}/plan`, 'Execution Plan'),
            this.resourceNode('Exceptions', profile, `${prefix}/exceptions`, 'Job Exceptions'),
            this.resourceNode('Configuration', profile, `${prefix}/config`, 'Job Configuration'),
            this.resourceNode('Metrics Chart', profile, `${prefix}/metrics`, 'Job Metrics', 'metrics'),
            this.resourceNode('Status', profile, `${prefix}/status`, 'Job Status'),
            this.resourceNode('Accumulators', profile, `${prefix}/accumulators`, 'Job Accumulators'),
            this.resourceNode('Execution Result', profile, `${prefix}/execution-result`, 'Job Execution Result'),
            this.resourceNode('JobManager Configuration', profile, `${prefix}/jobmanager/config`, 'JobManager Configuration'),
            this.resourceNode('JobManager Environment', profile, `${prefix}/jobmanager/environment`, 'JobManager Environment'),
            this.resourceNode('JobManager Log URL', profile, `${prefix}/jobmanager/log-url`, 'JobManager Log URL'),
            this.operationNode('Task Manager Log URL...', profile, {
                type: 'jobTaskManagerLogUrl',
                jobId,
                jobName
            }),
            this.resourceNode('Resource Requirements', profile, `${prefix}/resource-requirements`, 'Job Resource Requirements'),
            this.operationNode('Update Resource Requirements...', profile, {
                type: 'updateResourceRequirements',
                jobId,
                jobName
            }),
            this.resourceNode('Rescale Configuration', profile, `${prefix}/rescales/config`, 'Rescale Configuration'),
            this.resourceNode('Rescale History', profile, `${prefix}/rescales/history`, 'Rescale History'),
            this.resourceNode('Rescale Overview', profile, `${prefix}/rescales/overview`, 'Rescale Overview'),
            this.resourceNode('Rescale Summary', profile, `${prefix}/rescales/summary`, 'Rescale Summary'),
            this.operationNode('Rescale Details...', profile, {
                type: 'rescaleDetails',
                jobId,
                jobName
            }),
            this.node('Checkpoints', 'checkpoints', profile, { jobId, jobName }),
            this.node('Vertices', 'vertices', profile, { jobId, jobName })
        ];

        if (this.isJobRunning(node.description)) {
            children.push(
                this.operationNode('Cancel Job', profile, { type: 'cancelJob', jobId, jobName }),
                this.operationNode('Trigger Savepoint', profile, { type: 'savepoint', jobId, jobName }),
                this.operationNode('Stop with Savepoint', profile, { type: 'savepoint', jobId, jobName, stop: true }),
                this.operationNode('Trigger Checkpoint', profile, { type: 'checkpoint', jobId, jobName }),
                this.operationNode('Rescale Job', profile, { type: 'rescale', jobId, jobName })
            );
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

    private async getCheckpointChildren(node: FlinkTreeNode): Promise<FlinkTreeNode[]> {
        const profile = this.requireProfile(node);
        const jobId = this.requireJobId(node);
        const result = await new FlinkClient(profile).get<FlinkCheckpoints>(`/jobs/${encodeURIComponent(jobId)}/checkpoints`);
        const records = result.history ?? Object.values(result.latest ?? {}).filter(
            value => typeof value === 'object' && value !== null && 'id' in value
        ) as Array<{ id: number; status?: string }>;
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

    private async getCheckpointDetails(node: FlinkTreeNode): Promise<FlinkTreeNode[]> {
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

    private async getCheckpointVertexSubtasks(
        profile: ClusterProfile,
        jobId: string,
        checkpointId: number
    ): Promise<FlinkTreeNode[]> {
        const job = await new FlinkClient(profile).get<FlinkJobDetail>(`/jobs/${encodeURIComponent(jobId)}`);
        return (job.vertices ?? []).map(vertex => this.resourceNode(
            `Subtasks: ${vertex.name || vertex.id}`,
            profile,
            `/jobs/${encodeURIComponent(jobId)}/checkpoints/details/${checkpointId}/subtasks/${encodeURIComponent(vertex.id)}`,
            `Checkpoint ${checkpointId} Subtasks`
        ));
    }

    private async getVertexChildren(node: FlinkTreeNode): Promise<FlinkTreeNode[]> {
        if (node.childNodes) {
            return node.childNodes;
        }
        const profile = this.requireProfile(node);
        const jobId = this.requireJobId(node);
        const job = await new FlinkClient(profile).get<FlinkJobDetail>(`/jobs/${encodeURIComponent(jobId)}`);
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

    private getVertexDiagnostics(node: FlinkTreeNode): FlinkTreeNode[] {
        const profile = this.requireProfile(node);
        const jobId = this.requireJobId(node);
        const vertexId = node.vertexId;
        if (!vertexId) {
            return [];
        }
        const prefix = `/jobs/${encodeURIComponent(jobId)}/vertices/${encodeURIComponent(vertexId)}`;
        return [
            this.resourceNode('Details', profile, prefix, 'Vertex Details'),
            this.resourceNode('Accumulators', profile, `${prefix}/accumulators`, 'Vertex Accumulators'),
            this.operationNode('JobManager Operator Metrics...', profile, {
                type: 'metricsRequest',
                path: `${prefix}/jm-operator-metrics`,
                title: 'JobManager Operator Metrics'
            }),
            this.resourceNode('Backpressure', profile, `${prefix}/backpressure`, 'Backpressure'),
            this.resourceNode('Flamegraph', profile, `${prefix}/flamegraph`, 'Flamegraph'),
            this.resourceNode('Watermarks', profile, `${prefix}/watermarks`, 'Watermarks'),
            this.resourceNode('Subtask Times', profile, `${prefix}/subtasktimes`, 'Subtask Times'),
            this.resourceNode('Task Managers', profile, `${prefix}/taskmanagers`, 'Vertex Task Managers'),
            this.resourceNode('Metrics Chart', profile, `${prefix}/metrics`, 'Vertex Metrics', 'metrics'),
            this.resourceNode('Subtask Accumulators', profile, `${prefix}/subtasks/accumulators`, 'Subtask Accumulators'),
            this.operationNode('Aggregate Subtask Metrics...', profile, {
                type: 'metricsRequest',
                path: `${prefix}/subtasks/metrics`,
                title: 'Aggregate Subtask Metrics',
                dimension: 'subtasks'
            }),
            this.operationNode('Subtask Details...', profile, {
                type: 'subtaskRequest',
                jobId,
                vertexId,
                vertexName: String(node.label),
                request: 'details'
            }),
            this.operationNode('Subtask Metrics...', profile, {
                type: 'subtaskRequest',
                jobId,
                vertexId,
                vertexName: String(node.label),
                request: 'metrics'
            }),
            this.operationNode('Subtask Attempt Details...', profile, {
                type: 'subtaskRequest',
                jobId,
                vertexId,
                vertexName: String(node.label),
                request: 'attempt'
            }),
            this.operationNode('Subtask Attempt Accumulators...', profile, {
                type: 'subtaskRequest',
                jobId,
                vertexId,
                vertexName: String(node.label),
                request: 'attemptAccumulators'
            })
        ];
    }

    private async getTaskManagers(profile: ClusterProfile): Promise<FlinkTreeNode[]> {
        const result = await new FlinkClient(profile).get<FlinkTaskManagers>('/taskmanagers');
        const managers = (result.taskmanagers ?? []).map(manager => {
            const item = this.node(manager.id, 'taskManager', profile, { taskManagerId: manager.id });
            const used = Math.max(0, (manager.slotsNumber ?? 0) - (manager.freeSlots ?? 0));
            item.description = `${used}/${manager.slotsNumber ?? 0} slots`;
            item.iconPath = new vscode.ThemeIcon('server');
            return item;
        });
        return [
            this.operationNode('Aggregate Task Manager Metrics...', profile, {
                type: 'metricsRequest',
                path: '/taskmanagers/metrics',
                title: 'Aggregate Task Manager Metrics',
                dimension: 'taskmanagers'
            }),
            ...(managers.length ? managers : [this.messageNode('No task managers')])
        ];
    }

    private getTaskManagerChildren(node: FlinkTreeNode): FlinkTreeNode[] {
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

    private getJobManagerChildren(profile: ClusterProfile): FlinkTreeNode[] {
        return [
            this.resourceNode('Configuration', profile, '/jobmanager/config', 'JobManager Configuration'),
            this.resourceNode('Environment', profile, '/jobmanager/environment', 'JobManager Environment'),
            this.resourceNode('Logs', profile, '/jobmanager/logs', 'JobManager Logs', 'logList'),
            this.resourceNode('Metrics Chart', profile, '/jobmanager/metrics', 'JobManager Metrics', 'metrics'),
            this.resourceNode('Thread Dump', profile, '/jobmanager/thread-dump', 'JobManager Thread Dump')
        ];
    }

    private async getJars(profile: ClusterProfile, jarsNode: FlinkTreeNode): Promise<FlinkTreeNode[]> {
        const result = await new FlinkClient(profile).get<FlinkJars>('/jars');
        const jars = result.files ?? [];
        const nodes = jars.map(jar => {
            const item = this.node(jar.name || jar.id, 'jar', profile, {
                jarId: jar.id,
                jarName: jar.name || jar.id
            });
            item.refreshNode = jarsNode;
            item.description = jar.entry;
            item.tooltip = `Jar ID: ${jar.id}`;
            return item;
        });
        return nodes.length ? nodes : [this.messageNode('No uploaded JARs')];
    }

    private async uploadJarFromNode(node: FlinkTreeNode): Promise<void> {
        if (!node.profile) {
            return;
        }
        const operationNode = this.operationNode('Upload JAR', node.profile, { type: 'uploadJar' });
        operationNode.refreshNode = node;
        await this.runOperation(operationNode);
    }

    private jarActions(node: FlinkTreeNode): FlinkTreeNode[] {
        const profile = this.requireProfile(node);
        const jarId = node.jarId;
        if (!jarId) {
            return [];
        }
        const children = [this.operationNode('Plan', profile, { type: 'planJar', jarId })];
        children.push(
            this.operationNode('Submit JAR', profile, { type: 'submitJar', jarId }),
            this.operationNode('Submit Application...', profile, { type: 'submitApplication', jarId }),
            this.operationNode(
                'Delete JAR',
                profile,
                {
                    type: 'deleteJar',
                    jarId,
                    jarName: String(node.label)
                },
                node.refreshNode
            )
        );
        return children;
    }

    private getSavepoints(profile: ClusterProfile): FlinkTreeNode[] {
        const savepoints = this.context.globalState.get<SavedSavepoint[]>(this.savepointKey(profile), []);
        const children = savepoints.map(savepoint => {
            const item = new FlinkTreeNode(
                savepoint.jobName,
                vscode.TreeItemCollapsibleState.Collapsed,
                'savepoint',
                profile,
                { path: savepoint.path, title: 'Savepoint', viewType: 'savepoint' },
                undefined,
                undefined,
                undefined,
                undefined,
                undefined,
                undefined,
                undefined,
                undefined,
                savepoint.path
            );
            item.description = savepoint.path;
            item.tooltip = new Date(savepoint.createdAt).toLocaleString();
            item.contextValue = 'flinkSavepoint';
            item.command = {
                command: 'ApacheFlink.OpenResource',
                title: 'Open Savepoint Details',
                arguments: [item]
            };
            return item;
        });
        children.unshift(this.operationNode('Dispose Savepoint...', profile, { type: 'disposeSavepoint' }));
        if (!savepoints.length) {
            children.push(this.messageNode('No tracked savepoints'));
        }
        return children;
    }

    private savepointActions(node: FlinkTreeNode): FlinkTreeNode[] {
        const profile = this.requireProfile(node);
        const savepointPath = node.savepointPath;
        if (!savepointPath) {
            return [];
        }
        return [
            this.operationNode('Restart Job from Savepoint...', profile, {
                type: 'restartFromSavepoint',
                savepointPath
            })
        ];
    }

    private async openResource(node: FlinkTreeNode): Promise<void> {
        const profile = node.profile;
        const resource = node.resource;
        if (!profile || !resource) {
            return;
        }
        try {
            if (resource.viewType === 'savepoint') {
                const savepoint = this.context.globalState
                    .get<SavedSavepoint[]>(this.savepointKey(profile), [])
                    .find(item => item.path === resource.path);
                if (!savepoint) {
                    throw new Error('This tracked savepoint no longer exists.');
                }
                await FlinkDetailPanel.open(profile.id, resource.path, resource.title, 'savepoint', savepoint);
                return;
            }
            if (resource.kind === 'clusterOverview') {
                const client = new FlinkClient(profile);
                const [overview, config, jobManagerConfig, metrics] = await Promise.all([
                    client.get<unknown>('/overview'),
                    client.get<unknown>('/config'),
                    client.get<unknown>('/jobmanager/config'),
                    client.get<unknown>('/jobmanager/metrics')
                ]);
                await FlinkDetailPanel.open(profile.id, 'overview', resource.title, 'overview', {
                    overview,
                    config,
                    jobManagerConfig,
                    metrics
                });
                return;
            }
            if (resource.kind === 'metrics') {
                await this.openMetrics(profile, resource.path);
                return;
            }
            if (!resource.path) {
                return;
            }
            const client = new FlinkClient(profile);
            const data = await client.get<unknown>(resource.path);
            if (resource.kind === 'logList') {
                await this.openLog(profile, resource, data);
            } else {
                await FlinkDetailPanel.open(
                    profile.id,
                    resource.path,
                    resource.title,
                    resource.viewType,
                    data
                );
            }
        } catch (error) {
            this.showError(`Could not load ${resource.title}`, error);
        }
    }

    private async openMetrics(profile: ClusterProfile, path: string): Promise<void> {
        const metrics = await new FlinkClient(profile).get<FlinkMetric[]>(path);
        if (!metrics.length) {
            await vscode.window.showInformationMessage('No metrics are available for this resource.');
            return;
        }
        MetricChartPanel.open(profile.id, new FlinkClient(profile), path, metrics);
    }

    private async openLog(profile: ClusterProfile, resource: Resource, data: unknown): Promise<void> {
        const logs = Array.isArray(data)
            ? data
            : typeof data === 'object' && data !== null && Array.isArray((data as { logs?: unknown[] }).logs)
                ? (data as { logs: unknown[] }).logs
                : [];
        if (!logs.length) {
            await FlinkDetailPanel.open(
                profile.id,
                resource.path,
                resource.title,
                'logs',
                { logs: Array.isArray(data) ? data : (data as { logs?: unknown[] }).logs ?? [] }
            );
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
            : String((selectedValue as Record<string, unknown>)['name'] ?? (selectedValue as Record<string, unknown>)['id'] ?? '');
        if (!fileName) {
            throw new Error('The selected log entry did not include a file name.');
        }
        const path = `${resource.path}/${encodeURIComponent(fileName)}`;
        const content = await new FlinkClient(profile).get<unknown>(path);
        await FlinkDetailPanel.open(profile.id, path, fileName, 'logs', content);
    }

    private async runOperation(node: FlinkTreeNode): Promise<void> {
        const savedProfile = node.profile ? this.store.get(node.profile.id) : undefined;
        const operation = node.operation;
        if (!savedProfile || !operation) {
            if (node.profile && !savedProfile) {
                this.showError('Cluster no longer exists', new Error('Refresh the Flink view and try again.'));
            }
            return;
        }
        const profile = savedProfile;
        try {
            await this.executeOperation(profile, operation);
            this.refresh(node.refreshNode);
        } catch (error) {
            this.showError(`Could not ${String(operation.type).replace(/[A-Z]/g, letter => ` ${letter.toLowerCase()}`)}`, error);
        }
    }

    private async executeOperation(profile: ClusterProfile, operation: Operation): Promise<void> {
        const client = new FlinkClient(profile);
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
                if (                await this.confirm(`Trigger a checkpoint for "${operation.jobName}"?`, 'Trigger', true)) {
                    const prefix = `/jobs/${encodeURIComponent(operation.jobId)}`;
                    const result = await client.post<Record<string, unknown>>(`${prefix}/checkpoints`, {});
                    const triggerId = AsyncOperationTracker.getTriggerId(result);
                    await new AsyncOperationTracker(client).track(
                        'Triggering checkpoint',
                        `${prefix}/checkpoints/${encodeURIComponent(triggerId)}`
                    );
                    await vscode.window.showInformationMessage('Checkpoint completed.');
                }
                return;
            case 'rescale':
                await this.rescaleJob(client, profile.id, operation);
                return;
            case 'uploadJar':
                await this.uploadJar(client, profile.id);
                return;
            case 'submitJar':
                await this.submitJar(client, profile.id, operation.jarId);
                return;
            case 'submitApplication':
                await this.submitApplication(client, profile.id, operation.jarId);
                return;
            case 'planJar':
                await this.planJar(client, profile.id, operation.jarId);
                return;
            case 'restartFromSavepoint':
                await this.restartFromSavepoint(client, operation.savepointPath);
                return;
            case 'deleteJar':
                if (await this.confirm(`Delete JAR "${operation.jarName}"?`, 'Delete JAR', true)) {
                    await client.delete(`/jars/${encodeURIComponent(operation.jarId)}`);
                    await vscode.window.showInformationMessage(`Deleted JAR "${operation.jarName}".`);
                }
                return;
            case 'disposeSavepoint':
                await this.disposeSavepoint(client, profile, operation.path);
                return;
            case 'updateResourceRequirements':
                await this.updateResourceRequirements(client, operation.jobId, operation.jobName);
                return;
            case 'rescaleDetails':
                await this.openRescaleDetails(client, profile.id, operation.jobId, operation.jobName);
                return;
            case 'jobTaskManagerLogUrl':
                await this.openJobTaskManagerLogUrl(client, profile.id, operation.jobId, operation.jobName);
                return;
            case 'subtaskRequest':
                await this.openSubtaskRequest(client, profile.id, operation);
                return;
            case 'metricsRequest':
                await this.openMetricsRequest(client, profile.id, operation);
        }
    }

    private async triggerSavepoint(
        client: FlinkClient,
        profile: ClusterProfile,
        operation: Extract<Operation, { type: 'savepoint' }>
    ): Promise<void> {
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
        const triggerId = randomUUID();
        const requestBody: Record<string, unknown> = operation.stop
            ? { triggerId, drain: true }
            : { triggerId, 'cancel-job': false };
        if (destination.trim()) {
            requestBody[operation.stop ? 'targetDirectory' : 'target-directory'] = destination.trim();
        }
        const response = await client.post<Record<string, unknown>>(
            `${jobPath}/${operation.stop ? 'stop' : 'savepoints'}`,
            requestBody
        );
        const returnedId = this.triggerIdFromResult(response, triggerId);
        const result = await new AsyncOperationTracker(client).track(
            operation.stop ? 'Stopping job with savepoint' : 'Creating savepoint',
            `${jobPath}/savepoints/${encodeURIComponent(returnedId)}`
        );
        const savepointPath = this.findSavepointPath(result);
        if (savepointPath) {
            await this.rememberSavepoint(profile, operation.jobName, savepointPath);
            await vscode.window.showInformationMessage(`Savepoint created: ${savepointPath}`);
        } else {
            await FlinkDetailPanel.open(profile.id, 'savepoint-result', 'Savepoint Result', 'savepoint', result);
        }
    }

    private async rescaleJob(
        client: FlinkClient,
        profileId: string,
        operation: Extract<Operation, { type: 'rescale' }>
    ): Promise<void> {
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
        const response = await client.patchQuery<Record<string, unknown>>(prefix, { parallelism: value });
        const triggerId = AsyncOperationTracker.getTriggerId(response);
        const result = await new AsyncOperationTracker(client).track(
            'Rescaling job',
            `${prefix}/${encodeURIComponent(triggerId)}`
        );
        await FlinkDetailPanel.open(
            profileId,
            `rescale:${operation.jobId}`,
            'Rescale Result',
            'job',
            result
        );
    }

    private async uploadJar(client: FlinkClient, profileId: string): Promise<void> {
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
        await FlinkDetailPanel.open(profileId, 'uploaded-jar', 'Uploaded JAR', 'jarPlan', result);
    }

    private async planJar(client: FlinkClient, profileId: string, jarId: string): Promise<void> {
        const result = await client.post<unknown>(
            `/jars/${encodeURIComponent(jarId)}/plan`,
            {}
        );
        await FlinkDetailPanel.open(profileId, `jar-plan:${jarId}`, 'JAR Plan', 'jarPlan', result);
    }

    private async submitApplication(client: FlinkClient, profileId: string, jarId: string): Promise<void> {
        const entryClass = await vscode.window.showInputBox({ prompt: 'Entry class (optional)' });
        if (entryClass === undefined) { return; }
        const programArgs = await vscode.window.showInputBox({ prompt: 'Program arguments (optional)' });
        if (programArgs === undefined) { return; }
        const parallelism = await vscode.window.showInputBox({
            prompt: 'Parallelism (optional)',
            validateInput: input => !input || (/^\d+$/.test(input) && Number(input) > 0)
                ? undefined
                : 'Enter a positive whole number.'
        });
        if (parallelism === undefined) { return; }
        const savepointPath = await vscode.window.showInputBox({ prompt: 'Savepoint path (optional)' });
        if (savepointPath === undefined) { return; }
        const allowNonRestoredState = await vscode.window.showQuickPick(['No', 'Yes'], {
            placeHolder: 'Allow non-restored state?'
        });
        if (allowNonRestoredState === undefined) { return; }
        const claimModes = [
            { label: 'Default (omit claim mode)', value: '' },
            { label: 'NO_CLAIM', value: 'NO_CLAIM' },
            { label: 'CLAIM', value: 'CLAIM' },
            { label: 'LEGACY', value: 'LEGACY' }
        ];
        const claimMode = await vscode.window.showQuickPick(claimModes, {
            placeHolder: 'Savepoint claim mode (optional)'
        });
        if (claimMode === undefined) { return; }

        const body: Record<string, unknown> = {
            allowNonRestoredState: allowNonRestoredState === 'Yes',
            programArgsList: programArgs.trim() ? programArgs.trim().split(/\s+/) : []
        };
        if (entryClass.trim()) { body['entryClass'] = entryClass.trim(); }
        if (parallelism.trim()) { body['parallelism'] = Number(parallelism); }
        if (savepointPath.trim()) { body['savepointPath'] = savepointPath.trim(); }
        if (claimMode.value) { body['claimMode'] = claimMode.value; }

        const result = await client.post<Record<string, unknown>>(
            `/jars/${encodeURIComponent(jarId)}/run-application`,
            body
        );
        const applicationId = result['applicationid'] ?? result['applicationId'];
        await vscode.window.showInformationMessage(
            typeof applicationId === 'string'
                ? `Application submitted. Application ID: ${applicationId}`
                : 'Application submitted.'
        );
        await FlinkDetailPanel.open(
            profileId,
            `application:${String(applicationId ?? jarId)}`,
            'Submitted Application',
            'details',
            result
        );
    }

    private async submitJar(client: FlinkClient, profileId: string, jarId: string): Promise<void> {
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
        const allowNonRestoredState = await vscode.window.showQuickPick(
            ['No', 'Yes'],
            { placeHolder: 'Allow non-restored state?' }
        );
        if (!allowNonRestoredState) {
            return;
        }
        if (!await this.confirm(`Submit JAR "${jarId}"?`, 'Submit', true)) {
            return;
        }

        const body: {
            entryClass?: string;
            programArgsList: string[];
            parallelism?: number;
            savepointPath?: string;
            allowNonRestoredState: boolean;
        } = {
            programArgsList: programArgs.trim() ? programArgs.trim().split(/\s+/) : [],
            allowNonRestoredState: allowNonRestoredState === 'Yes'
        };
        if (entryClass.trim()) { body.entryClass = entryClass.trim(); }
        if (parallelism.trim()) { body.parallelism = Number(parallelism); }
        if (savepointPath.trim()) { body.savepointPath = savepointPath.trim(); }
        const result = await client.post(
            `/jars/${encodeURIComponent(jarId)}/run`,
            body
        );
        await vscode.window.showInformationMessage('Job submitted.');
        await FlinkDetailPanel.open(profileId, 'submitted-job', 'Submitted Job', 'job', result);
    }

    private async restartFromSavepoint(client: FlinkClient, savepointPath: string): Promise<void> {
        const jars = await client.get<FlinkJars>('/jars');
        const jarChoices = (jars.files ?? []).map(jar => ({
            label: jar.name || jar.id,
            description: jar.id,
            jarId: jar.id
        }));
        if (!jarChoices.length) {
            throw new Error('No uploaded JARs were found on this cluster.');
        }
        const selectedJar = await vscode.window.showQuickPick(jarChoices, {
            placeHolder: 'Select the JAR to restart from this savepoint'
        });
        if (!selectedJar) {
            return;
        }

        const entryClass = await vscode.window.showInputBox({ prompt: 'Entry class (optional)' });
        if (entryClass === undefined) {
            return;
        }
        const programArgs = await vscode.window.showInputBox({
            prompt: 'Program arguments (optional, separated by spaces)',
            placeHolder: '--input /path/to/input'
        });
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
        const claimModes: Array<{
            label: 'NO_CLAIM' | 'CLAIM' | 'LEGACY';
            value: 'NO_CLAIM' | 'CLAIM' | 'LEGACY';
        }> = [
            { label: 'NO_CLAIM', value: 'NO_CLAIM' },
            { label: 'CLAIM', value: 'CLAIM' },
            { label: 'LEGACY', value: 'LEGACY' }
        ];
        const selectedClaimMode = await vscode.window.showQuickPick(claimModes, {
            placeHolder: 'Select savepoint claim mode'
        });
        if (selectedClaimMode === undefined) {
            return;
        }
        const allowNonRestoredState = await vscode.window.showQuickPick(['No', 'Yes'], {
            placeHolder: 'Allow non-restored state?'
        });
        if (allowNonRestoredState === undefined) {
            return;
        }

        const body: {
            savepointPath: string;
            allowNonRestoredState: boolean;
            parallelism?: number;
            entryClass?: string;
            programArgsList: string[];
            claimMode: typeof claimModes[number]['value'];
        } = {
            savepointPath,
            allowNonRestoredState: allowNonRestoredState === 'Yes',
            programArgsList: programArgs.trim() ? programArgs.trim().split(/\s+/) : [],
            claimMode: selectedClaimMode.value
        };
        if (parallelism.trim()) {
            body.parallelism = Number(parallelism);
        }
        if (entryClass.trim()) {
            body.entryClass = entryClass.trim();
        }

        const result = await client.post<Record<string, unknown>>(
            `/jars/${encodeURIComponent(selectedJar.jarId)}/run`,
            body
        );
        const jobId = result['jobid'];
        if (typeof jobId !== 'string' || !jobId) {
            throw new Error('Flink did not return a job ID for the submitted job.');
        }
        await vscode.window.showInformationMessage(`Job started from savepoint. Job ID: ${jobId}`);
    }

    private async updateResourceRequirements(
        client: FlinkClient,
        jobId: string,
        jobName: string
    ): Promise<void> {
        const input = await vscode.window.showInputBox({
            prompt: `Resource requirements JSON for "${jobName}"`,
            placeHolder: '{"<vertex-id>":{"parallelism":{"lowerBound":1,"upperBound":4}}}',
            validateInput: value => {
                try {
                    const parsed: unknown = JSON.parse(value);
                    return typeof parsed === 'object' && parsed !== null && !Array.isArray(parsed)
                        ? undefined
                        : 'Enter a JSON object keyed by vertex ID.';
                } catch {
                    return 'Enter valid JSON.';
                }
            }
        });
        if (input === undefined) { return; }
        const body: unknown = JSON.parse(input);
        await client.put(`/jobs/${encodeURIComponent(jobId)}/resource-requirements`, body);
        const result = await client.get<unknown>(`/jobs/${encodeURIComponent(jobId)}/resource-requirements`);
        await FlinkDetailPanel.open(
            jobId,
            `resource-requirements:${jobId}`,
            `Resource Requirements: ${jobName}`,
            'details',
            result
        );
    }

    private async openRescaleDetails(
        client: FlinkClient,
        profileId: string,
        jobId: string,
        jobName: string
    ): Promise<void> {
        const rescaleId = await vscode.window.showInputBox({
            prompt: `Rescale UUID for "${jobName}"`
        });
        if (!rescaleId?.trim()) { return; }
        const path = `/jobs/${encodeURIComponent(jobId)}/rescales/details/${encodeURIComponent(rescaleId.trim())}`;
        const result = await client.get<unknown>(path);
        await FlinkDetailPanel.open(profileId, path, `Rescale Details: ${jobName}`, 'details', result);
    }

    private async openJobTaskManagerLogUrl(
        client: FlinkClient,
        profileId: string,
        jobId: string,
        jobName: string
    ): Promise<void> {
        const taskManagerId = await vscode.window.showInputBox({
            prompt: `Task Manager ID for job "${jobName}"`
        });
        if (!taskManagerId?.trim()) { return; }
        const path = `/jobs/${encodeURIComponent(jobId)}/taskmanagers/${encodeURIComponent(taskManagerId.trim())}/log-url`;
        const result = await client.get<unknown>(path);
        await FlinkDetailPanel.open(profileId, path, `Task Manager Log URL: ${jobName}`, 'details', result);
    }

    private async openSubtaskRequest(
        client: FlinkClient,
        profileId: string,
        operation: Extract<Operation, { type: 'subtaskRequest' }>
    ): Promise<void> {
        const index = await vscode.window.showInputBox({
            prompt: `Subtask index for "${operation.vertexName}"`,
            validateInput: input => /^\d+$/.test(input) ? undefined : 'Enter a non-negative whole number.'
        });
        if (index === undefined) { return; }
        const prefix = `/jobs/${encodeURIComponent(operation.jobId)}/vertices/${encodeURIComponent(operation.vertexId)}/subtasks/${encodeURIComponent(index)}`;
        let path: string;
        if (operation.request === 'details') {
            path = prefix;
        } else if (operation.request === 'metrics') {
            path = `${prefix}/metrics`;
            const metricIds = await vscode.window.showInputBox({
                prompt: 'Metric IDs (optional, comma-separated)'
            });
            if (metricIds === undefined) { return; }
            if (metricIds.trim()) { path += `?${new URLSearchParams({ get: metricIds.trim() })}`; }
        } else {
            const attempt = await vscode.window.showInputBox({
                prompt: `Attempt number for subtask ${index}`,
                validateInput: input => /^\d+$/.test(input) ? undefined : 'Enter a non-negative whole number.'
            });
            if (attempt === undefined) { return; }
            path = `${prefix}/attempts/${encodeURIComponent(attempt)}`;
            if (operation.request === 'attemptAccumulators') { path += '/accumulators'; }
        }
        const result = await client.get<unknown>(path);
        await FlinkDetailPanel.open(
            profileId,
            path,
            `${operation.vertexName}: Subtask ${index}`,
            'details',
            result
        );
    }

    private async openMetricsRequest(
        client: FlinkClient,
        profileId: string,
        operation: Extract<Operation, { type: 'metricsRequest' }>
    ): Promise<void> {
        const metricIds = await vscode.window.showInputBox({
            prompt: 'Metric IDs (optional, comma-separated)'
        });
        if (metricIds === undefined) { return; }
        const parameters = new URLSearchParams();
        if (metricIds.trim()) { parameters.set('get', metricIds.trim()); }
        if (operation.dimension) {
            const entityIds = await vscode.window.showInputBox({
                prompt: `${operation.dimension} IDs (optional, comma-separated)`
            });
            if (entityIds === undefined) { return; }
            if (entityIds.trim()) { parameters.set(operation.dimension, entityIds.trim()); }
            const aggregation = await vscode.window.showQuickPick(
                ['MIN', 'MAX', 'SUM', 'AVG', 'SKEW'],
                { placeHolder: 'Aggregation (optional)' }
            );
            if (aggregation === undefined) { return; }
            if (aggregation) { parameters.set('agg', aggregation); }
        }
        const path = parameters.size ? `${operation.path}?${parameters}` : operation.path;
        const result = await client.get<unknown>(path);
        await FlinkDetailPanel.open(profileId, path, operation.title, 'details', result);
    }

    private async disposeSavepoint(client: FlinkClient, profile: ClusterProfile, existingPath?: string): Promise<void> {
        const savepoints = this.context.globalState.get<SavedSavepoint[]>(this.savepointKey(profile), []);
        const path = existingPath ?? await vscode.window.showQuickPick(
            [...new Set(savepoints.map(savepoint => savepoint.path)), 'Enter another path...'],
            { placeHolder: 'Select a savepoint to dispose' }
        );
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
        const response = await client.post<Record<string, unknown>>('/savepoint-disposal', {
            'savepoint-path': targetPath
        });
        const returnedId = AsyncOperationTracker.getTriggerId(response);
        const result = await new AsyncOperationTracker(client).track(
            'Disposing savepoint',
            `/savepoint-disposal/${encodeURIComponent(returnedId)}`
        );
        await this.context.globalState.update(
            this.savepointKey(profile),
            savepoints.filter(savepoint => savepoint.path !== targetPath)
        );
        await FlinkDetailPanel.open(profile.id, 'savepoint-disposal', 'Savepoint Disposal Result', 'savepoint', result);
    }

    private async addCluster(): Promise<void> {
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
        } catch (error) {
            this.showError('Could not add Flink cluster', error);
        }
    }

    private async editCluster(node: FlinkTreeNode): Promise<void> {
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
        } catch (error) {
            this.showError('Could not update Flink cluster', error);
        }
    }

    private async removeCluster(node: FlinkTreeNode): Promise<void> {
        const profile = node.profile;
        if (!profile || !await this.confirm(`Remove cluster "${profile.name}"?`, 'Remove', true)) {
            return;
        }
        try {
            await this.store.remove(profile.id);
            this.refresh();
        } catch (error) {
            this.showError(`Could not remove "${profile.name}"`, error);
        }
    }

    private async testConnection(node: FlinkTreeNode): Promise<void> {
        const profile = node.profile;
        if (!profile) {
            return;
        }
        try {
            const result = await new FlinkClient(profile).testConnection();
            await vscode.window.showInformationMessage(`Connected to Flink ${result.version} at ${profile.baseUrl}.`);
        } catch (error) {
            this.showError(`Could not connect to "${profile.name}"`, error);
        }
    }

    private async confirm(message: string, action: string, modal = false): Promise<boolean> {
        const selected = modal
            ? await vscode.window.showWarningMessage(message, { modal: true }, action)
            : await vscode.window.showWarningMessage(message, action);
        return selected === action;
    }

    private async rememberSavepoint(profile: ClusterProfile, jobName: string, path: string): Promise<void> {
        const key = this.savepointKey(profile);
        const existing = this.context.globalState.get<SavedSavepoint[]>(key, []);
        await this.context.globalState.update(key, [
            { path, jobName, createdAt: Date.now() },
            ...existing.filter(savepoint => savepoint.path !== path)
        ].slice(0, 100));
    }

    private findSavepointPath(result: FlinkAsyncStatus): string | undefined {
        const findPath = (value: unknown): string | undefined => {
            if (Array.isArray(value)) {
                for (const entry of value) {
                    const match = findPath(entry);
                    if (match) { return match; }
                }
                return undefined;
            }
            if (typeof value !== 'object' || value === null) {
                return undefined;
            }
            const record = value as Record<string, unknown>;
            for (const key of ['location', 'savepoint-path', 'savepointPath', 'external-path', 'externalPath']) {
                if (typeof record[key] === 'string') {
                    return record[key];
                }
            }
            for (const child of Object.values(record)) {
                const match = findPath(child);
                if (match) { return match; }
            }
            return undefined;
        };
        return findPath(result);
    }

    private triggerIdFromResult(result: Record<string, unknown>, fallback: string): string {
        try {
            return AsyncOperationTracker.getTriggerId(result);
        } catch {
            return fallback;
        }
    }

    private node(
        label: string,
        nodeType: string,
        profile: ClusterProfile,
        values: {
            jobId?: string;
            jobName?: string;
            vertexId?: string;
            taskManagerId?: string;
            jarId?: string;
            jarName?: string;
            savepointPath?: string;
            checkpointId?: number;
        } = {}
    ): FlinkTreeNode {
        const item = new FlinkTreeNode(
            label,
            vscode.TreeItemCollapsibleState.Collapsed,
            nodeType,
            profile,
            undefined,
            undefined,
            values.jobId,
            values.jobName,
            values.vertexId,
            values.taskManagerId,
            values.jarId,
            undefined,
            values.checkpointId,
            values.savepointPath,
            values.jarName
        );
        return item;
    }

    private resourceNode(
        label: string,
        profile: ClusterProfile,
        path: string | undefined,
        title: string,
        kind?: Resource['kind']
    ): FlinkTreeNode {
        const resource: Resource = {
            path: path ?? '',
            title,
            kind,
            viewType: panelTypeFor(title, path ?? '')
        };
        const item = new FlinkTreeNode(label, vscode.TreeItemCollapsibleState.None, 'resource', profile, resource);
        item.command = {
            command: 'ApacheFlink.OpenResource',
            title: `Open ${label}`,
            arguments: [item]
        };
        item.iconPath = new vscode.ThemeIcon(kind === 'metrics' ? 'graph' : 'file');
        return item;
    }

    private operationNode(
        label: string,
        profile: ClusterProfile,
        operation: Operation,
        refreshNode?: FlinkTreeNode
    ): FlinkTreeNode {
        const item = new FlinkTreeNode(label, vscode.TreeItemCollapsibleState.None, 'operation', profile, undefined, operation);
        item.refreshNode = refreshNode;
        item.command = {
            command: 'ApacheFlink.RunOperation',
            title: label,
            arguments: [item]
        };
        item.iconPath = new vscode.ThemeIcon('play');
        return item;
    }

    private messageNode(message: string): FlinkTreeNode {
        return new FlinkTreeNode(message, vscode.TreeItemCollapsibleState.None, 'message');
    }

    private async validateUrl(value: string): Promise<string | undefined> {
        try {
            const url = new URL(value);
            if (url.protocol !== 'http:' && url.protocol !== 'https:') {
                return 'The URL must use HTTP or HTTPS.';
            }
            if (url.username || url.password || url.search || url.hash) {
                return 'Do not include credentials, query parameters, or fragments.';
            }
            return undefined;
        } catch {
            return 'Enter a valid Flink REST URL, for example http://localhost:8081.';
        }
    }

    private requireProfile(node: FlinkTreeNode): ClusterProfile {
        if (!node.profile) {
            throw new Error('Cluster profile not found.');
        }
        return node.profile;
    }

    private requireJobId(node: FlinkTreeNode): string {
        if (!node.jobId) {
            throw new Error('Job ID not found.');
        }
        return node.jobId;
    }

    private stateIcon(state: string): vscode.ThemeIcon {
        if (state === 'RUNNING') { return new vscode.ThemeIcon('play', new vscode.ThemeColor('charts.green')); }
        if (state === 'FAILED') { return new vscode.ThemeIcon('error', new vscode.ThemeColor('charts.red')); }
        if (state === 'CANCELED' || state === 'CANCELLING') { return new vscode.ThemeIcon('circle-slash'); }
        return new vscode.ThemeIcon('circle-outline');
    }

    private isJobRunning(description: string | boolean | vscode.TreeItemLabel | undefined): boolean {
        return description === 'RUNNING';
    }

    private savepointKey(profile: ClusterProfile): string {
        return `flink.savepoints.${profile.id}`;
    }

    private errorMessage(error: unknown): string {
        return error instanceof Error ? error.message : String(error);
    }

    private showError(message: string, error: unknown): void {
        void vscode.window.showErrorMessage(`${message}: ${this.errorMessage(error)}`);
    }
}

function panelTypeFor(title: string, path: string): FlinkPanelType {
    const normalizedTitle = title.toLowerCase();
    const normalizedPath = path.toLowerCase();
    if (normalizedTitle.includes('cluster overview')) { return 'overview'; }
    if (normalizedTitle.includes('job overview')) { return 'job'; }
    if (normalizedTitle.includes('execution plan')) { return 'plan'; }
    if (normalizedTitle.includes('jar plan')) { return 'jarPlan'; }
    if (normalizedTitle.includes('exception')) { return 'exceptions'; }
    if (normalizedTitle.includes('checkpoint configuration')) { return 'configuration'; }
    if (normalizedTitle.includes('checkpoint subtasks')) { return 'subtasks'; }
    if (normalizedTitle.includes('checkpoint details')) { return 'checkpoint'; }
    if (normalizedTitle.includes('checkpoint history')) { return 'checkpoints'; }
    if (normalizedTitle.includes('vertex details')) { return 'vertex'; }
    if (normalizedTitle.includes('backpressure')) { return 'backpressure'; }
    if (normalizedTitle.includes('flamegraph')) { return 'flamegraph'; }
    if (normalizedTitle.includes('watermarks')) { return 'watermarks'; }
    if (normalizedTitle.includes('vertex task managers')) { return 'taskManagers'; }
    if (normalizedTitle.includes('task manager metrics')) { return 'details'; }
    if (normalizedTitle.includes('task manager details')) { return 'taskManager'; }
    if (normalizedTitle.includes('task manager thread dump') || normalizedTitle.includes('jobmanager thread dump')) {
        return 'threadDump';
    }
    if (normalizedTitle.includes('jobmanager configuration') || normalizedTitle.includes('job configuration')) {
        return 'configuration';
    }
    if (normalizedTitle.includes('environment')) { return 'environment'; }
    if (normalizedTitle.includes('jobmanager metrics')) { return 'details'; }
    if (normalizedTitle.includes('jobmanager logs') || normalizedTitle.includes('task manager logs')) { return 'logs'; }
    if (normalizedTitle.includes('task manager')) { return 'taskManager'; }
    if (normalizedPath.includes('/checkpoints/details/') && normalizedPath.includes('/subtasks/')) { return 'subtasks'; }
    if (normalizedPath.includes('/checkpoints/details/')) { return 'checkpoint'; }
    if (normalizedPath.endsWith('/checkpoints')) { return 'checkpoints'; }
    if (normalizedPath.endsWith('/exceptions')) { return 'exceptions'; }
    if (normalizedPath.endsWith('/plan')) { return 'plan'; }
    if (normalizedPath.endsWith('/thread-dump')) { return 'threadDump'; }
    if (normalizedPath.endsWith('/logs')) { return 'logs'; }
    if (normalizedPath.endsWith('/config')) { return 'configuration'; }
    if (normalizedPath.endsWith('/environment')) { return 'environment'; }
    if (normalizedPath.includes('/backpressure')) { return 'backpressure'; }
    if (normalizedPath.includes('/flamegraph')) { return 'flamegraph'; }
    if (normalizedPath.includes('/watermarks')) { return 'watermarks'; }
    if (normalizedPath.includes('/subtasktimes')) { return 'subtasks'; }
    if (normalizedPath.endsWith('/taskmanagers')) { return 'taskManagers'; }
    if (normalizedPath.endsWith('/overview') || normalizedPath === '/overview') { return 'overview'; }
    if (normalizedPath.includes('/vertices/')) { return 'vertex'; }
    if (normalizedPath.includes('/jobs/')) { return 'job'; }
    if (normalizedPath.includes('/taskmanagers/')) { return 'taskManager'; }
    if (normalizedPath.includes('/jobmanager/')) { return 'jobManager'; }
    return 'details';
}
