import * as vscode from 'vscode';
import { ClusterStore } from '../flink/ClusterStore';
import { FlinkClient } from '../flink/FlinkClient';
import { ClusterProfile, FlinkJobsOverview, FlinkTaskManagers } from '../flink/types';

type Resource =
    | { kind: 'overview'; profile: ClusterProfile }
    | { kind: 'job'; profile: ClusterProfile; jobId: string; resource: 'overview' | 'plan' | 'exceptions' | 'config' }
    | { kind: 'taskManager'; profile: ClusterProfile; taskManagerId: string; resource: 'overview' | 'logs' }
    | { kind: 'jobManager'; profile: ClusterProfile; resource: 'config' | 'logs' };

export class FlinkTreeNode extends vscode.TreeItem {
    constructor(
        label: string,
        collapsibleState: vscode.TreeItemCollapsibleState,
        public readonly nodeType: string,
        public readonly profile?: ClusterProfile,
        public readonly resource?: Resource,
        description?: string,
        public readonly jobId?: string,
        public readonly taskManagerId?: string,
        public readonly childNodes?: FlinkTreeNode[]
    ) {
        super(label, collapsibleState);
        this.description = description;
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
            vscode.commands.registerCommand('ApacheFlink.OpenResource', (node: FlinkTreeNode) => this.openResource(node))
        );
    }

    public getTreeItem(element: FlinkTreeNode): vscode.TreeItem {
        return element;
    }

    public async getChildren(element?: FlinkTreeNode): Promise<FlinkTreeNode[]> {
        if (!element) {
            return this.store.getAll().map(profile => {
                const item = new FlinkTreeNode(
                    profile.name,
                    vscode.TreeItemCollapsibleState.Collapsed,
                    'cluster',
                    profile,
                    undefined,
                    profile.baseUrl
                );
                item.contextValue = 'flinkCluster';
                return item;
            });
        }

        try {
            return await this.getNodeChildren(element);
        } catch (error) {
            const detail = error instanceof Error ? error.message : String(error);
            return [new FlinkTreeNode(`Unable to load: ${detail}`, vscode.TreeItemCollapsibleState.None, 'error')];
        }
    }

    public refresh(): void {
        this.changes.fire();
    }

    private async getNodeChildren(node: FlinkTreeNode): Promise<FlinkTreeNode[]> {
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

    private async getJobGroups(profile: ClusterProfile): Promise<FlinkTreeNode[]> {
        const result = await new FlinkClient(profile).get<FlinkJobsOverview>('/jobs/overview');
        const jobs = result.jobs ?? [];
        if (jobs.length === 0) {
            return [new FlinkTreeNode('No jobs', vscode.TreeItemCollapsibleState.None, 'empty')];
        }

        const groups = new Map<string, FlinkTreeNode[]>();
        for (const job of jobs) {
            const status = (job.state || 'UNKNOWN').toUpperCase();
            const group = groups.get(status) ?? [];
            const item = new FlinkTreeNode(
                job.name || job.jid,
                vscode.TreeItemCollapsibleState.Collapsed,
                'job',
                profile,
                undefined,
                status,
                job.jid
            );
            item.id = `${profile.id}:job:${job.jid}`;
            item.tooltip = `Job ID: ${job.jid}`;
            item.contextValue = `flinkJob.${status.toLowerCase()}`;
            group.push(item);
            groups.set(status, group);
        }

        return [...groups.entries()].map(([status, groupedJobs]) => {
            const groupNode = new FlinkTreeNode(
                `${status} (${groupedJobs.length})`,
                vscode.TreeItemCollapsibleState.Expanded,
                'jobGroup',
                profile,
                undefined,
                undefined,
                undefined,
                undefined,
                groupedJobs
            );
            return groupNode;
        });
    }

    private async getTaskManagers(profile: ClusterProfile): Promise<FlinkTreeNode[]> {
        const result = await new FlinkClient(profile).get<FlinkTaskManagers>('/taskmanagers');
        const managers = result.taskmanagers ?? [];
        if (managers.length === 0) {
            return [new FlinkTreeNode('No task managers', vscode.TreeItemCollapsibleState.None, 'empty')];
        }

        return managers.map(manager => {
            const item = new FlinkTreeNode(
                manager.id,
                vscode.TreeItemCollapsibleState.Collapsed,
                'taskManager',
                profile,
                undefined,
                `${Math.max(0, (manager.slotsNumber ?? 0) - (manager.freeSlots ?? 0))}/${manager.slotsNumber ?? 0} slots`,
                undefined,
                manager.id
            );
            item.contextValue = 'flinkTaskManager';
            return item;
        });
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
        if (!profile) {
            return;
        }

        const confirmation = await vscode.window.showWarningMessage(
            `Remove Flink cluster "${profile.name}"?`,
            { modal: true },
            'Remove'
        );
        if (confirmation !== 'Remove') {
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

    private async openResource(node: FlinkTreeNode): Promise<void> {
        const resource = node.resource;
        if (!resource) {
            return;
        }

        try {
            const client = new FlinkClient(resource.profile);
            const data = await this.fetchResource(client, resource);
            const content = JSON.stringify(data, null, 2) ?? String(data);
            const document = await vscode.workspace.openTextDocument({ language: 'json', content });
            await vscode.window.showTextDocument(document, vscode.ViewColumn.One);
        } catch (error) {
            this.showError(`Could not load ${node.label}`, error);
        }
    }

    private async fetchResource(client: FlinkClient, resource: Resource): Promise<unknown> {
        switch (resource.kind) {
            case 'overview': {
                const [overview, config] = await Promise.all([
                    client.get<unknown>('/overview'),
                    client.get<unknown>('/config')
                ]);
                return { overview, config };
            }
            case 'job': {
                const prefix = `/jobs/${encodeURIComponent(resource.jobId)}`;
                const endpoint = resource.resource === 'overview'
                    ? prefix
                    : `${prefix}/${resource.resource}`;
                return client.get<unknown>(endpoint);
            }
            case 'taskManager': {
                const prefix = `/taskmanagers/${encodeURIComponent(resource.taskManagerId)}`;
                return client.get<unknown>(resource.resource === 'overview' ? prefix : `${prefix}/logs`);
            }
            case 'jobManager':
                return client.get<unknown>(`/jobmanager/${resource.resource}`);
        }
    }

    private actionNode(label: string, profile: ClusterProfile, resource: Resource): FlinkTreeNode {
        const node = new FlinkTreeNode(label, vscode.TreeItemCollapsibleState.None, 'resource', profile, resource);
        node.command = {
            command: 'ApacheFlink.OpenResource',
            title: `Open ${label}`,
            arguments: [node]
        };
        return node;
    }

    private validateUrl(value: string): string | undefined {
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

    private showError(message: string, error: unknown): void {
        const detail = error instanceof Error ? error.message : String(error);
        void vscode.window.showErrorMessage(`${message}: ${detail}`);
    }
}
