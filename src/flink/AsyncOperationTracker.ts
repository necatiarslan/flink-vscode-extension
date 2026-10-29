import * as vscode from 'vscode';
import { FlinkAsyncStatus } from './types';
import { FlinkClient } from './FlinkClient';

export class AsyncOperationTracker {
    constructor(
        private readonly client: FlinkClient,
        private readonly pollIntervalMs = 2_000,
        private readonly timeoutMs = 15 * 60_000
    ) {}

    public async track(label: string, statusPath: string, initial?: FlinkAsyncStatus): Promise<FlinkAsyncStatus> {
        return vscode.window.withProgress(
            {
                location: vscode.ProgressLocation.Notification,
                title: label,
                cancellable: false
            },
            async progress => {
                const startedAt = Date.now();
                let result = initial;
                while (Date.now() - startedAt < this.timeoutMs) {
                    if (result === undefined) {
                        result = await this.client.get<FlinkAsyncStatus>(statusPath);
                    }

                    const status = (result.status?.id ?? result.operation?.status?.id ?? '').toUpperCase();
                    progress.report({ message: status || 'Waiting for Flink...' });
                    if (status === 'COMPLETED' || status === 'SUCCESS') {
                        return result;
                    }
                    if (status === 'FAILED') {
                        throw new Error(`Flink operation failed: ${JSON.stringify(result)}`);
                    }

                    await new Promise(resolve => setTimeout(resolve, this.pollIntervalMs));
                    result = undefined;
                }

                throw new Error(`${label} did not complete within ${this.timeoutMs / 60_000} minutes.`);
            }
        );
    }

    public static getTriggerId(result: Record<string, unknown>): string {
        const triggerId = result['triggerid'] ?? result['triggerId'] ?? result['request-id'] ?? result['requestId'];
        if (typeof triggerId !== 'string' || !triggerId) {
            throw new Error(`Flink did not return an operation ID: ${JSON.stringify(result)}`);
        }
        return triggerId;
    }
}
