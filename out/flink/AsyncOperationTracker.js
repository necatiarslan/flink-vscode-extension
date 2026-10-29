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
exports.AsyncOperationTracker = void 0;
const vscode = __importStar(require("vscode"));
class AsyncOperationTracker {
    client;
    pollIntervalMs;
    timeoutMs;
    constructor(client, pollIntervalMs = 2_000, timeoutMs = 15 * 60_000) {
        this.client = client;
        this.pollIntervalMs = pollIntervalMs;
        this.timeoutMs = timeoutMs;
    }
    async track(label, statusPath, initial) {
        return vscode.window.withProgress({
            location: vscode.ProgressLocation.Notification,
            title: label,
            cancellable: false
        }, async (progress) => {
            const startedAt = Date.now();
            let result = initial;
            while (Date.now() - startedAt < this.timeoutMs) {
                if (result === undefined) {
                    result = await this.client.get(statusPath);
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
        });
    }
    static getTriggerId(result) {
        const triggerId = result['triggerid'] ?? result['triggerId'] ?? result['request-id'] ?? result['requestId'];
        if (typeof triggerId !== 'string' || !triggerId) {
            throw new Error(`Flink did not return an operation ID: ${JSON.stringify(result)}`);
        }
        return triggerId;
    }
}
exports.AsyncOperationTracker = AsyncOperationTracker;
//# sourceMappingURL=AsyncOperationTracker.js.map