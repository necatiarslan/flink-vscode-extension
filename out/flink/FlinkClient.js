"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.FlinkClient = exports.FlinkApiError = void 0;
class FlinkApiError extends Error {
    statusCode;
    constructor(message, statusCode) {
        super(message);
        this.statusCode = statusCode;
        this.name = 'FlinkApiError';
    }
}
exports.FlinkApiError = FlinkApiError;
class FlinkClient {
    profile;
    timeoutMs;
    constructor(profile, timeoutMs = 10_000) {
        this.profile = profile;
        this.timeoutMs = timeoutMs;
    }
    get(path) {
        return this.request(path);
    }
    async testConnection() {
        const config = await this.get('/config');
        const overview = await this.get('/overview');
        const version = config['flink-version'];
        return {
            version: typeof version === 'string' ? version : 'Unknown',
            overview
        };
    }
    async request(path) {
        const url = new URL(path.replace(/^\/+/, ''), `${this.profile.baseUrl}/`);
        let response;
        try {
            response = await fetch(url, {
                headers: { Accept: 'application/json' },
                signal: AbortSignal.timeout(this.timeoutMs)
            });
        }
        catch (error) {
            const detail = error instanceof Error ? error.message : String(error);
            throw new FlinkApiError(`Could not connect to ${url.origin}: ${detail}`);
        }
        if (!response.ok) {
            const body = await response.text();
            const detail = body.trim() || response.statusText;
            throw new FlinkApiError(`Flink REST API returned HTTP ${response.status}: ${detail}`, response.status);
        }
        try {
            return await response.json();
        }
        catch (error) {
            const detail = error instanceof Error ? error.message : String(error);
            throw new FlinkApiError(`Flink REST API returned invalid JSON: ${detail}`);
        }
    }
}
exports.FlinkClient = FlinkClient;
//# sourceMappingURL=FlinkClient.js.map