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
    post(path, body) {
        return this.request(path, 'POST', body);
    }
    patch(path, body) {
        return this.request(path, 'PATCH', body);
    }
    postQuery(path, params) {
        return this.request(`${path}?${new URLSearchParams(params)}`, 'POST');
    }
    patchQuery(path, params) {
        return this.request(`${path}?${new URLSearchParams(params)}`, 'PATCH');
    }
    delete(path) {
        return this.request(path, 'DELETE');
    }
    async uploadJar(fileName, content) {
        const arrayBuffer = new ArrayBuffer(content.byteLength);
        new Uint8Array(arrayBuffer).set(content);
        const form = new FormData();
        form.append('jarfile', new Blob([arrayBuffer]), fileName);
        return this.request('/jars/upload', 'POST', form);
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
    async request(path, method = 'GET', body) {
        const url = new URL(path.replace(/^\/+/, ''), `${this.profile.baseUrl}/`);
        let response;
        try {
            response = await fetch(url, {
                method,
                headers: body instanceof FormData
                    ? { Accept: 'application/json' }
                    : body === undefined
                        ? { Accept: 'application/json' }
                        : { Accept: 'application/json', 'Content-Type': 'application/json' },
                body: body === undefined ? undefined : body instanceof FormData ? body : JSON.stringify(body),
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
        if (response.status === 204) {
            return undefined;
        }
        const text = await response.text();
        if (!text.trim()) {
            return undefined;
        }
        if (!response.headers.get('content-type')?.toLowerCase().includes('json')) {
            return text;
        }
        try {
            return JSON.parse(text);
        }
        catch (error) {
            const detail = error instanceof Error ? error.message : String(error);
            throw new FlinkApiError(`Flink REST API returned invalid JSON: ${detail}`);
        }
    }
}
exports.FlinkClient = FlinkClient;
//# sourceMappingURL=FlinkClient.js.map