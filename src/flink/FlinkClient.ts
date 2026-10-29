import { ClusterProfile } from './types';

export class FlinkApiError extends Error {
    constructor(
        message: string,
        public readonly statusCode?: number
    ) {
        super(message);
        this.name = 'FlinkApiError';
    }
}

export class FlinkClient {
    constructor(
        private readonly profile: ClusterProfile,
        private readonly timeoutMs = 10_000
    ) {}

    public get<T>(path: string): Promise<T> {
        return this.request<T>(path);
    }

    public post<T>(path: string, body?: unknown): Promise<T> {
        return this.request<T>(path, 'POST', body);
    }

    public patch<T>(path: string, body?: unknown): Promise<T> {
        return this.request<T>(path, 'PATCH', body);
    }

    public postQuery<T>(path: string, params: Record<string, string>): Promise<T> {
        return this.request<T>(`${path}?${new URLSearchParams(params)}`, 'POST');
    }

    public patchQuery<T>(path: string, params: Record<string, string>): Promise<T> {
        return this.request<T>(`${path}?${new URLSearchParams(params)}`, 'PATCH');
    }

    public delete<T>(path: string): Promise<T> {
        return this.request<T>(path, 'DELETE');
    }

    public async uploadJar(fileName: string, content: Uint8Array): Promise<unknown> {
        const arrayBuffer = new ArrayBuffer(content.byteLength);
        new Uint8Array(arrayBuffer).set(content);
        const form = new FormData();
        form.append('jarfile', new Blob([arrayBuffer]), fileName);
        return this.request('/jars/upload', 'POST', form);
    }

    public async testConnection(): Promise<{ version: string; overview: unknown }> {
        const config = await this.get<Record<string, unknown>>('/config');
        const overview = await this.get<unknown>('/overview');
        const version = config['flink-version'];
        return {
            version: typeof version === 'string' ? version : 'Unknown',
            overview
        };
    }

    private async request<T>(path: string, method: string = 'GET', body?: unknown): Promise<T> {
        const url = new URL(path.replace(/^\/+/, ''), `${this.profile.baseUrl}/`);
        let response: Response;

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
        } catch (error) {
            const detail = error instanceof Error ? error.message : String(error);
            throw new FlinkApiError(`Could not connect to ${url.origin}: ${detail}`);
        }

        if (!response.ok) {
            const body = await response.text();
            const detail = body.trim() || response.statusText;
            throw new FlinkApiError(
                `Flink REST API returned HTTP ${response.status}: ${detail}`,
                response.status
            );
        }

        if (response.status === 204) {
            return undefined as T;
        }

        const text = await response.text();
        if (!text.trim()) {
            return undefined as T;
        }
        if (!response.headers.get('content-type')?.toLowerCase().includes('json')) {
            return text as T;
        }
        try {
            return JSON.parse(text) as T;
        } catch (error) {
            const detail = error instanceof Error ? error.message : String(error);
            throw new FlinkApiError(`Flink REST API returned invalid JSON: ${detail}`);
        }
    }
}
