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

    public async testConnection(): Promise<{ version: string; overview: unknown }> {
        const config = await this.get<Record<string, unknown>>('/config');
        const overview = await this.get<unknown>('/overview');
        const version = config['flink-version'];
        return {
            version: typeof version === 'string' ? version : 'Unknown',
            overview
        };
    }

    private async request<T>(path: string): Promise<T> {
        const url = new URL(path.replace(/^\/+/, ''), `${this.profile.baseUrl}/`);
        let response: Response;

        try {
            response = await fetch(url, {
                headers: { Accept: 'application/json' },
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

        try {
            return await response.json() as T;
        } catch (error) {
            const detail = error instanceof Error ? error.message : String(error);
            throw new FlinkApiError(`Flink REST API returned invalid JSON: ${detail}`);
        }
    }
}
