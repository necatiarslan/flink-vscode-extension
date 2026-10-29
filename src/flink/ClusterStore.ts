import * as vscode from 'vscode';
import { randomUUID } from 'crypto';
import { ClusterProfile } from './types';

const STORAGE_KEY = 'flink.clusters';

export class ClusterStore {
    constructor(private readonly state: vscode.Memento) {}

    public getAll(): ClusterProfile[] {
        return this.state.get<ClusterProfile[]>(STORAGE_KEY, []);
    }

    public get(id: string): ClusterProfile | undefined {
        return this.getAll().find(profile => profile.id === id);
    }

    public async add(name: string, baseUrl: string, readOnly: boolean): Promise<ClusterProfile> {
        const profile: ClusterProfile = {
            id: randomUUID(),
            name,
            baseUrl: normalizeBaseUrl(baseUrl),
            readOnly
        };
        await this.save([...this.getAll(), profile]);
        return profile;
    }

    public async update(id: string, name: string, baseUrl: string, readOnly: boolean): Promise<void> {
        const profiles = this.getAll();
        const index = profiles.findIndex(profile => profile.id === id);
        if (index < 0) {
            throw new Error('The selected Flink cluster no longer exists.');
        }

        profiles[index] = { id, name, baseUrl: normalizeBaseUrl(baseUrl), readOnly };
        await this.save(profiles);
    }

    public async remove(id: string): Promise<void> {
        await this.save(this.getAll().filter(profile => profile.id !== id));
    }

    private async save(profiles: ClusterProfile[]): Promise<void> {
        await this.state.update(STORAGE_KEY, profiles);
    }
}

function normalizeBaseUrl(value: string): string {
    let url: URL;
    try {
        url = new URL(value);
    } catch {
        throw new Error('Enter a valid Flink REST URL, for example http://localhost:8081.');
    }

    if (url.protocol !== 'http:' && url.protocol !== 'https:') {
        throw new Error('Flink REST URLs must use HTTP or HTTPS.');
    }
    if (url.username || url.password || url.search || url.hash) {
        throw new Error('Do not include credentials, query parameters, or fragments in the cluster URL.');
    }

    return url.toString().replace(/\/+$/, '');
}
