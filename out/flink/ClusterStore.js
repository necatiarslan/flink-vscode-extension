"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ClusterStore = void 0;
const crypto_1 = require("crypto");
const STORAGE_KEY = 'flink.clusters';
class ClusterStore {
    state;
    constructor(state) {
        this.state = state;
    }
    getAll() {
        return this.state.get(STORAGE_KEY, []);
    }
    get(id) {
        return this.getAll().find(profile => profile.id === id);
    }
    async add(name, baseUrl) {
        const profile = {
            id: (0, crypto_1.randomUUID)(),
            name,
            baseUrl: normalizeBaseUrl(baseUrl)
        };
        await this.save([...this.getAll(), profile]);
        return profile;
    }
    async update(id, name, baseUrl) {
        const profiles = this.getAll();
        const index = profiles.findIndex(profile => profile.id === id);
        if (index < 0) {
            throw new Error('The selected Flink cluster no longer exists.');
        }
        profiles[index] = { id, name, baseUrl: normalizeBaseUrl(baseUrl) };
        await this.save(profiles);
    }
    async remove(id) {
        await this.save(this.getAll().filter(profile => profile.id !== id));
    }
    async save(profiles) {
        await this.state.update(STORAGE_KEY, profiles);
    }
}
exports.ClusterStore = ClusterStore;
function normalizeBaseUrl(value) {
    let url;
    try {
        url = new URL(value);
    }
    catch {
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
//# sourceMappingURL=ClusterStore.js.map