export interface ClusterProfile {
    id: string;
    name: string;
    baseUrl: string;
    readOnly?: boolean;
}

export interface FlinkJobOverview {
    jid: string;
    name: string;
    state: string;
    startTime?: number;
    duration?: number;
    tasks?: {
        total?: number;
        created?: number;
        scheduled?: number;
        deploying?: number;
        running?: number;
        finished?: number;
        canceling?: number;
        canceled?: number;
        failed?: number;
        reconciling?: number;
        initializing?: number;
    };
}

export interface FlinkJobsOverview {
    jobs?: FlinkJobOverview[];
}

export interface FlinkTaskManagers {
    taskmanagers?: Array<{
        id: string;
        slotsNumber?: number;
        freeSlots?: number;
        [key: string]: unknown;
    }>;
}

export interface FlinkJar {
    id: string;
    name: string;
    uploaded?: number;
    entry?: string;
    [key: string]: unknown;
}

export interface FlinkJars {
    files?: FlinkJar[];
}

export interface FlinkJobDetail extends FlinkJobOverview {
    vertices?: Array<{ id: string; name: string; status?: string; [key: string]: unknown }>;
}

export interface FlinkCheckpoints {
    counts?: Record<string, number>;
    latest?: {
        completed?: { id: number; status?: string; [key: string]: unknown };
        failed?: { id: number; status?: string; [key: string]: unknown };
        [key: string]: unknown;
    };
    history?: Array<{ id: number; status?: string; [key: string]: unknown }>;
    [key: string]: unknown;
}

export interface FlinkMetric {
    id: string;
    value?: string;
}

export interface FlinkAsyncStatus {
    status?: { id?: string };
    operation?: { status?: { id?: string }; [key: string]: unknown };
    [key: string]: unknown;
}
