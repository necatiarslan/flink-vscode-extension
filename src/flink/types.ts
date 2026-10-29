export interface ClusterProfile {
    id: string;
    name: string;
    baseUrl: string;
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
