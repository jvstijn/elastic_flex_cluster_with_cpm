export const CPM_CLUSTER_REGISTRY = 'cpm-cluster-registry';
export const CPM_ROUTING_CONFIG = 'cpm-routing-config';

export const SCORING_DOC_ID = '_global';
export const CONFIG_TYPE_SCORING = 'scoring_weights';
export const CONFIG_TYPE_STREAM_LOCK = 'stream_lock';

export const WATCHER_FORECAST = 'cpm-forecast-trigger';

/** Daily CPM chain (matches ansible bootstrap order). */
export const WATCHER_CHAIN = [
  'cpm-registry-sync',
  'cpm-scoring',
  'cpm-routing-advisor',
  'cpm-state-manager',
  'cpm-pipeline-manager',
  'cpm-stream-coverage',
] as const;

export type WatcherId = (typeof WATCHER_CHAIN)[number] | typeof WATCHER_FORECAST;

export const WEIGHT_KEYS = ['disk', 'jvm', 'shard', 'load'] as const;
export type WeightKey = (typeof WEIGHT_KEYS)[number];

export function streamLockDocId(type: string, dataset: string, namespace: string): string {
  return `${type}-${dataset}-${namespace}`;
}
