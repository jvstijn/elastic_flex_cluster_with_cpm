export const CPM_CLUSTER_REGISTRY = 'cpm-cluster-registry';
export const CPM_ROUTING_CONFIG = 'cpm-routing-config';

/** Kibana management + ES feature id (must match registerApp / registerElasticsearchFeature). */
export const CPM_PLUGIN_ID = 'cpm';

/**
 * Show Stack Management → Ingest → Cluster Pipeline Manager when the user has any of
 * these cluster privileges (OR), same ingest-section pattern as built-in pipeline UIs.
 * Superuser / admin roles that include any of these also qualify.
 */
export const CPM_VIEW_CLUSTER_PRIVILEGES = [
  'monitor',
  'manage',
  'manage_pipeline',
  'manage_logstash_pipelines',
] as const;

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
