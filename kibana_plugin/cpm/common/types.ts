import type { WeightKey } from './constants';

export interface ClusterRegistryDoc {
  cluster_uuid: string;
  cluster_id: string;
  cluster_name: string;
  active: boolean;
  dc?: string;
  ingest_hosts?: string;
  node_count?: number;
  disk_total_bytes?: number;
  heap_max_bytes?: number;
}

export interface ScoringWeightsConfig {
  config_type: 'scoring_weights';
  weights: Record<WeightKey, number>;
  alert_threshold: number;
  forecast_horizon_hours: number;
  updated_at?: string;
  updated_by?: string;
}

export interface StreamLockDoc {
  config_type: 'stream_lock';
  data_stream_type: string;
  dataset: string;
  namespace: string;
  cluster_id: string;
  pipeline_type: 'catchall' | 'dedicated';
  locked: boolean;
  reason?: string;
}

export interface WatcherRunResult {
  id: string;
  ok: boolean;
  error?: string;
}

export interface RunChainResponse {
  results: WatcherRunResult[];
}
