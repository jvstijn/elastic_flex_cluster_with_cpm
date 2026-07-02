import type { HttpSetup } from '@kbn/core/public';
import type {
  ClusterRegistryDoc,
  RunChainResponse,
  ScoringWeightsConfig,
  StreamLockDoc,
} from '../common/types';

export function createCpmApi(http: HttpSetup) {
  return {
    async getClusters(): Promise<ClusterRegistryDoc[]> {
      const res = await http.get<{ clusters: Array<ClusterRegistryDoc & { id: string }> }>(
        '/api/cpm/clusters'
      );
      return res.clusters;
    },

    async setClusterActive(clusterId: string, active: boolean) {
      return http.put(`/api/cpm/clusters/${encodeURIComponent(clusterId)}`, {
        body: JSON.stringify({ active }),
      });
    },

    async getScoring(): Promise<ScoringWeightsConfig> {
      return http.get<ScoringWeightsConfig>('/api/cpm/scoring');
    },

    async saveScoring(config: ScoringWeightsConfig) {
      return http.put('/api/cpm/scoring', {
        body: JSON.stringify({
          weights: config.weights,
          write_queue_threshold: config.write_queue_threshold,
          shard_max_threshold: config.shard_max_threshold,
          kafka_group_id: config.kafka_group_id,
          alert_threshold: config.alert_threshold,
          forecast_horizon_hours: config.forecast_horizon_hours,
        }),
      });
    },

    async getLocks(): Promise<Array<StreamLockDoc & { id: string }>> {
      const res = await http.get<{ locks: Array<StreamLockDoc & { id: string }> }>(
        '/api/cpm/locks'
      );
      return res.locks;
    },

    async saveLock(lockId: string, doc: StreamLockDoc) {
      return http.put(`/api/cpm/locks/${encodeURIComponent(lockId)}`, {
        body: JSON.stringify(doc),
      });
    },

    async deleteLock(lockId: string) {
      return http.delete(`/api/cpm/locks/${encodeURIComponent(lockId)}`);
    },

    async runWatchers(options?: {
      watchers?: string[];
      includeForecast?: boolean;
    }): Promise<RunChainResponse> {
      return http.post<RunChainResponse>('/api/cpm/run', {
        body: JSON.stringify(options ?? {}),
      });
    },
  };
}

export type CpmApi = ReturnType<typeof createCpmApi>;
