import type { ElasticsearchClient } from '@kbn/core/server';
import { CPM_VIEW_CLUSTER_PRIVILEGES } from '../../common/constants';

type HasPrivilegesResponse = {
  username?: string;
  has_all_requested?: boolean;
  cluster?: Record<string, boolean>;
};

/** True when the user has monitor, manage, or manage_pipeline (any one). */
export async function userCanAccessCpm(esClient: ElasticsearchClient): Promise<boolean> {
  const result = (await esClient.transport.request({
    method: 'POST',
    path: '/_security/user/_has_privileges',
    body: {
      cluster: [...CPM_VIEW_CLUSTER_PRIVILEGES],
    },
  })) as HasPrivilegesResponse;

  if (result.has_all_requested) {
    return true;
  }

  const cluster = result.cluster ?? {};
  return CPM_VIEW_CLUSTER_PRIVILEGES.some((privilege) => cluster[privilege] === true);
}
