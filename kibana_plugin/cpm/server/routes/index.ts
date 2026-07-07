import type { IRouter, RequestHandlerContext } from '@kbn/core/server';
import { schema } from '@kbn/config-schema';
import {
  CPM_CLUSTER_REGISTRY,
  CPM_ROUTING_CONFIG,
  CONFIG_TYPE_SCORING,
  CONFIG_TYPE_STREAM_LOCK,
  SCORING_DOC_ID,
  WATCHER_CHAIN,
  WATCHER_FORECAST,
} from '../../common/constants';
import type { RunChainResponse, WatcherRunResult } from '../../common/types';
import { userCanAccessCpm } from '../lib/check_access';

/** ES index mappings are strict; never persist Kibana/API metadata fields like `id`. */
function withoutApiMeta<T extends Record<string, unknown>>(body: T): Omit<T, 'id'> {
  const { id: _id, ...rest } = body;
  return rest;
}

async function denyUnlessCpmAccess(
  context: RequestHandlerContext,
  response: { forbidden: (opts: { body: { message: string } }) => unknown }
) {
  const esClient = (await context.core).elasticsearch.client.asCurrentUser;
  if (await userCanAccessCpm(esClient)) {
    return null;
  }
  return response.forbidden({
    body: { message: 'Cluster Pipeline Manager requires monitor, manage, manage_pipeline, or manage_logstash_pipelines.' },
  });
}

async function runWatcher(
  esClient: { transport: { request: (opts: object) => Promise<unknown> } },
  id: string
): Promise<WatcherRunResult> {
  try {
    await esClient.transport.request({
      method: 'POST',
      path: `/_watcher/watch/${id}/_execute`,
      body: { record_execution: true },
    });
    return { id, ok: true };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return { id, ok: false, error: message };
  }
}

export function defineRoutes(router: IRouter) {
  router.get({ path: '/api/cpm/access', validate: false }, async (context, _request, response) => {
    const esClient = (await context.core).elasticsearch.client.asCurrentUser;
    const allowed = await userCanAccessCpm(esClient);
    return response.ok({ body: { allowed } });
  });

  router.get({ path: '/api/cpm/clusters', validate: false }, async (context, _request, response) => {
    const denied = await denyUnlessCpmAccess(context, response);
    if (denied) return denied;

    const esClient = (await context.core).elasticsearch.client.asCurrentUser;
    try {
      const result = await esClient.search({
        index: CPM_CLUSTER_REGISTRY,
        size: 200,
        query: { match_all: {} },
        sort: [{ cluster_name: { order: 'asc', unmapped_type: 'keyword' } }],
      });
      const clusters = (result.hits.hits ?? []).map((hit) => ({
        id: hit._id,
        ...(hit._source as object),
      }));
      return response.ok({ body: { clusters } });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return response.customError({ statusCode: 500, body: { message } });
    }
  });

  router.put(
    {
      path: '/api/cpm/clusters/{clusterId}',
      validate: {
        params: schema.object({ clusterId: schema.string() }),
        body: schema.object({}, { unknowns: 'allow' }),
      },
    },
    async (context, request, response) => {
      const denied = await denyUnlessCpmAccess(context, response);
      if (denied) return denied;

      const esClient = (await context.core).elasticsearch.client.asCurrentUser;
      const clusterId = (request.params as { clusterId: string }).clusterId;
      const body = withoutApiMeta(request.body as Record<string, unknown>);

      try {
        const existing = await esClient.get({
          index: CPM_CLUSTER_REGISTRY,
          id: clusterId,
        });
        const existingSource = existing._source as Record<string, unknown>;
        const source = {
          ...existingSource,
          ...body,
          cluster_uuid: existingSource.cluster_uuid ?? clusterId,
          cluster_id: existingSource.cluster_id ?? clusterId,
        };
        await esClient.index({
          index: CPM_CLUSTER_REGISTRY,
          id: clusterId,
          document: source,
          refresh: 'wait_for',
        });
        return response.ok({ body: { id: clusterId, ...source } });
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        return response.customError({ statusCode: 500, body: { message } });
      }
    }
  );

  router.get(
    { path: '/api/cpm/scoring', validate: false },
    async (context, _request, response) => {
      const denied = await denyUnlessCpmAccess(context, response);
      if (denied) return denied;

      const esClient = (await context.core).elasticsearch.client.asCurrentUser;
      try {
        const result = await esClient.get({
          index: CPM_ROUTING_CONFIG,
          id: SCORING_DOC_ID,
        });
        return response.ok({ body: result._source });
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        return response.customError({ statusCode: 404, body: { message } });
      }
    }
  );

  router.put({ path: '/api/cpm/scoring', validate: {
    body: schema.object({
      weights: schema.object({}, { unknowns: 'allow' }),
      write_queue_threshold: schema.number(),
      shard_max_threshold: schema.number(),
      kafka_group_id: schema.string(),
      alert_threshold: schema.number(),
      forecast_horizon_hours: schema.number(),
    }),
  } }, async (context, request, response) => {
    const denied = await denyUnlessCpmAccess(context, response);
    if (denied) return denied;

    const esClient = (await context.core).elasticsearch.client.asCurrentUser;
    const body = request.body as {
      weights: Record<string, number>;
      write_queue_threshold: number;
      shard_max_threshold: number;
      kafka_group_id: string;
      alert_threshold: number;
      forecast_horizon_hours: number;
    };
    const username = (await context.core).security.authc.getCurrentUser()?.username ?? 'unknown';

    const doc = {
      config_type: CONFIG_TYPE_SCORING,
      weights: body.weights,
      write_queue_threshold: body.write_queue_threshold,
      shard_max_threshold: body.shard_max_threshold,
      kafka_group_id: body.kafka_group_id,
      alert_threshold: body.alert_threshold,
      forecast_horizon_hours: body.forecast_horizon_hours,
      updated_at: new Date().toISOString(),
      updated_by: username,
    };

    try {
      await esClient.index({
        index: CPM_ROUTING_CONFIG,
        id: SCORING_DOC_ID,
        document: doc,
        refresh: 'wait_for',
      });
      return response.ok({ body: doc });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return response.customError({ statusCode: 500, body: { message } });
    }
  });

  router.get({ path: '/api/cpm/locks', validate: false }, async (context, _request, response) => {
    const denied = await denyUnlessCpmAccess(context, response);
    if (denied) return denied;

    const esClient = (await context.core).elasticsearch.client.asCurrentUser;
    try {
      const result = await esClient.search({
        index: CPM_ROUTING_CONFIG,
        size: 500,
        query: { term: { config_type: CONFIG_TYPE_STREAM_LOCK } },
        sort: [{ dataset: { order: 'asc', unmapped_type: 'keyword' } }],
      });
      const locks = (result.hits.hits ?? []).map((hit) => ({
        id: hit._id,
        ...(hit._source as object),
      }));
      return response.ok({ body: { locks } });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return response.customError({ statusCode: 500, body: { message } });
    }
  });

  router.put(
    {
      path: '/api/cpm/locks/{lockId}',
      validate: {
        params: schema.object({ lockId: schema.string() }),
        body: schema.object({}, { unknowns: 'allow' }),
      },
    },
    async (context, request, response) => {
      const denied = await denyUnlessCpmAccess(context, response);
      if (denied) return denied;

      const esClient = (await context.core).elasticsearch.client.asCurrentUser;
      const lockId = (request.params as { lockId: string }).lockId;
      const body = withoutApiMeta(request.body as Record<string, unknown>);
      const username = (await context.core).security.authc.getCurrentUser()?.username ?? 'unknown';

      const doc = {
        ...body,
        config_type: CONFIG_TYPE_STREAM_LOCK,
        locked: true,
        updated_at: new Date().toISOString(),
        updated_by: username,
      };

      try {
        await esClient.index({
          index: CPM_ROUTING_CONFIG,
          id: lockId,
          document: doc,
          refresh: 'wait_for',
        });
        return response.ok({ body: { id: lockId, ...doc } });
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        return response.customError({ statusCode: 500, body: { message } });
      }
    }
  );

  router.delete(
    {
      path: '/api/cpm/locks/{lockId}',
      validate: {
        params: schema.object({ lockId: schema.string() }),
      },
    },
    async (context, request, response) => {
      const denied = await denyUnlessCpmAccess(context, response);
      if (denied) return denied;

      const esClient = (await context.core).elasticsearch.client.asCurrentUser;
      const lockId = (request.params as { lockId: string }).lockId;
      try {
        await esClient.delete({
          index: CPM_ROUTING_CONFIG,
          id: lockId,
          refresh: 'wait_for',
        });
        return response.ok({ body: { deleted: lockId } });
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        return response.customError({ statusCode: 500, body: { message } });
      }
    }
  );

  router.post(
    {
      path: '/api/cpm/run',
      validate: {
        body: schema.object({
          watchers: schema.maybe(schema.arrayOf(schema.string())),
          includeForecast: schema.maybe(schema.boolean()),
          applyLocks: schema.maybe(schema.boolean()),
        }),
      },
    },
    async (context, request, response) => {
      const denied = await denyUnlessCpmAccess(context, response);
      if (denied) return denied;

      const esClient = (await context.core).elasticsearch.client.asCurrentUser;
      const body = (request.body ?? {}) as {
        watchers?: string[];
        includeForecast?: boolean;
        applyLocks?: boolean;
      };

      const chain: string[] = body.watchers?.length
        ? body.watchers
        : [WATCHER_FORECAST, ...WATCHER_CHAIN];

      const results: WatcherRunResult[] = [];
      for (const id of chain) {
        results.push(await runWatcher(esClient, id));
      }

      const payload: RunChainResponse = { results };
      const allOk = results.every((r) => r.ok);
      return allOk
        ? response.ok({ body: payload })
        : response.customError({ statusCode: 207, body: payload });
    }
  );
}
