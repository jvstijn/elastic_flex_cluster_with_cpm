"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.defineRoutes = defineRoutes;
var _configSchema = require("@kbn/config-schema");
var _constants = require("../../common/constants");
var _check_access = require("../lib/check_access");
/**
 * Kibana 9.x requires every route to declare `security.authz`. CPM authorizes each
 * request itself against Elasticsearch cluster privileges (see userCanAccessCpm /
 * denyUnlessCpmAccess), so opt out of Kibana's built-in authorization here.
 */
const CPM_ROUTE_SECURITY = {
  authz: {
    enabled: false,
    reason: 'CPM authorizes each request via Elasticsearch _has_privileges (monitor/manage/manage_pipeline/manage_logstash_pipelines).'
  }
};

/** ES index mappings are strict; never persist Kibana/API metadata fields like `id`. */
function withoutApiMeta(body) {
  const {
    id: _id,
    ...rest
  } = body;
  return rest;
}
async function denyUnlessCpmAccess(context, response) {
  const esClient = (await context.core).elasticsearch.client.asCurrentUser;
  if (await (0, _check_access.userCanAccessCpm)(esClient)) {
    return null;
  }
  return response.forbidden({
    body: {
      message: 'Cluster Pipeline Manager requires monitor, manage, manage_pipeline, or manage_logstash_pipelines.'
    }
  });
}
async function runWatcher(esClient, id) {
  try {
    await esClient.transport.request({
      method: 'POST',
      path: `/_watcher/watch/${id}/_execute`,
      body: {
        record_execution: true
      }
    });
    return {
      id,
      ok: true
    };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return {
      id,
      ok: false,
      error: message
    };
  }
}
function defineRoutes(router) {
  router.get({
    path: '/api/cpm/access',
    validate: false,
    security: CPM_ROUTE_SECURITY
  }, async (context, _request, response) => {
    const esClient = (await context.core).elasticsearch.client.asCurrentUser;
    const allowed = await (0, _check_access.userCanAccessCpm)(esClient);
    return response.ok({
      body: {
        allowed
      }
    });
  });
  router.get({
    path: '/api/cpm/clusters',
    validate: false,
    security: CPM_ROUTE_SECURITY
  }, async (context, _request, response) => {
    const denied = await denyUnlessCpmAccess(context, response);
    if (denied) return denied;
    const esClient = (await context.core).elasticsearch.client.asCurrentUser;
    try {
      var _result$hits$hits;
      const result = await esClient.search({
        index: _constants.CPM_CLUSTER_REGISTRY,
        size: 200,
        query: {
          match_all: {}
        },
        sort: [{
          cluster_name: {
            order: 'asc',
            unmapped_type: 'keyword'
          }
        }]
      });
      const clusters = ((_result$hits$hits = result.hits.hits) !== null && _result$hits$hits !== void 0 ? _result$hits$hits : []).map(hit => ({
        id: hit._id,
        ...hit._source
      }));
      return response.ok({
        body: {
          clusters
        }
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return response.customError({
        statusCode: 500,
        body: {
          message
        }
      });
    }
  });
  router.put({
    path: '/api/cpm/clusters/{clusterId}',
    security: CPM_ROUTE_SECURITY,
    validate: {
      params: _configSchema.schema.object({
        clusterId: _configSchema.schema.string()
      }),
      body: _configSchema.schema.object({}, {
        unknowns: 'allow'
      })
    }
  }, async (context, request, response) => {
    const denied = await denyUnlessCpmAccess(context, response);
    if (denied) return denied;
    const esClient = (await context.core).elasticsearch.client.asCurrentUser;
    const clusterId = request.params.clusterId;
    const body = withoutApiMeta(request.body);
    try {
      var _existingSource$clust, _existingSource$clust2;
      const existing = await esClient.get({
        index: _constants.CPM_CLUSTER_REGISTRY,
        id: clusterId
      });
      const existingSource = existing._source;
      const source = {
        ...existingSource,
        ...body,
        cluster_uuid: (_existingSource$clust = existingSource.cluster_uuid) !== null && _existingSource$clust !== void 0 ? _existingSource$clust : clusterId,
        cluster_id: (_existingSource$clust2 = existingSource.cluster_id) !== null && _existingSource$clust2 !== void 0 ? _existingSource$clust2 : clusterId
      };
      await esClient.index({
        index: _constants.CPM_CLUSTER_REGISTRY,
        id: clusterId,
        document: source,
        refresh: 'wait_for'
      });
      return response.ok({
        body: {
          id: clusterId,
          ...source
        }
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return response.customError({
        statusCode: 500,
        body: {
          message
        }
      });
    }
  });
  router.get({
    path: '/api/cpm/scoring',
    validate: false,
    security: CPM_ROUTE_SECURITY
  }, async (context, _request, response) => {
    const denied = await denyUnlessCpmAccess(context, response);
    if (denied) return denied;
    const esClient = (await context.core).elasticsearch.client.asCurrentUser;
    try {
      const result = await esClient.get({
        index: _constants.CPM_ROUTING_CONFIG,
        id: _constants.SCORING_DOC_ID
      });
      return response.ok({
        body: result._source
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return response.customError({
        statusCode: 404,
        body: {
          message
        }
      });
    }
  });
  router.put({
    path: '/api/cpm/scoring',
    security: CPM_ROUTE_SECURITY,
    validate: {
      body: _configSchema.schema.object({
        weights: _configSchema.schema.object({}, {
          unknowns: 'allow'
        }),
        write_queue_threshold: _configSchema.schema.number(),
        shard_max_threshold: _configSchema.schema.number(),
        kafka_group_id: _configSchema.schema.string(),
        alert_threshold: _configSchema.schema.number(),
        forecast_horizon_hours: _configSchema.schema.number()
      })
    }
  }, async (context, request, response) => {
    var _await$context$core$s, _await$context$core$s2;
    const denied = await denyUnlessCpmAccess(context, response);
    if (denied) return denied;
    const esClient = (await context.core).elasticsearch.client.asCurrentUser;
    const body = request.body;
    const username = (_await$context$core$s = (_await$context$core$s2 = (await context.core).security.authc.getCurrentUser()) === null || _await$context$core$s2 === void 0 ? void 0 : _await$context$core$s2.username) !== null && _await$context$core$s !== void 0 ? _await$context$core$s : 'unknown';
    const doc = {
      config_type: _constants.CONFIG_TYPE_SCORING,
      weights: body.weights,
      write_queue_threshold: body.write_queue_threshold,
      shard_max_threshold: body.shard_max_threshold,
      kafka_group_id: body.kafka_group_id,
      alert_threshold: body.alert_threshold,
      forecast_horizon_hours: body.forecast_horizon_hours,
      updated_at: new Date().toISOString(),
      updated_by: username
    };
    try {
      await esClient.index({
        index: _constants.CPM_ROUTING_CONFIG,
        id: _constants.SCORING_DOC_ID,
        document: doc,
        refresh: 'wait_for'
      });
      return response.ok({
        body: doc
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return response.customError({
        statusCode: 500,
        body: {
          message
        }
      });
    }
  });
  router.get({
    path: '/api/cpm/locks',
    validate: false,
    security: CPM_ROUTE_SECURITY
  }, async (context, _request, response) => {
    const denied = await denyUnlessCpmAccess(context, response);
    if (denied) return denied;
    const esClient = (await context.core).elasticsearch.client.asCurrentUser;
    try {
      var _result$hits$hits2;
      const result = await esClient.search({
        index: _constants.CPM_ROUTING_CONFIG,
        size: 500,
        query: {
          term: {
            config_type: _constants.CONFIG_TYPE_STREAM_LOCK
          }
        },
        sort: [{
          dataset: {
            order: 'asc',
            unmapped_type: 'keyword'
          }
        }]
      });
      const locks = ((_result$hits$hits2 = result.hits.hits) !== null && _result$hits$hits2 !== void 0 ? _result$hits$hits2 : []).map(hit => ({
        id: hit._id,
        ...hit._source
      }));
      return response.ok({
        body: {
          locks
        }
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return response.customError({
        statusCode: 500,
        body: {
          message
        }
      });
    }
  });
  router.put({
    path: '/api/cpm/locks/{lockId}',
    security: CPM_ROUTE_SECURITY,
    validate: {
      params: _configSchema.schema.object({
        lockId: _configSchema.schema.string()
      }),
      body: _configSchema.schema.object({}, {
        unknowns: 'allow'
      })
    }
  }, async (context, request, response) => {
    var _await$context$core$s3, _await$context$core$s4;
    const denied = await denyUnlessCpmAccess(context, response);
    if (denied) return denied;
    const esClient = (await context.core).elasticsearch.client.asCurrentUser;
    const lockId = request.params.lockId;
    const body = withoutApiMeta(request.body);
    const username = (_await$context$core$s3 = (_await$context$core$s4 = (await context.core).security.authc.getCurrentUser()) === null || _await$context$core$s4 === void 0 ? void 0 : _await$context$core$s4.username) !== null && _await$context$core$s3 !== void 0 ? _await$context$core$s3 : 'unknown';
    const doc = {
      ...body,
      config_type: _constants.CONFIG_TYPE_STREAM_LOCK,
      locked: true,
      updated_at: new Date().toISOString(),
      updated_by: username
    };
    try {
      await esClient.index({
        index: _constants.CPM_ROUTING_CONFIG,
        id: lockId,
        document: doc,
        refresh: 'wait_for'
      });
      return response.ok({
        body: {
          id: lockId,
          ...doc
        }
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return response.customError({
        statusCode: 500,
        body: {
          message
        }
      });
    }
  });
  router.delete({
    path: '/api/cpm/locks/{lockId}',
    security: CPM_ROUTE_SECURITY,
    validate: {
      params: _configSchema.schema.object({
        lockId: _configSchema.schema.string()
      })
    }
  }, async (context, request, response) => {
    const denied = await denyUnlessCpmAccess(context, response);
    if (denied) return denied;
    const esClient = (await context.core).elasticsearch.client.asCurrentUser;
    const lockId = request.params.lockId;
    try {
      await esClient.delete({
        index: _constants.CPM_ROUTING_CONFIG,
        id: lockId,
        refresh: 'wait_for'
      });
      return response.ok({
        body: {
          deleted: lockId
        }
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return response.customError({
        statusCode: 500,
        body: {
          message
        }
      });
    }
  });
  router.post({
    path: '/api/cpm/run',
    security: CPM_ROUTE_SECURITY,
    validate: {
      body: _configSchema.schema.object({
        watchers: _configSchema.schema.maybe(_configSchema.schema.arrayOf(_configSchema.schema.string())),
        includeForecast: _configSchema.schema.maybe(_configSchema.schema.boolean()),
        applyLocks: _configSchema.schema.maybe(_configSchema.schema.boolean())
      })
    }
  }, async (context, request, response) => {
    var _request$body, _body$watchers;
    const denied = await denyUnlessCpmAccess(context, response);
    if (denied) return denied;
    const esClient = (await context.core).elasticsearch.client.asCurrentUser;
    const body = (_request$body = request.body) !== null && _request$body !== void 0 ? _request$body : {};
    const chain = (_body$watchers = body.watchers) !== null && _body$watchers !== void 0 && _body$watchers.length ? body.watchers : [_constants.WATCHER_FORECAST, ..._constants.WATCHER_CHAIN];
    const results = [];
    for (const id of chain) {
      results.push(await runWatcher(esClient, id));
    }
    const payload = {
      results
    };
    const allOk = results.every(r => r.ok);
    return allOk ? response.ok({
      body: payload
    }) : response.customError({
      statusCode: 207,
      body: payload
    });
  });
}
//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJuYW1lcyI6WyJfY29uZmlnU2NoZW1hIiwicmVxdWlyZSIsIl9jb25zdGFudHMiLCJfY2hlY2tfYWNjZXNzIiwiQ1BNX1JPVVRFX1NFQ1VSSVRZIiwiYXV0aHoiLCJlbmFibGVkIiwicmVhc29uIiwid2l0aG91dEFwaU1ldGEiLCJib2R5IiwiaWQiLCJfaWQiLCJyZXN0IiwiZGVueVVubGVzc0NwbUFjY2VzcyIsImNvbnRleHQiLCJyZXNwb25zZSIsImVzQ2xpZW50IiwiY29yZSIsImVsYXN0aWNzZWFyY2giLCJjbGllbnQiLCJhc0N1cnJlbnRVc2VyIiwidXNlckNhbkFjY2Vzc0NwbSIsImZvcmJpZGRlbiIsIm1lc3NhZ2UiLCJydW5XYXRjaGVyIiwidHJhbnNwb3J0IiwicmVxdWVzdCIsIm1ldGhvZCIsInBhdGgiLCJyZWNvcmRfZXhlY3V0aW9uIiwib2siLCJlcnIiLCJFcnJvciIsIlN0cmluZyIsImVycm9yIiwiZGVmaW5lUm91dGVzIiwicm91dGVyIiwiZ2V0IiwidmFsaWRhdGUiLCJzZWN1cml0eSIsIl9yZXF1ZXN0IiwiYWxsb3dlZCIsImRlbmllZCIsIl9yZXN1bHQkaGl0cyRoaXRzIiwicmVzdWx0Iiwic2VhcmNoIiwiaW5kZXgiLCJDUE1fQ0xVU1RFUl9SRUdJU1RSWSIsInNpemUiLCJxdWVyeSIsIm1hdGNoX2FsbCIsInNvcnQiLCJjbHVzdGVyX25hbWUiLCJvcmRlciIsInVubWFwcGVkX3R5cGUiLCJjbHVzdGVycyIsImhpdHMiLCJtYXAiLCJoaXQiLCJfc291cmNlIiwiY3VzdG9tRXJyb3IiLCJzdGF0dXNDb2RlIiwicHV0IiwicGFyYW1zIiwic2NoZW1hIiwib2JqZWN0IiwiY2x1c3RlcklkIiwic3RyaW5nIiwidW5rbm93bnMiLCJfZXhpc3RpbmdTb3VyY2UkY2x1c3QiLCJfZXhpc3RpbmdTb3VyY2UkY2x1c3QyIiwiZXhpc3RpbmciLCJleGlzdGluZ1NvdXJjZSIsInNvdXJjZSIsImNsdXN0ZXJfdXVpZCIsImNsdXN0ZXJfaWQiLCJkb2N1bWVudCIsInJlZnJlc2giLCJDUE1fUk9VVElOR19DT05GSUciLCJTQ09SSU5HX0RPQ19JRCIsIndlaWdodHMiLCJ3cml0ZV9xdWV1ZV90aHJlc2hvbGQiLCJudW1iZXIiLCJzaGFyZF9tYXhfdGhyZXNob2xkIiwia2Fma2FfZ3JvdXBfaWQiLCJhbGVydF90aHJlc2hvbGQiLCJmb3JlY2FzdF9ob3Jpem9uX2hvdXJzIiwiX2F3YWl0JGNvbnRleHQkY29yZSRzIiwiX2F3YWl0JGNvbnRleHQkY29yZSRzMiIsInVzZXJuYW1lIiwiYXV0aGMiLCJnZXRDdXJyZW50VXNlciIsImRvYyIsImNvbmZpZ190eXBlIiwiQ09ORklHX1RZUEVfU0NPUklORyIsInVwZGF0ZWRfYXQiLCJEYXRlIiwidG9JU09TdHJpbmciLCJ1cGRhdGVkX2J5IiwiX3Jlc3VsdCRoaXRzJGhpdHMyIiwidGVybSIsIkNPTkZJR19UWVBFX1NUUkVBTV9MT0NLIiwiZGF0YXNldCIsImxvY2tzIiwibG9ja0lkIiwiX2F3YWl0JGNvbnRleHQkY29yZSRzMyIsIl9hd2FpdCRjb250ZXh0JGNvcmUkczQiLCJsb2NrZWQiLCJkZWxldGUiLCJkZWxldGVkIiwicG9zdCIsIndhdGNoZXJzIiwibWF5YmUiLCJhcnJheU9mIiwiaW5jbHVkZUZvcmVjYXN0IiwiYm9vbGVhbiIsImFwcGx5TG9ja3MiLCJfcmVxdWVzdCRib2R5IiwiX2JvZHkkd2F0Y2hlcnMiLCJjaGFpbiIsImxlbmd0aCIsIldBVENIRVJfRk9SRUNBU1QiLCJXQVRDSEVSX0NIQUlOIiwicmVzdWx0cyIsInB1c2giLCJwYXlsb2FkIiwiYWxsT2siLCJldmVyeSIsInIiXSwic291cmNlcyI6WyJpbmRleC50cyJdLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgdHlwZSB7IElSb3V0ZXIsIFJlcXVlc3RIYW5kbGVyQ29udGV4dCB9IGZyb20gJ0BrYm4vY29yZS9zZXJ2ZXInO1xuaW1wb3J0IHsgc2NoZW1hIH0gZnJvbSAnQGtibi9jb25maWctc2NoZW1hJztcbmltcG9ydCB7XG4gIENQTV9DTFVTVEVSX1JFR0lTVFJZLFxuICBDUE1fUk9VVElOR19DT05GSUcsXG4gIENPTkZJR19UWVBFX1NDT1JJTkcsXG4gIENPTkZJR19UWVBFX1NUUkVBTV9MT0NLLFxuICBTQ09SSU5HX0RPQ19JRCxcbiAgV0FUQ0hFUl9DSEFJTixcbiAgV0FUQ0hFUl9GT1JFQ0FTVCxcbn0gZnJvbSAnLi4vLi4vY29tbW9uL2NvbnN0YW50cyc7XG5pbXBvcnQgdHlwZSB7IFJ1bkNoYWluUmVzcG9uc2UsIFdhdGNoZXJSdW5SZXN1bHQgfSBmcm9tICcuLi8uLi9jb21tb24vdHlwZXMnO1xuaW1wb3J0IHsgdXNlckNhbkFjY2Vzc0NwbSB9IGZyb20gJy4uL2xpYi9jaGVja19hY2Nlc3MnO1xuXG4vKipcbiAqIEtpYmFuYSA5LnggcmVxdWlyZXMgZXZlcnkgcm91dGUgdG8gZGVjbGFyZSBgc2VjdXJpdHkuYXV0aHpgLiBDUE0gYXV0aG9yaXplcyBlYWNoXG4gKiByZXF1ZXN0IGl0c2VsZiBhZ2FpbnN0IEVsYXN0aWNzZWFyY2ggY2x1c3RlciBwcml2aWxlZ2VzIChzZWUgdXNlckNhbkFjY2Vzc0NwbSAvXG4gKiBkZW55VW5sZXNzQ3BtQWNjZXNzKSwgc28gb3B0IG91dCBvZiBLaWJhbmEncyBidWlsdC1pbiBhdXRob3JpemF0aW9uIGhlcmUuXG4gKi9cbmNvbnN0IENQTV9ST1VURV9TRUNVUklUWSA9IHtcbiAgYXV0aHo6IHtcbiAgICBlbmFibGVkOiBmYWxzZSxcbiAgICByZWFzb246XG4gICAgICAnQ1BNIGF1dGhvcml6ZXMgZWFjaCByZXF1ZXN0IHZpYSBFbGFzdGljc2VhcmNoIF9oYXNfcHJpdmlsZWdlcyAobW9uaXRvci9tYW5hZ2UvbWFuYWdlX3BpcGVsaW5lL21hbmFnZV9sb2dzdGFzaF9waXBlbGluZXMpLicsXG4gIH0sXG59IGFzIGNvbnN0O1xuXG4vKiogRVMgaW5kZXggbWFwcGluZ3MgYXJlIHN0cmljdDsgbmV2ZXIgcGVyc2lzdCBLaWJhbmEvQVBJIG1ldGFkYXRhIGZpZWxkcyBsaWtlIGBpZGAuICovXG5mdW5jdGlvbiB3aXRob3V0QXBpTWV0YTxUIGV4dGVuZHMgUmVjb3JkPHN0cmluZywgdW5rbm93bj4+KGJvZHk6IFQpOiBPbWl0PFQsICdpZCc+IHtcbiAgY29uc3QgeyBpZDogX2lkLCAuLi5yZXN0IH0gPSBib2R5O1xuICByZXR1cm4gcmVzdDtcbn1cblxuYXN5bmMgZnVuY3Rpb24gZGVueVVubGVzc0NwbUFjY2VzcyhcbiAgY29udGV4dDogUmVxdWVzdEhhbmRsZXJDb250ZXh0LFxuICByZXNwb25zZTogeyBmb3JiaWRkZW46IChvcHRzOiB7IGJvZHk6IHsgbWVzc2FnZTogc3RyaW5nIH0gfSkgPT4gdW5rbm93biB9XG4pIHtcbiAgY29uc3QgZXNDbGllbnQgPSAoYXdhaXQgY29udGV4dC5jb3JlKS5lbGFzdGljc2VhcmNoLmNsaWVudC5hc0N1cnJlbnRVc2VyO1xuICBpZiAoYXdhaXQgdXNlckNhbkFjY2Vzc0NwbShlc0NsaWVudCkpIHtcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuICByZXR1cm4gcmVzcG9uc2UuZm9yYmlkZGVuKHtcbiAgICBib2R5OiB7IG1lc3NhZ2U6ICdDbHVzdGVyIFBpcGVsaW5lIE1hbmFnZXIgcmVxdWlyZXMgbW9uaXRvciwgbWFuYWdlLCBtYW5hZ2VfcGlwZWxpbmUsIG9yIG1hbmFnZV9sb2dzdGFzaF9waXBlbGluZXMuJyB9LFxuICB9KTtcbn1cblxuYXN5bmMgZnVuY3Rpb24gcnVuV2F0Y2hlcihcbiAgZXNDbGllbnQ6IHsgdHJhbnNwb3J0OiB7IHJlcXVlc3Q6IChvcHRzOiBvYmplY3QpID0+IFByb21pc2U8dW5rbm93bj4gfSB9LFxuICBpZDogc3RyaW5nXG4pOiBQcm9taXNlPFdhdGNoZXJSdW5SZXN1bHQ+IHtcbiAgdHJ5IHtcbiAgICBhd2FpdCBlc0NsaWVudC50cmFuc3BvcnQucmVxdWVzdCh7XG4gICAgICBtZXRob2Q6ICdQT1NUJyxcbiAgICAgIHBhdGg6IGAvX3dhdGNoZXIvd2F0Y2gvJHtpZH0vX2V4ZWN1dGVgLFxuICAgICAgYm9keTogeyByZWNvcmRfZXhlY3V0aW9uOiB0cnVlIH0sXG4gICAgfSk7XG4gICAgcmV0dXJuIHsgaWQsIG9rOiB0cnVlIH07XG4gIH0gY2F0Y2ggKGVycikge1xuICAgIGNvbnN0IG1lc3NhZ2UgPSBlcnIgaW5zdGFuY2VvZiBFcnJvciA/IGVyci5tZXNzYWdlIDogU3RyaW5nKGVycik7XG4gICAgcmV0dXJuIHsgaWQsIG9rOiBmYWxzZSwgZXJyb3I6IG1lc3NhZ2UgfTtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gZGVmaW5lUm91dGVzKHJvdXRlcjogSVJvdXRlcikge1xuICByb3V0ZXIuZ2V0KHsgcGF0aDogJy9hcGkvY3BtL2FjY2VzcycsIHZhbGlkYXRlOiBmYWxzZSwgc2VjdXJpdHk6IENQTV9ST1VURV9TRUNVUklUWSB9LCBhc3luYyAoY29udGV4dCwgX3JlcXVlc3QsIHJlc3BvbnNlKSA9PiB7XG4gICAgY29uc3QgZXNDbGllbnQgPSAoYXdhaXQgY29udGV4dC5jb3JlKS5lbGFzdGljc2VhcmNoLmNsaWVudC5hc0N1cnJlbnRVc2VyO1xuICAgIGNvbnN0IGFsbG93ZWQgPSBhd2FpdCB1c2VyQ2FuQWNjZXNzQ3BtKGVzQ2xpZW50KTtcbiAgICByZXR1cm4gcmVzcG9uc2Uub2soeyBib2R5OiB7IGFsbG93ZWQgfSB9KTtcbiAgfSk7XG5cbiAgcm91dGVyLmdldCh7IHBhdGg6ICcvYXBpL2NwbS9jbHVzdGVycycsIHZhbGlkYXRlOiBmYWxzZSwgc2VjdXJpdHk6IENQTV9ST1VURV9TRUNVUklUWSB9LCBhc3luYyAoY29udGV4dCwgX3JlcXVlc3QsIHJlc3BvbnNlKSA9PiB7XG4gICAgY29uc3QgZGVuaWVkID0gYXdhaXQgZGVueVVubGVzc0NwbUFjY2Vzcyhjb250ZXh0LCByZXNwb25zZSk7XG4gICAgaWYgKGRlbmllZCkgcmV0dXJuIGRlbmllZDtcblxuICAgIGNvbnN0IGVzQ2xpZW50ID0gKGF3YWl0IGNvbnRleHQuY29yZSkuZWxhc3RpY3NlYXJjaC5jbGllbnQuYXNDdXJyZW50VXNlcjtcbiAgICB0cnkge1xuICAgICAgY29uc3QgcmVzdWx0ID0gYXdhaXQgZXNDbGllbnQuc2VhcmNoKHtcbiAgICAgICAgaW5kZXg6IENQTV9DTFVTVEVSX1JFR0lTVFJZLFxuICAgICAgICBzaXplOiAyMDAsXG4gICAgICAgIHF1ZXJ5OiB7IG1hdGNoX2FsbDoge30gfSxcbiAgICAgICAgc29ydDogW3sgY2x1c3Rlcl9uYW1lOiB7IG9yZGVyOiAnYXNjJywgdW5tYXBwZWRfdHlwZTogJ2tleXdvcmQnIH0gfV0sXG4gICAgICB9KTtcbiAgICAgIGNvbnN0IGNsdXN0ZXJzID0gKHJlc3VsdC5oaXRzLmhpdHMgPz8gW10pLm1hcCgoaGl0KSA9PiAoe1xuICAgICAgICBpZDogaGl0Ll9pZCxcbiAgICAgICAgLi4uKGhpdC5fc291cmNlIGFzIG9iamVjdCksXG4gICAgICB9KSk7XG4gICAgICByZXR1cm4gcmVzcG9uc2Uub2soeyBib2R5OiB7IGNsdXN0ZXJzIH0gfSk7XG4gICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICBjb25zdCBtZXNzYWdlID0gZXJyIGluc3RhbmNlb2YgRXJyb3IgPyBlcnIubWVzc2FnZSA6IFN0cmluZyhlcnIpO1xuICAgICAgcmV0dXJuIHJlc3BvbnNlLmN1c3RvbUVycm9yKHsgc3RhdHVzQ29kZTogNTAwLCBib2R5OiB7IG1lc3NhZ2UgfSB9KTtcbiAgICB9XG4gIH0pO1xuXG4gIHJvdXRlci5wdXQoXG4gICAge1xuICAgICAgcGF0aDogJy9hcGkvY3BtL2NsdXN0ZXJzL3tjbHVzdGVySWR9JyxcbiAgICAgIHNlY3VyaXR5OiBDUE1fUk9VVEVfU0VDVVJJVFksXG4gICAgICB2YWxpZGF0ZToge1xuICAgICAgICBwYXJhbXM6IHNjaGVtYS5vYmplY3QoeyBjbHVzdGVySWQ6IHNjaGVtYS5zdHJpbmcoKSB9KSxcbiAgICAgICAgYm9keTogc2NoZW1hLm9iamVjdCh7fSwgeyB1bmtub3duczogJ2FsbG93JyB9KSxcbiAgICAgIH0sXG4gICAgfSxcbiAgICBhc3luYyAoY29udGV4dCwgcmVxdWVzdCwgcmVzcG9uc2UpID0+IHtcbiAgICAgIGNvbnN0IGRlbmllZCA9IGF3YWl0IGRlbnlVbmxlc3NDcG1BY2Nlc3MoY29udGV4dCwgcmVzcG9uc2UpO1xuICAgICAgaWYgKGRlbmllZCkgcmV0dXJuIGRlbmllZDtcblxuICAgICAgY29uc3QgZXNDbGllbnQgPSAoYXdhaXQgY29udGV4dC5jb3JlKS5lbGFzdGljc2VhcmNoLmNsaWVudC5hc0N1cnJlbnRVc2VyO1xuICAgICAgY29uc3QgY2x1c3RlcklkID0gKHJlcXVlc3QucGFyYW1zIGFzIHsgY2x1c3RlcklkOiBzdHJpbmcgfSkuY2x1c3RlcklkO1xuICAgICAgY29uc3QgYm9keSA9IHdpdGhvdXRBcGlNZXRhKHJlcXVlc3QuYm9keSBhcyBSZWNvcmQ8c3RyaW5nLCB1bmtub3duPik7XG5cbiAgICAgIHRyeSB7XG4gICAgICAgIGNvbnN0IGV4aXN0aW5nID0gYXdhaXQgZXNDbGllbnQuZ2V0KHtcbiAgICAgICAgICBpbmRleDogQ1BNX0NMVVNURVJfUkVHSVNUUlksXG4gICAgICAgICAgaWQ6IGNsdXN0ZXJJZCxcbiAgICAgICAgfSk7XG4gICAgICAgIGNvbnN0IGV4aXN0aW5nU291cmNlID0gZXhpc3RpbmcuX3NvdXJjZSBhcyBSZWNvcmQ8c3RyaW5nLCB1bmtub3duPjtcbiAgICAgICAgY29uc3Qgc291cmNlID0ge1xuICAgICAgICAgIC4uLmV4aXN0aW5nU291cmNlLFxuICAgICAgICAgIC4uLmJvZHksXG4gICAgICAgICAgY2x1c3Rlcl91dWlkOiBleGlzdGluZ1NvdXJjZS5jbHVzdGVyX3V1aWQgPz8gY2x1c3RlcklkLFxuICAgICAgICAgIGNsdXN0ZXJfaWQ6IGV4aXN0aW5nU291cmNlLmNsdXN0ZXJfaWQgPz8gY2x1c3RlcklkLFxuICAgICAgICB9O1xuICAgICAgICBhd2FpdCBlc0NsaWVudC5pbmRleCh7XG4gICAgICAgICAgaW5kZXg6IENQTV9DTFVTVEVSX1JFR0lTVFJZLFxuICAgICAgICAgIGlkOiBjbHVzdGVySWQsXG4gICAgICAgICAgZG9jdW1lbnQ6IHNvdXJjZSxcbiAgICAgICAgICByZWZyZXNoOiAnd2FpdF9mb3InLFxuICAgICAgICB9KTtcbiAgICAgICAgcmV0dXJuIHJlc3BvbnNlLm9rKHsgYm9keTogeyBpZDogY2x1c3RlcklkLCAuLi5zb3VyY2UgfSB9KTtcbiAgICAgIH0gY2F0Y2ggKGVycikge1xuICAgICAgICBjb25zdCBtZXNzYWdlID0gZXJyIGluc3RhbmNlb2YgRXJyb3IgPyBlcnIubWVzc2FnZSA6IFN0cmluZyhlcnIpO1xuICAgICAgICByZXR1cm4gcmVzcG9uc2UuY3VzdG9tRXJyb3IoeyBzdGF0dXNDb2RlOiA1MDAsIGJvZHk6IHsgbWVzc2FnZSB9IH0pO1xuICAgICAgfVxuICAgIH1cbiAgKTtcblxuICByb3V0ZXIuZ2V0KFxuICAgIHsgcGF0aDogJy9hcGkvY3BtL3Njb3JpbmcnLCB2YWxpZGF0ZTogZmFsc2UsIHNlY3VyaXR5OiBDUE1fUk9VVEVfU0VDVVJJVFkgfSxcbiAgICBhc3luYyAoY29udGV4dCwgX3JlcXVlc3QsIHJlc3BvbnNlKSA9PiB7XG4gICAgICBjb25zdCBkZW5pZWQgPSBhd2FpdCBkZW55VW5sZXNzQ3BtQWNjZXNzKGNvbnRleHQsIHJlc3BvbnNlKTtcbiAgICAgIGlmIChkZW5pZWQpIHJldHVybiBkZW5pZWQ7XG5cbiAgICAgIGNvbnN0IGVzQ2xpZW50ID0gKGF3YWl0IGNvbnRleHQuY29yZSkuZWxhc3RpY3NlYXJjaC5jbGllbnQuYXNDdXJyZW50VXNlcjtcbiAgICAgIHRyeSB7XG4gICAgICAgIGNvbnN0IHJlc3VsdCA9IGF3YWl0IGVzQ2xpZW50LmdldCh7XG4gICAgICAgICAgaW5kZXg6IENQTV9ST1VUSU5HX0NPTkZJRyxcbiAgICAgICAgICBpZDogU0NPUklOR19ET0NfSUQsXG4gICAgICAgIH0pO1xuICAgICAgICByZXR1cm4gcmVzcG9uc2Uub2soeyBib2R5OiByZXN1bHQuX3NvdXJjZSB9KTtcbiAgICAgIH0gY2F0Y2ggKGVycikge1xuICAgICAgICBjb25zdCBtZXNzYWdlID0gZXJyIGluc3RhbmNlb2YgRXJyb3IgPyBlcnIubWVzc2FnZSA6IFN0cmluZyhlcnIpO1xuICAgICAgICByZXR1cm4gcmVzcG9uc2UuY3VzdG9tRXJyb3IoeyBzdGF0dXNDb2RlOiA0MDQsIGJvZHk6IHsgbWVzc2FnZSB9IH0pO1xuICAgICAgfVxuICAgIH1cbiAgKTtcblxuICByb3V0ZXIucHV0KHsgcGF0aDogJy9hcGkvY3BtL3Njb3JpbmcnLCBzZWN1cml0eTogQ1BNX1JPVVRFX1NFQ1VSSVRZLCB2YWxpZGF0ZToge1xuICAgIGJvZHk6IHNjaGVtYS5vYmplY3Qoe1xuICAgICAgd2VpZ2h0czogc2NoZW1hLm9iamVjdCh7fSwgeyB1bmtub3duczogJ2FsbG93JyB9KSxcbiAgICAgIHdyaXRlX3F1ZXVlX3RocmVzaG9sZDogc2NoZW1hLm51bWJlcigpLFxuICAgICAgc2hhcmRfbWF4X3RocmVzaG9sZDogc2NoZW1hLm51bWJlcigpLFxuICAgICAga2Fma2FfZ3JvdXBfaWQ6IHNjaGVtYS5zdHJpbmcoKSxcbiAgICAgIGFsZXJ0X3RocmVzaG9sZDogc2NoZW1hLm51bWJlcigpLFxuICAgICAgZm9yZWNhc3RfaG9yaXpvbl9ob3Vyczogc2NoZW1hLm51bWJlcigpLFxuICAgIH0pLFxuICB9IH0sIGFzeW5jIChjb250ZXh0LCByZXF1ZXN0LCByZXNwb25zZSkgPT4ge1xuICAgIGNvbnN0IGRlbmllZCA9IGF3YWl0IGRlbnlVbmxlc3NDcG1BY2Nlc3MoY29udGV4dCwgcmVzcG9uc2UpO1xuICAgIGlmIChkZW5pZWQpIHJldHVybiBkZW5pZWQ7XG5cbiAgICBjb25zdCBlc0NsaWVudCA9IChhd2FpdCBjb250ZXh0LmNvcmUpLmVsYXN0aWNzZWFyY2guY2xpZW50LmFzQ3VycmVudFVzZXI7XG4gICAgY29uc3QgYm9keSA9IHJlcXVlc3QuYm9keSBhcyB7XG4gICAgICB3ZWlnaHRzOiBSZWNvcmQ8c3RyaW5nLCBudW1iZXI+O1xuICAgICAgd3JpdGVfcXVldWVfdGhyZXNob2xkOiBudW1iZXI7XG4gICAgICBzaGFyZF9tYXhfdGhyZXNob2xkOiBudW1iZXI7XG4gICAgICBrYWZrYV9ncm91cF9pZDogc3RyaW5nO1xuICAgICAgYWxlcnRfdGhyZXNob2xkOiBudW1iZXI7XG4gICAgICBmb3JlY2FzdF9ob3Jpem9uX2hvdXJzOiBudW1iZXI7XG4gICAgfTtcbiAgICBjb25zdCB1c2VybmFtZSA9IChhd2FpdCBjb250ZXh0LmNvcmUpLnNlY3VyaXR5LmF1dGhjLmdldEN1cnJlbnRVc2VyKCk/LnVzZXJuYW1lID8/ICd1bmtub3duJztcblxuICAgIGNvbnN0IGRvYyA9IHtcbiAgICAgIGNvbmZpZ190eXBlOiBDT05GSUdfVFlQRV9TQ09SSU5HLFxuICAgICAgd2VpZ2h0czogYm9keS53ZWlnaHRzLFxuICAgICAgd3JpdGVfcXVldWVfdGhyZXNob2xkOiBib2R5LndyaXRlX3F1ZXVlX3RocmVzaG9sZCxcbiAgICAgIHNoYXJkX21heF90aHJlc2hvbGQ6IGJvZHkuc2hhcmRfbWF4X3RocmVzaG9sZCxcbiAgICAgIGthZmthX2dyb3VwX2lkOiBib2R5LmthZmthX2dyb3VwX2lkLFxuICAgICAgYWxlcnRfdGhyZXNob2xkOiBib2R5LmFsZXJ0X3RocmVzaG9sZCxcbiAgICAgIGZvcmVjYXN0X2hvcml6b25faG91cnM6IGJvZHkuZm9yZWNhc3RfaG9yaXpvbl9ob3VycyxcbiAgICAgIHVwZGF0ZWRfYXQ6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgIHVwZGF0ZWRfYnk6IHVzZXJuYW1lLFxuICAgIH07XG5cbiAgICB0cnkge1xuICAgICAgYXdhaXQgZXNDbGllbnQuaW5kZXgoe1xuICAgICAgICBpbmRleDogQ1BNX1JPVVRJTkdfQ09ORklHLFxuICAgICAgICBpZDogU0NPUklOR19ET0NfSUQsXG4gICAgICAgIGRvY3VtZW50OiBkb2MsXG4gICAgICAgIHJlZnJlc2g6ICd3YWl0X2ZvcicsXG4gICAgICB9KTtcbiAgICAgIHJldHVybiByZXNwb25zZS5vayh7IGJvZHk6IGRvYyB9KTtcbiAgICB9IGNhdGNoIChlcnIpIHtcbiAgICAgIGNvbnN0IG1lc3NhZ2UgPSBlcnIgaW5zdGFuY2VvZiBFcnJvciA/IGVyci5tZXNzYWdlIDogU3RyaW5nKGVycik7XG4gICAgICByZXR1cm4gcmVzcG9uc2UuY3VzdG9tRXJyb3IoeyBzdGF0dXNDb2RlOiA1MDAsIGJvZHk6IHsgbWVzc2FnZSB9IH0pO1xuICAgIH1cbiAgfSk7XG5cbiAgcm91dGVyLmdldCh7IHBhdGg6ICcvYXBpL2NwbS9sb2NrcycsIHZhbGlkYXRlOiBmYWxzZSwgc2VjdXJpdHk6IENQTV9ST1VURV9TRUNVUklUWSB9LCBhc3luYyAoY29udGV4dCwgX3JlcXVlc3QsIHJlc3BvbnNlKSA9PiB7XG4gICAgY29uc3QgZGVuaWVkID0gYXdhaXQgZGVueVVubGVzc0NwbUFjY2Vzcyhjb250ZXh0LCByZXNwb25zZSk7XG4gICAgaWYgKGRlbmllZCkgcmV0dXJuIGRlbmllZDtcblxuICAgIGNvbnN0IGVzQ2xpZW50ID0gKGF3YWl0IGNvbnRleHQuY29yZSkuZWxhc3RpY3NlYXJjaC5jbGllbnQuYXNDdXJyZW50VXNlcjtcbiAgICB0cnkge1xuICAgICAgY29uc3QgcmVzdWx0ID0gYXdhaXQgZXNDbGllbnQuc2VhcmNoKHtcbiAgICAgICAgaW5kZXg6IENQTV9ST1VUSU5HX0NPTkZJRyxcbiAgICAgICAgc2l6ZTogNTAwLFxuICAgICAgICBxdWVyeTogeyB0ZXJtOiB7IGNvbmZpZ190eXBlOiBDT05GSUdfVFlQRV9TVFJFQU1fTE9DSyB9IH0sXG4gICAgICAgIHNvcnQ6IFt7IGRhdGFzZXQ6IHsgb3JkZXI6ICdhc2MnLCB1bm1hcHBlZF90eXBlOiAna2V5d29yZCcgfSB9XSxcbiAgICAgIH0pO1xuICAgICAgY29uc3QgbG9ja3MgPSAocmVzdWx0LmhpdHMuaGl0cyA/PyBbXSkubWFwKChoaXQpID0+ICh7XG4gICAgICAgIGlkOiBoaXQuX2lkLFxuICAgICAgICAuLi4oaGl0Ll9zb3VyY2UgYXMgb2JqZWN0KSxcbiAgICAgIH0pKTtcbiAgICAgIHJldHVybiByZXNwb25zZS5vayh7IGJvZHk6IHsgbG9ja3MgfSB9KTtcbiAgICB9IGNhdGNoIChlcnIpIHtcbiAgICAgIGNvbnN0IG1lc3NhZ2UgPSBlcnIgaW5zdGFuY2VvZiBFcnJvciA/IGVyci5tZXNzYWdlIDogU3RyaW5nKGVycik7XG4gICAgICByZXR1cm4gcmVzcG9uc2UuY3VzdG9tRXJyb3IoeyBzdGF0dXNDb2RlOiA1MDAsIGJvZHk6IHsgbWVzc2FnZSB9IH0pO1xuICAgIH1cbiAgfSk7XG5cbiAgcm91dGVyLnB1dChcbiAgICB7XG4gICAgICBwYXRoOiAnL2FwaS9jcG0vbG9ja3Mve2xvY2tJZH0nLFxuICAgICAgc2VjdXJpdHk6IENQTV9ST1VURV9TRUNVUklUWSxcbiAgICAgIHZhbGlkYXRlOiB7XG4gICAgICAgIHBhcmFtczogc2NoZW1hLm9iamVjdCh7IGxvY2tJZDogc2NoZW1hLnN0cmluZygpIH0pLFxuICAgICAgICBib2R5OiBzY2hlbWEub2JqZWN0KHt9LCB7IHVua25vd25zOiAnYWxsb3cnIH0pLFxuICAgICAgfSxcbiAgICB9LFxuICAgIGFzeW5jIChjb250ZXh0LCByZXF1ZXN0LCByZXNwb25zZSkgPT4ge1xuICAgICAgY29uc3QgZGVuaWVkID0gYXdhaXQgZGVueVVubGVzc0NwbUFjY2Vzcyhjb250ZXh0LCByZXNwb25zZSk7XG4gICAgICBpZiAoZGVuaWVkKSByZXR1cm4gZGVuaWVkO1xuXG4gICAgICBjb25zdCBlc0NsaWVudCA9IChhd2FpdCBjb250ZXh0LmNvcmUpLmVsYXN0aWNzZWFyY2guY2xpZW50LmFzQ3VycmVudFVzZXI7XG4gICAgICBjb25zdCBsb2NrSWQgPSAocmVxdWVzdC5wYXJhbXMgYXMgeyBsb2NrSWQ6IHN0cmluZyB9KS5sb2NrSWQ7XG4gICAgICBjb25zdCBib2R5ID0gd2l0aG91dEFwaU1ldGEocmVxdWVzdC5ib2R5IGFzIFJlY29yZDxzdHJpbmcsIHVua25vd24+KTtcbiAgICAgIGNvbnN0IHVzZXJuYW1lID0gKGF3YWl0IGNvbnRleHQuY29yZSkuc2VjdXJpdHkuYXV0aGMuZ2V0Q3VycmVudFVzZXIoKT8udXNlcm5hbWUgPz8gJ3Vua25vd24nO1xuXG4gICAgICBjb25zdCBkb2MgPSB7XG4gICAgICAgIC4uLmJvZHksXG4gICAgICAgIGNvbmZpZ190eXBlOiBDT05GSUdfVFlQRV9TVFJFQU1fTE9DSyxcbiAgICAgICAgbG9ja2VkOiB0cnVlLFxuICAgICAgICB1cGRhdGVkX2F0OiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICAgIHVwZGF0ZWRfYnk6IHVzZXJuYW1lLFxuICAgICAgfTtcblxuICAgICAgdHJ5IHtcbiAgICAgICAgYXdhaXQgZXNDbGllbnQuaW5kZXgoe1xuICAgICAgICAgIGluZGV4OiBDUE1fUk9VVElOR19DT05GSUcsXG4gICAgICAgICAgaWQ6IGxvY2tJZCxcbiAgICAgICAgICBkb2N1bWVudDogZG9jLFxuICAgICAgICAgIHJlZnJlc2g6ICd3YWl0X2ZvcicsXG4gICAgICAgIH0pO1xuICAgICAgICByZXR1cm4gcmVzcG9uc2Uub2soeyBib2R5OiB7IGlkOiBsb2NrSWQsIC4uLmRvYyB9IH0pO1xuICAgICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICAgIGNvbnN0IG1lc3NhZ2UgPSBlcnIgaW5zdGFuY2VvZiBFcnJvciA/IGVyci5tZXNzYWdlIDogU3RyaW5nKGVycik7XG4gICAgICAgIHJldHVybiByZXNwb25zZS5jdXN0b21FcnJvcih7IHN0YXR1c0NvZGU6IDUwMCwgYm9keTogeyBtZXNzYWdlIH0gfSk7XG4gICAgICB9XG4gICAgfVxuICApO1xuXG4gIHJvdXRlci5kZWxldGUoXG4gICAge1xuICAgICAgcGF0aDogJy9hcGkvY3BtL2xvY2tzL3tsb2NrSWR9JyxcbiAgICAgIHNlY3VyaXR5OiBDUE1fUk9VVEVfU0VDVVJJVFksXG4gICAgICB2YWxpZGF0ZToge1xuICAgICAgICBwYXJhbXM6IHNjaGVtYS5vYmplY3QoeyBsb2NrSWQ6IHNjaGVtYS5zdHJpbmcoKSB9KSxcbiAgICAgIH0sXG4gICAgfSxcbiAgICBhc3luYyAoY29udGV4dCwgcmVxdWVzdCwgcmVzcG9uc2UpID0+IHtcbiAgICAgIGNvbnN0IGRlbmllZCA9IGF3YWl0IGRlbnlVbmxlc3NDcG1BY2Nlc3MoY29udGV4dCwgcmVzcG9uc2UpO1xuICAgICAgaWYgKGRlbmllZCkgcmV0dXJuIGRlbmllZDtcblxuICAgICAgY29uc3QgZXNDbGllbnQgPSAoYXdhaXQgY29udGV4dC5jb3JlKS5lbGFzdGljc2VhcmNoLmNsaWVudC5hc0N1cnJlbnRVc2VyO1xuICAgICAgY29uc3QgbG9ja0lkID0gKHJlcXVlc3QucGFyYW1zIGFzIHsgbG9ja0lkOiBzdHJpbmcgfSkubG9ja0lkO1xuICAgICAgdHJ5IHtcbiAgICAgICAgYXdhaXQgZXNDbGllbnQuZGVsZXRlKHtcbiAgICAgICAgICBpbmRleDogQ1BNX1JPVVRJTkdfQ09ORklHLFxuICAgICAgICAgIGlkOiBsb2NrSWQsXG4gICAgICAgICAgcmVmcmVzaDogJ3dhaXRfZm9yJyxcbiAgICAgICAgfSk7XG4gICAgICAgIHJldHVybiByZXNwb25zZS5vayh7IGJvZHk6IHsgZGVsZXRlZDogbG9ja0lkIH0gfSk7XG4gICAgICB9IGNhdGNoIChlcnIpIHtcbiAgICAgICAgY29uc3QgbWVzc2FnZSA9IGVyciBpbnN0YW5jZW9mIEVycm9yID8gZXJyLm1lc3NhZ2UgOiBTdHJpbmcoZXJyKTtcbiAgICAgICAgcmV0dXJuIHJlc3BvbnNlLmN1c3RvbUVycm9yKHsgc3RhdHVzQ29kZTogNTAwLCBib2R5OiB7IG1lc3NhZ2UgfSB9KTtcbiAgICAgIH1cbiAgICB9XG4gICk7XG5cbiAgcm91dGVyLnBvc3QoXG4gICAge1xuICAgICAgcGF0aDogJy9hcGkvY3BtL3J1bicsXG4gICAgICBzZWN1cml0eTogQ1BNX1JPVVRFX1NFQ1VSSVRZLFxuICAgICAgdmFsaWRhdGU6IHtcbiAgICAgICAgYm9keTogc2NoZW1hLm9iamVjdCh7XG4gICAgICAgICAgd2F0Y2hlcnM6IHNjaGVtYS5tYXliZShzY2hlbWEuYXJyYXlPZihzY2hlbWEuc3RyaW5nKCkpKSxcbiAgICAgICAgICBpbmNsdWRlRm9yZWNhc3Q6IHNjaGVtYS5tYXliZShzY2hlbWEuYm9vbGVhbigpKSxcbiAgICAgICAgICBhcHBseUxvY2tzOiBzY2hlbWEubWF5YmUoc2NoZW1hLmJvb2xlYW4oKSksXG4gICAgICAgIH0pLFxuICAgICAgfSxcbiAgICB9LFxuICAgIGFzeW5jIChjb250ZXh0LCByZXF1ZXN0LCByZXNwb25zZSkgPT4ge1xuICAgICAgY29uc3QgZGVuaWVkID0gYXdhaXQgZGVueVVubGVzc0NwbUFjY2Vzcyhjb250ZXh0LCByZXNwb25zZSk7XG4gICAgICBpZiAoZGVuaWVkKSByZXR1cm4gZGVuaWVkO1xuXG4gICAgICBjb25zdCBlc0NsaWVudCA9IChhd2FpdCBjb250ZXh0LmNvcmUpLmVsYXN0aWNzZWFyY2guY2xpZW50LmFzQ3VycmVudFVzZXI7XG4gICAgICBjb25zdCBib2R5ID0gKHJlcXVlc3QuYm9keSA/PyB7fSkgYXMge1xuICAgICAgICB3YXRjaGVycz86IHN0cmluZ1tdO1xuICAgICAgICBpbmNsdWRlRm9yZWNhc3Q/OiBib29sZWFuO1xuICAgICAgICBhcHBseUxvY2tzPzogYm9vbGVhbjtcbiAgICAgIH07XG5cbiAgICAgIGNvbnN0IGNoYWluOiBzdHJpbmdbXSA9IGJvZHkud2F0Y2hlcnM/Lmxlbmd0aFxuICAgICAgICA/IGJvZHkud2F0Y2hlcnNcbiAgICAgICAgOiBbV0FUQ0hFUl9GT1JFQ0FTVCwgLi4uV0FUQ0hFUl9DSEFJTl07XG5cbiAgICAgIGNvbnN0IHJlc3VsdHM6IFdhdGNoZXJSdW5SZXN1bHRbXSA9IFtdO1xuICAgICAgZm9yIChjb25zdCBpZCBvZiBjaGFpbikge1xuICAgICAgICByZXN1bHRzLnB1c2goYXdhaXQgcnVuV2F0Y2hlcihlc0NsaWVudCwgaWQpKTtcbiAgICAgIH1cblxuICAgICAgY29uc3QgcGF5bG9hZDogUnVuQ2hhaW5SZXNwb25zZSA9IHsgcmVzdWx0cyB9O1xuICAgICAgY29uc3QgYWxsT2sgPSByZXN1bHRzLmV2ZXJ5KChyKSA9PiByLm9rKTtcbiAgICAgIHJldHVybiBhbGxPa1xuICAgICAgICA/IHJlc3BvbnNlLm9rKHsgYm9keTogcGF5bG9hZCB9KVxuICAgICAgICA6IHJlc3BvbnNlLmN1c3RvbUVycm9yKHsgc3RhdHVzQ29kZTogMjA3LCBib2R5OiBwYXlsb2FkIH0pO1xuICAgIH1cbiAgKTtcbn1cbiJdLCJtYXBwaW5ncyI6Ijs7Ozs7O0FBQ0EsSUFBQUEsYUFBQSxHQUFBQyxPQUFBO0FBQ0EsSUFBQUMsVUFBQSxHQUFBRCxPQUFBO0FBVUEsSUFBQUUsYUFBQSxHQUFBRixPQUFBO0FBRUE7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBLE1BQU1HLGtCQUFrQixHQUFHO0VBQ3pCQyxLQUFLLEVBQUU7SUFDTEMsT0FBTyxFQUFFLEtBQUs7SUFDZEMsTUFBTSxFQUNKO0VBQ0o7QUFDRixDQUFVOztBQUVWO0FBQ0EsU0FBU0MsY0FBY0EsQ0FBb0NDLElBQU8sRUFBaUI7RUFDakYsTUFBTTtJQUFFQyxFQUFFLEVBQUVDLEdBQUc7SUFBRSxHQUFHQztFQUFLLENBQUMsR0FBR0gsSUFBSTtFQUNqQyxPQUFPRyxJQUFJO0FBQ2I7QUFFQSxlQUFlQyxtQkFBbUJBLENBQ2hDQyxPQUE4QixFQUM5QkMsUUFBeUUsRUFDekU7RUFDQSxNQUFNQyxRQUFRLEdBQUcsQ0FBQyxNQUFNRixPQUFPLENBQUNHLElBQUksRUFBRUMsYUFBYSxDQUFDQyxNQUFNLENBQUNDLGFBQWE7RUFDeEUsSUFBSSxNQUFNLElBQUFDLDhCQUFnQixFQUFDTCxRQUFRLENBQUMsRUFBRTtJQUNwQyxPQUFPLElBQUk7RUFDYjtFQUNBLE9BQU9ELFFBQVEsQ0FBQ08sU0FBUyxDQUFDO0lBQ3hCYixJQUFJLEVBQUU7TUFBRWMsT0FBTyxFQUFFO0lBQW9HO0VBQ3ZILENBQUMsQ0FBQztBQUNKO0FBRUEsZUFBZUMsVUFBVUEsQ0FDdkJSLFFBQXdFLEVBQ3hFTixFQUFVLEVBQ2lCO0VBQzNCLElBQUk7SUFDRixNQUFNTSxRQUFRLENBQUNTLFNBQVMsQ0FBQ0MsT0FBTyxDQUFDO01BQy9C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bUQsT0FBTyxDQUFDLENBQUMsQ0FBQztRQUMvQ0MsVUFBVSxFQUFFcEQsb0JBQU0sQ0FBQ2dELEtBQUssQ0FBQ2hELG9CQUFNLENBQUNtRCxPQUFPLENBQUMsQ0FBQztNQUMzQyxDQUFDO0lBQ0g7RUFDRixDQUFDLEVBQ0QsT0FBT3JHLE9BQU8sRUFBRVksT0FBTyxFQUFFWCxRQUFRLEtBQUs7SUFBQSxJQUFBc0csYUFBQSxFQUFBQyxjQUFBO0lBQ3BDLE1BQU01RSxNQUFNLEdBQUcsTUFBTTdCLG1CQUFtQixDQUFDQyxPQUFPLEVBQUVDLFFBQVEsQ0FBQztJQUMzRCxJQUFJMkIsTUFBTSxFQUFFLE9BQU9BLE1BQU07SUFFekIsTUFBTTFCLFFBQVEsR0FBRyxDQUFDLE1BQU1GLE9BQU8sQ0FBQ0csSUFBSSxFQUFFQyxhQUFhLENBQUNDLE1BQU0sQ0FBQ0MsYUFBYTtJQUN4RSxNQUFNWCxJQUFJLElBQUE0RyxhQUFBLEdBQUkzRixPQUFPLENBQUNqQixJQUFJLGNBQUE0RyxhQUFBLGNBQUFBLGFBQUEsR0FBSSxDQUFDLENBSTlCO0lBRUQsTUFBTUUsS0FBZSxHQUFHLENBQUFELGNBQUEsR0FBQTdHLElBQUksQ0FBQ3NHLFFBQVEsY0FBQU8sY0FBQSxlQUFiQSxjQUFBLENBQWVFLE1BQU0sR0FDekMvRyxJQUFJLENBQUNzRyxRQUFRLEdBQ2IsQ0FBQ1UsMkJBQWdCLEVBQUUsR0FBR0Msd0JBQWEsQ0FBQztJQUV4QyxNQUFNQyxPQUEyQixHQUFHLEVBQUU7SUFDdEMsS0FBSyxNQUFNakgsRUFBRSxJQUFJNkcsS0FBSyxFQUFFO01BQ3RCSSxPQUFPLENBQUNDLElBQUksQ0FBQyxNQUFNcEcsVUFBVSxDQUFDUixRQUFRLEVBQUVOLEVBQUUsQ0FBQyxDQUFDO0lBQzlDO0lBRUEsTUFBTW1ILE9BQXlCLEdBQUc7TUFBRUY7SUFBUSxDQUFDO0lBQzdDLE1BQU1HLEtBQUssR0FBR0gsT0FBTyxDQUFDSSxLQUFLLENBQUVDLENBQUMsSUFBS0EsQ0FBQyxDQUFDbEcsRUFBRSxDQUFDO0lBQ3hDLE9BQU9nRyxLQUFLLEdBQ1IvRyxRQUFRLENBQUNlLEVBQUUsQ0FBQztNQUFFckIsSUFBSSxFQUFFb0g7SUFBUSxDQUFDLENBQUMsR0FDOUI5RyxRQUFRLENBQUM2QyxXQUFXLENBQUM7TUFBRUMsVUFBVSxFQUFFLEdBQUc7TUFBRXBELElBQUksRUFBRW9IO0lBQVEsQ0FBQyxDQUFDO0VBQzlELENBQ0YsQ0FBQztBQUNIIiwiaWdub3JlTGlzdCI6W119