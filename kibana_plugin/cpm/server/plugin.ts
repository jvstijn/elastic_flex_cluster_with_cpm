import type {
  CoreSetup,
  CoreStart,
  Logger,
  Plugin,
  PluginInitializerContext,
} from '@kbn/core/server';
import { CPM_PLUGIN_ID, CPM_VIEW_CLUSTER_PRIVILEGES } from '../common/constants';
import { defineRoutes } from './routes';
import type { CpmPluginSetup, CpmPluginSetupDeps, CpmPluginStart } from './types';

export class CpmPlugin implements Plugin<CpmPluginSetup, CpmPluginStart> {
  private readonly logger: Logger;

  constructor(initializerContext: PluginInitializerContext) {
    this.logger = initializerContext.logger.get();
  }

  public setup(core: CoreSetup, { features }: CpmPluginSetupDeps): CpmPluginSetup {
    const router = core.http.createRouter();

    features.registerElasticsearchFeature({
      id: CPM_PLUGIN_ID,
      management: {
        ingest: [CPM_PLUGIN_ID],
      },
      privileges: CPM_VIEW_CLUSTER_PRIVILEGES.map((privilege) => ({
        requiredClusterPrivileges: [privilege],
        ui: [],
      })),
    });

    defineRoutes(router);
    this.logger.debug('CPM server routes registered');
    return {};
  }

  public start(_core: CoreStart): CpmPluginStart {
    return {};
  }

  public stop() {}
}
