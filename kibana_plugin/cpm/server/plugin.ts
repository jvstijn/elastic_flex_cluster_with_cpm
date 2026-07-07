import type {
  CoreSetup,
  CoreStart,
  Logger,
  Plugin,
  PluginInitializerContext,
} from '@kbn/core/server';
import { defineRoutes } from './routes';
import type { CpmPluginSetup, CpmPluginStart } from './types';

export class CpmPlugin implements Plugin<CpmPluginSetup, CpmPluginStart> {
  private readonly logger: Logger;

  constructor(initializerContext: PluginInitializerContext) {
    this.logger = initializerContext.logger.get();
  }

  public setup(core: CoreSetup): CpmPluginSetup {
    const router = core.http.createRouter();
    defineRoutes(router);
    this.logger.debug('CPM server routes registered');
    return {};
  }

  public start(_core: CoreStart): CpmPluginStart {
    return {};
  }

  public stop() {}
}
