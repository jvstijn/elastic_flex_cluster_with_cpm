import type { PluginInitializerContext } from '@kbn/core/server';
import { CpmPlugin } from './plugin';

export const plugin = (initializerContext: PluginInitializerContext) => {
  return new CpmPlugin(initializerContext);
};
