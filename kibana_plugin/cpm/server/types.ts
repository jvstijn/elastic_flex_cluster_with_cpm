import type { FeaturesPluginSetup } from '@kbn/features-plugin/server';

export interface CpmPluginSetup {}

export interface CpmPluginStart {}

export interface CpmPluginSetupDeps {
  features: FeaturesPluginSetup;
}

