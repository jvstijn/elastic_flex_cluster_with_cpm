import type { CoreSetup, CoreStart, Plugin } from '@kbn/core/public';
import type { ManagementSetup } from '@kbn/management-plugin/public';

export class CpmPlugin implements Plugin {
  public setup(core: CoreSetup, { management }: { management: ManagementSetup }) {
    management.sections.section.ingest.registerApp({
      id: 'cpm',
      title: 'Cluster Pipeline Manager',
      order: 50,
      async mount(params) {
        const { renderManagementApp } = await import('./application');
        const [coreStart] = await core.getStartServices();
        return renderManagementApp(coreStart, params);
      },
    });
  }

  public start(_core: CoreStart) {}

  public stop() {}
}

export function plugin() {
  return new CpmPlugin();
}
