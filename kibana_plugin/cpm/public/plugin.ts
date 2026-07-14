import type { CoreSetup, CoreStart, Plugin } from '@kbn/core/public';
import type { ManagementSetup } from '@kbn/management-plugin/public';

type CpmManagementApp = ReturnType<ManagementSetup['sections']['section']['ingest']['registerApp']>;

export class CpmPlugin implements Plugin {
  private cpmApp?: CpmManagementApp;

  public setup(core: CoreSetup, { management }: { management: ManagementSetup }) {
    this.cpmApp = management.sections.section.ingest.registerApp({
      id: 'cpm',
      title: 'Cluster Pipeline Manager',
      order: 50,
      async mount(params) {
        const { mountManagementSection } = await import('./application');
        return mountManagementSection(core, params);
      },
    });
  }

  public start(core: CoreStart) {
    // kibana_admin grants all management UI capabilities and bypasses the
    // Elasticsearch-feature cluster-privilege check — verify ES access explicitly.
    void core.http
      .get<{ allowed: boolean }>('/api/cpm/access')
      .then(({ allowed }) => {
        if (!allowed) {
          this.cpmApp?.disable();
        }
      })
      .catch(() => {
        this.cpmApp?.disable();
      });
  }

  public stop() {}
}

export function plugin() {
  return new CpmPlugin();
}
