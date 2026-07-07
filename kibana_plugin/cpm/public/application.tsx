import React from 'react';
import ReactDOM from 'react-dom';
import type { CoreSetup } from '@kbn/core/public';
import type { ManagementAppMountParams } from '@kbn/management-plugin/public';
import { CpmApp } from './components/CpmApp';

/**
 * Mount via CoreStart.rendering.addContext — Kibana 8.19 RenderingService wraps
 * KibanaRenderContextProvider (theme, i18n, analytics, executionContext).
 * @see src/core/packages/rendering/browser-internal/src/rendering_service.tsx
 */
export async function mountManagementSection(
  coreSetup: CoreSetup,
  params: ManagementAppMountParams
) {
  const { element } = params;
  const [coreStart] = await coreSetup.getStartServices();

  ReactDOM.render(
    coreStart.rendering.addContext(<CpmApp core={coreStart} />),
    element
  );

  return () => ReactDOM.unmountComponentAtNode(element);
}
