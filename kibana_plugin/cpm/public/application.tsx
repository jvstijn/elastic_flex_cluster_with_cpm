import React from 'react';
import ReactDOM from 'react-dom';
import type { AppMountParameters, CoreStart } from '@kbn/core/public';
import { CpmApp } from './components/CpmApp';

export function renderManagementApp(coreStart: CoreStart, { element }: AppMountParameters) {
  ReactDOM.render(coreStart.rendering.addContext(<CpmApp core={coreStart} />), element);

  return () => ReactDOM.unmountComponentAtNode(element);
}
