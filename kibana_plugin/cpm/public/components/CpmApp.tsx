import React, { useMemo, useState } from 'react';
import type { CoreStart } from '@kbn/core/public';
import { EuiSpacer, EuiTab, EuiTabs, EuiText, EuiTitle } from '@elastic/eui';
import { createCpmApi } from '../services/api';
import { ClusterRegistryTab } from './ClusterRegistryTab';
import { ScoringWeightsTab } from './ScoringWeightsTab';
import { StreamLocksTab } from './StreamLocksTab';
import { RunCpmTab } from './RunCpmTab';

interface Props {
  core: CoreStart;
}

const TABS = [
  { id: 'clusters', name: 'Clusters' },
  { id: 'scoring', name: 'Scoring weights' },
  { id: 'locks', name: 'Stream locks' },
  { id: 'run', name: 'Run CPM' },
] as const;

type TabId = (typeof TABS)[number]['id'];

export function CpmApp({ core }: Props) {
  const [activeTab, setActiveTab] = useState<TabId>('clusters');
  const api = useMemo(() => createCpmApi(core.http), [core.http]);

  return (
    <>
      <EuiTitle size="l">
        <h1>Cluster Pipeline Manager</h1>
      </EuiTitle>
      <EuiText size="s" color="subdued">
        Manage cluster registry, scoring weights, stream locks, and watcher execution.
      </EuiText>
      <EuiSpacer size="m" />

      <EuiTabs>
        {TABS.map((tab) => (
          <EuiTab
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            isSelected={activeTab === tab.id}
          >
            {tab.name}
          </EuiTab>
        ))}
      </EuiTabs>

      <EuiSpacer size="m" />

      {activeTab === 'clusters' && (
        <ClusterRegistryTab api={api} notifications={core.notifications} />
      )}
      {activeTab === 'scoring' && (
        <ScoringWeightsTab api={api} notifications={core.notifications} />
      )}
      {activeTab === 'locks' && <StreamLocksTab api={api} notifications={core.notifications} />}
      {activeTab === 'run' && <RunCpmTab api={api} notifications={core.notifications} />}
    </>
  );
}
