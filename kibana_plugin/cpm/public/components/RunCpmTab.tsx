import React, { useState } from 'react';
import type { NotificationsStart } from '@kbn/core/public';
import {
  EuiButton,
  EuiCheckbox,
  EuiCode,
  EuiFlexGroup,
  EuiFlexItem,
  EuiPanel,
  EuiSpacer,
  EuiSteps,
  EuiText,
  EuiTitle,
  EuiHealth,
  EuiCallOut,
} from '@elastic/eui';
import { WATCHER_CHAIN, WATCHER_FORECAST } from '../../common/constants';
import type { WatcherRunResult } from '../../common/types';
import type { CpmApi } from '../services/api';

interface Props {
  api: CpmApi;
  notifications: NotificationsStart;
}

const CHAIN_STEPS = [
  {
    id: WATCHER_FORECAST,
    title: 'cpm-forecast-trigger',
    optional: true,
    description: 'Hourly ML forecast refresh (optional before scoring)',
  },
  ...WATCHER_CHAIN.map((id, i) => ({
    id,
    title: id,
    optional: false,
    description: [
      'Sync clusters from stack monitoring → cpm-cluster-registry',
      'Score clusters using ML forecasts and weights',
      'Compute routing suggestions from scores and index rates',
      'Build desired pipeline state (respects stream locks)',
      'Push Logstash pipelines to remote clusters',
    ][i],
  })),
];

export function RunCpmTab({ api, notifications }: Props) {
  const [includeForecast, setIncludeForecast] = useState(false);
  const [running, setRunning] = useState(false);
  const [results, setResults] = useState<WatcherRunResult[] | null>(null);

  const runFullChain = async () => {
    setRunning(true);
    setResults(null);
    try {
      const res = await api.runWatchers({ includeForecast });
      setResults(res.results);
      const failed = res.results.filter((r) => !r.ok);
      if (failed.length === 0) {
        notifications.toasts.addSuccess('CPM watcher chain completed');
      } else {
        notifications.toasts.addWarning(
          `${failed.length} watcher(s) failed: ${failed.map((f) => f.id).join(', ')}`
        );
      }
    } catch (err) {
      notifications.toasts.addDanger(err instanceof Error ? err.message : 'Chain failed');
    } finally {
      setRunning(false);
    }
  };

  const runSingle = async (watcherId: string) => {
    setRunning(true);
    try {
      const res = await api.runWatchers({ watchers: [watcherId] });
      setResults(res.results);
      const r = res.results[0];
      if (r?.ok) {
        notifications.toasts.addSuccess(`${watcherId} executed`);
      } else {
        notifications.toasts.addDanger(r?.error ?? `${watcherId} failed`);
      }
    } catch (err) {
      notifications.toasts.addDanger(err instanceof Error ? err.message : 'Execution failed');
    } finally {
      setRunning(false);
    }
  };

  return (
    <>
      <EuiText size="s" color="subdued">
        Manually execute the CPM watcher chain. Each call uses{' '}
        <EuiCode>record_execution: true</EuiCode> so runs appear on the Platform Overview dashboard.
      </EuiText>
      <EuiSpacer size="l" />

      <EuiPanel paddingSize="m">
        <EuiTitle size="s">
          <h3>Full chain</h3>
        </EuiTitle>
        <EuiSpacer size="s" />
        <EuiCheckbox
          id="include-forecast"
          label="Include cpm-forecast-trigger before scoring (if ML forecasts are stale)"
          checked={includeForecast}
          onChange={(e) => setIncludeForecast(e.target.checked)}
        />
        <EuiSpacer size="m" />
        <EuiButton fill iconType="play" onClick={runFullChain} isLoading={running}>
          Run full CPM chain
        </EuiButton>
      </EuiPanel>

      <EuiSpacer size="l" />

      <EuiTitle size="s">
        <h3>Watcher chain</h3>
      </EuiTitle>
      <EuiSpacer size="m" />

      <EuiSteps
        steps={CHAIN_STEPS.map((step) => ({
          title: step.title,
          children: (
            <>
              <EuiText size="s">{step.description}</EuiText>
              {step.optional && (
                <EuiText size="xs" color="subdued">
                  Optional
                </EuiText>
              )}
              <EuiSpacer size="s" />
              <EuiButton size="s" onClick={() => runSingle(step.id)} disabled={running}>
                Run only this watcher
              </EuiButton>
            </>
          ),
        }))}
      />

      {results && results.length > 0 && (
        <>
          <EuiSpacer size="l" />
          <EuiTitle size="s">
            <h3>Last run results</h3>
          </EuiTitle>
          <EuiSpacer size="m" />
          <EuiFlexGroup direction="column" gutterSize="s">
            {results.map((r) => (
              <EuiFlexItem key={r.id}>
                <EuiHealth color={r.ok ? 'success' : 'danger'}>
                  <EuiCode>{r.id}</EuiCode>
                  {r.ok ? ' — OK' : ` — ${r.error}`}
                </EuiHealth>
              </EuiFlexItem>
            ))}
          </EuiFlexGroup>
        </>
      )}

      <EuiSpacer size="l" />
      <EuiCallOut title="After changing clusters or locks" size="s">
        Run at minimum <EuiCode>cpm-state-manager</EuiCode> then{' '}
        <EuiCode>cpm-pipeline-manager</EuiCode>. After registry or weight changes, run the full chain
        from <EuiCode>cpm-registry-sync</EuiCode> or <EuiCode>cpm-scoring</EuiCode> onward.
      </EuiCallOut>
    </>
  );
}
