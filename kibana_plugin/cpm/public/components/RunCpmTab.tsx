import React, { useMemo, useState } from 'react';
import type { NotificationsStart } from '@kbn/core/public';
import {
  EuiButton,
  EuiCallOut,
  EuiCode,
  EuiFlexGroup,
  EuiFlexItem,
  EuiIcon,
  EuiModal,
  EuiModalBody,
  EuiModalFooter,
  EuiModalHeader,
  EuiModalHeaderTitle,
  EuiSpacer,
  EuiText,
  EuiHealth,
} from '@elastic/eui';
import { WATCHER_CHAIN, WATCHER_FORECAST } from '../../common/constants';
import type { WatcherRunResult } from '../../common/types';
import type { CpmApi } from '../services/api';

interface Props {
  api: CpmApi;
  notifications: NotificationsStart;
}

type StepStatus = 'pending' | 'running' | 'success' | 'error';

const FULL_CHAIN = [WATCHER_FORECAST, ...WATCHER_CHAIN] as const;

const STEP_META: Record<string, { label: string; description: string }> = {
  [WATCHER_FORECAST]: {
    label: 'Forecast',
    description: 'Refresh ML capacity forecasts',
  },
  'cpm-registry-sync': {
    label: 'Registry',
    description: 'Sync clusters from stack monitoring',
  },
  'cpm-scoring': {
    label: 'Scoring',
    description: 'Score clusters using forecasts and weights',
  },
  'cpm-routing-advisor': {
    label: 'Routing',
    description: 'Compute routing suggestions',
  },
  'cpm-state-manager': {
    label: 'State',
    description: 'Build desired pipeline state (respects locks)',
  },
  'cpm-pipeline-manager': {
    label: 'Pipelines',
    description: 'Push Logstash pipelines to remote clusters',
  },
};

const BALL_COLORS: Record<StepStatus, string> = {
  pending: '#d3dae6',
  running: '#d3dae6',
  success: '#0b64dd',
  error: '#bd271e',
};

function PhaseBall({ status }: { status: StepStatus }) {
  return (
    <div
      style={{
        width: 28,
        height: 28,
        borderRadius: '50%',
        backgroundColor: BALL_COLORS[status],
        border: status === 'running' ? '3px solid #0b64dd' : '2px solid transparent',
        boxSizing: 'border-box',
        margin: '0 auto',
      }}
      aria-hidden
    />
  );
}

export function RunCpmTab({ api, notifications }: Props) {
  const [running, setRunning] = useState(false);
  const [stepStatus, setStepStatus] = useState<Record<string, StepStatus>>({});
  const [results, setResults] = useState<WatcherRunResult[] | null>(null);
  const [showResults, setShowResults] = useState(false);

  const initialStatus = useMemo(
    () => Object.fromEntries(FULL_CHAIN.map((id) => [id, 'pending' as StepStatus])),
    []
  );

  const resetChain = () => {
    setStepStatus({ ...initialStatus });
    setResults(null);
  };

  const runFullChain = async () => {
    setRunning(true);
    resetChain();
    const collected: WatcherRunResult[] = [];

    for (const watcherId of FULL_CHAIN) {
      setStepStatus((prev) => ({ ...prev, [watcherId]: 'running' }));
      try {
        const res = await api.runWatchers({ watchers: [watcherId] });
        const result = res.results[0] ?? { id: watcherId, ok: false, error: 'No result' };
        collected.push(result);
        setStepStatus((prev) => ({
          ...prev,
          [watcherId]: result.ok ? 'success' : 'error',
        }));
      } catch (err) {
        const message = err instanceof Error ? err.message : 'Execution failed';
        collected.push({ id: watcherId, ok: false, error: message });
        setStepStatus((prev) => ({ ...prev, [watcherId]: 'error' }));
      }
    }

    setResults(collected);
    setShowResults(true);
    setRunning(false);

    const failed = collected.filter((r) => !r.ok);
    if (failed.length === 0) {
      notifications.toasts.addSuccess('CPM watcher chain completed');
    } else {
      notifications.toasts.addWarning(
        `${failed.length} watcher(s) failed: ${failed.map((f) => f.id).join(', ')}`
      );
    }
  };

  const runSingle = async (watcherId: string) => {
    setRunning(true);
    setStepStatus({ ...initialStatus, [watcherId]: 'running' });
    try {
      const res = await api.runWatchers({ watchers: [watcherId] });
      const result = res.results[0];
      setResults(res.results);
      setStepStatus((prev) => ({
        ...prev,
        [watcherId]: result?.ok ? 'success' : 'error',
      }));
      setShowResults(true);
      if (result?.ok) {
        notifications.toasts.addSuccess(`${watcherId} executed`);
      } else {
        notifications.toasts.addDanger(result?.error ?? `${watcherId} failed`);
      }
    } catch (err) {
      notifications.toasts.addDanger(err instanceof Error ? err.message : 'Execution failed');
      setStepStatus((prev) => ({ ...prev, [watcherId]: 'error' }));
    } finally {
      setRunning(false);
    }
  };

  const statusFor = (id: string): StepStatus => stepStatus[id] ?? 'pending';

  return (
    <>
      <EuiText size="s" color="subdued">
        Run the full CPM chain (includes <EuiCode>cpm-forecast-trigger</EuiCode> automatically).
        Each step uses <EuiCode>record_execution: true</EuiCode> for the Platform Overview dashboard.
      </EuiText>
      <EuiSpacer size="l" />

      <EuiFlexGroup alignItems="flexStart" justifyContent="center" responsive={false} wrap>
        {FULL_CHAIN.map((watcherId, index) => (
          <React.Fragment key={watcherId}>
            <EuiFlexItem grow={false} style={{ minWidth: 88, textAlign: 'center' }}>
              <PhaseBall status={statusFor(watcherId)} />
              <EuiSpacer size="s" />
              <EuiText size="xs">
                <strong>{STEP_META[watcherId]?.label ?? watcherId}</strong>
              </EuiText>
              <EuiSpacer size="xs" />
              <EuiButton size="s" onClick={() => runSingle(watcherId)} disabled={running}>
                Run
              </EuiButton>
            </EuiFlexItem>
            {index < FULL_CHAIN.length - 1 && (
              <EuiFlexItem grow={false} style={{ paddingTop: 6 }}>
                <EuiIcon type="arrowRight" color="subdued" />
              </EuiFlexItem>
            )}
          </React.Fragment>
        ))}
      </EuiFlexGroup>

      <EuiSpacer size="l" />

      <EuiButton fill iconType="play" onClick={runFullChain} isLoading={running}>
        Run full CPM chain
      </EuiButton>
      {results && results.length > 0 && (
        <>
          {' '}
          <EuiButton iconType="list" onClick={() => setShowResults(true)} disabled={running}>
            Last run results
          </EuiButton>
        </>
      )}

      <EuiSpacer size="l" />
      <EuiCallOut title="After changing clusters or locks" size="s">
        Run at minimum <EuiCode>cpm-state-manager</EuiCode> then{' '}
        <EuiCode>cpm-pipeline-manager</EuiCode>. After registry or weight changes, run the full
        chain from <EuiCode>cpm-registry-sync</EuiCode> or <EuiCode>cpm-scoring</EuiCode> onward.
      </EuiCallOut>

      {showResults && results && (
        <EuiModal onClose={() => setShowResults(false)} maxWidth={640}>
          <EuiModalHeader>
            <EuiModalHeaderTitle>Last run results</EuiModalHeaderTitle>
          </EuiModalHeader>
          <EuiModalBody>
            {results.map((r) => (
              <div key={r.id}>
                <EuiHealth color={r.ok ? 'success' : 'danger'}>
                  <EuiCode>{r.id}</EuiCode>
                  {r.ok ? ' — OK' : ` — ${r.error}`}
                </EuiHealth>
                <EuiSpacer size="s" />
              </div>
            ))}
          </EuiModalBody>
          <EuiModalFooter>
            <EuiButton onClick={() => setShowResults(false)} fill>
              Close
            </EuiButton>
          </EuiModalFooter>
        </EuiModal>
      )}
    </>
  );
}
