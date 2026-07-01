import React, { useCallback, useEffect, useState } from 'react';
import type { NotificationsStart } from '@kbn/core/public';
import {
  EuiButton,
  EuiCallOut,
  EuiFieldNumber,
  EuiForm,
  EuiFormRow,
  EuiLoadingSpinner,
  EuiRange,
  EuiSpacer,
  EuiText,
  EuiTitle,
} from '@elastic/eui';
import { WEIGHT_KEYS, type WeightKey } from '../../common/constants';
import type { ScoringWeightsConfig } from '../../common/types';
import type { CpmApi } from '../services/api';

const WEIGHT_LABELS: Record<WeightKey, string> = {
  disk: 'Disk usage forecast',
  jvm: 'JVM heap forecast',
  shard: 'Shard count forecast',
  load: 'Write queue / load forecast',
};

interface Props {
  api: CpmApi;
  notifications: NotificationsStart;
}

const DEFAULT_WEIGHTS: ScoringWeightsConfig = {
  config_type: 'scoring_weights',
  weights: { disk: 0.5, jvm: 0.25, shard: 0.05, load: 0.2 },
  alert_threshold: 80,
  forecast_horizon_hours: 24,
};

export function ScoringWeightsTab({ api, notifications }: Props) {
  const [config, setConfig] = useState<ScoringWeightsConfig>(DEFAULT_WEIGHTS);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const doc = await api.getScoring();
      setConfig({ ...DEFAULT_WEIGHTS, ...doc });
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, [api]);

  useEffect(() => {
    load();
  }, [load]);

  const weightSum = WEIGHT_KEYS.reduce((sum, k) => sum + (config.weights[k] ?? 0), 0);

  const save = async () => {
    if (Math.abs(weightSum - 1) > 0.01) {
      notifications.toasts.addWarning('Weights should sum to 1.0 (current: ' + weightSum.toFixed(2) + ')');
    }
    setSaving(true);
    try {
      await api.saveScoring(config);
      notifications.toasts.addSuccess('Scoring weights saved to cpm-routing-config/_global');
    } catch (err) {
      notifications.toasts.addDanger(err instanceof Error ? err.message : 'Save failed');
    } finally {
      setSaving(false);
    }
  };

  if (loading) {
    return <EuiLoadingSpinner size="xl" />;
  }

  return (
    <>
      {error && (
        <>
          <EuiCallOut title="Could not load scoring config" color="danger" iconType="alert">
            <p>{error}</p>
          </EuiCallOut>
          <EuiSpacer />
        </>
      )}

      <EuiText size="s" color="subdued">
        Document <code>cpm-routing-config/_global</code> — used by <code>cpm-scoring</code> watcher.
      </EuiText>
      <EuiSpacer size="l" />

      <EuiForm component="div">
        <EuiTitle size="xs">
          <h3>Forecast weights (sum: {weightSum.toFixed(2)})</h3>
        </EuiTitle>
        <EuiSpacer size="m" />

        {WEIGHT_KEYS.map((key) => (
          <EuiFormRow key={key} label={WEIGHT_LABELS[key]} fullWidth>
            <EuiRange
              min={0}
              max={1}
              step={0.05}
              value={config.weights[key] ?? 0}
              onChange={(e) =>
                setConfig((prev) => ({
                  ...prev,
                  weights: { ...prev.weights, [key]: Number(e.currentTarget.value) },
                }))
              }
              showInput
              fullWidth
            />
          </EuiFormRow>
        ))}

        <EuiSpacer size="l" />
        <EuiFormRow label="Alert threshold (%)" helpText="Clusters scoring above this trigger alerts">
          <EuiFieldNumber
            value={config.alert_threshold}
            min={0}
            max={100}
            onChange={(e) =>
              setConfig((prev) => ({ ...prev, alert_threshold: Number(e.target.value) }))
            }
          />
        </EuiFormRow>

        <EuiFormRow label="Forecast horizon (hours)" helpText="ML forecast window used by scoring">
          <EuiFieldNumber
            value={config.forecast_horizon_hours}
            min={1}
            max={168}
            onChange={(e) =>
              setConfig((prev) => ({
                ...prev,
                forecast_horizon_hours: Number(e.target.value),
              }))
            }
          />
        </EuiFormRow>

        <EuiSpacer size="m" />
        <EuiButton fill onClick={save} isLoading={saving}>
          Save weights
        </EuiButton>
      </EuiForm>
    </>
  );
}
