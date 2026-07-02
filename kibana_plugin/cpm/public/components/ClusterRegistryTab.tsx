import React, { useCallback, useEffect, useState } from 'react';
import type { NotificationsStart } from '@kbn/core/public';
import {
  EuiBasicTable,
  EuiButton,
  EuiCallOut,
  EuiHealth,
  EuiLoadingSpinner,
  EuiSpacer,
  EuiSwitch,
  EuiText,
} from '@elastic/eui';
import type { CpmApi } from '../services/api';
import type { ClusterRegistryDoc } from '../../common/types';

interface ClusterRow extends ClusterRegistryDoc {
  id: string;
}

interface Props {
  api: CpmApi;
  notifications: NotificationsStart;
}

export function ClusterRegistryTab({ api, notifications }: Props) {
  const [clusters, setClusters] = useState<ClusterRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [savingId, setSavingId] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const rows = await api.getClusters();
      setClusters(rows as ClusterRow[]);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, [api]);

  useEffect(() => {
    load();
  }, [load]);

  const toggleActive = async (row: ClusterRow, active: boolean) => {
    setSavingId(row.id);
    try {
      await api.setClusterActive(row.id, active);
      setClusters((prev) =>
        prev.map((c) => (c.id === row.id ? { ...c, active } : c))
      );
      notifications.toasts.addSuccess(
        `${row.cluster_name ?? row.cluster_id} ${active ? 'enabled' : 'disabled'}`
      );
    } catch (err) {
      notifications.toasts.addDanger(
        err instanceof Error ? err.message : 'Failed to update cluster'
      );
    } finally {
      setSavingId(null);
    }
  };

  const columns = [
    {
      field: 'cluster_name',
      name: 'Cluster',
      render: (name: string, row: ClusterRow) => name || row.cluster_id,
    },
    {
      field: 'cluster_id',
      name: 'ID',
    },
    {
      field: 'dc',
      name: 'DC',
    },
    {
      field: 'node_count',
      name: 'Nodes',
    },
    {
      field: 'active',
      name: 'Enabled',
      render: (active: boolean, row: ClusterRow) => (
        <EuiSwitch
          showLabel={false}
          checked={active !== false}
          disabled={savingId === row.id}
          onChange={(e) => toggleActive(row, e.target.checked)}
          aria-label={`Toggle ${row.cluster_name ?? row.cluster_id}`}
        />
      ),
    },
    {
      field: 'active',
      name: 'Status',
      render: (active: boolean) => (
        <EuiHealth color={active !== false ? 'success' : 'default'}>
          {active !== false ? 'Active' : 'Disabled'}
        </EuiHealth>
      ),
    },
  ];

  if (loading) {
    return <EuiLoadingSpinner size="xl" />;
  }

  return (
    <>
      {error && (
        <>
          <EuiCallOut title="Could not load cluster registry" color="danger" iconType="alert">
            <p>{error}</p>
          </EuiCallOut>
          <EuiSpacer />
        </>
      )}

      <EuiText size="s" color="subdued">
        Clusters discovered by <code>cpm-registry-sync</code> from stack monitoring. Disabled
        clusters are excluded from scoring and routing.
      </EuiText>
      <EuiSpacer size="m" />

      <EuiButton iconType="refresh" onClick={load} size="s">
        Refresh
      </EuiButton>
      <EuiSpacer size="m" />

      <EuiBasicTable items={clusters} columns={columns} tableLayout="auto" />
    </>
  );
}
