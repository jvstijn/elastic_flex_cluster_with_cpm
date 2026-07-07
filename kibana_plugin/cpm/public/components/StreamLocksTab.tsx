import React, { useCallback, useEffect, useState } from 'react';
import type { NotificationsStart } from '@kbn/core/public';
import {
  EuiBasicTable,
  EuiButton,
  EuiButtonIcon,
  EuiCallOut,
  EuiConfirmModal,
  EuiLoadingSpinner,
  EuiSpacer,
  EuiText,
} from '@elastic/eui';
import type { ClusterRegistryDoc, StreamLockDoc } from '../../common/types';
import type { CpmApi } from '../services/api';
import { StreamLockWizard } from './StreamLockWizard';

interface LockRow extends StreamLockDoc {
  id: string;
}

interface Props {
  api: CpmApi;
  notifications: NotificationsStart;
}

export function StreamLocksTab({ api, notifications }: Props) {
  const [locks, setLocks] = useState<LockRow[]>([]);
  const [clusters, setClusters] = useState<Array<ClusterRegistryDoc & { id: string }>>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showWizard, setShowWizard] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<LockRow | null>(null);
  const [deleting, setDeleting] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [lockRows, clusterRows] = await Promise.all([api.getLocks(), api.getClusters()]);
      setLocks(lockRows as LockRow[]);
      setClusters(clusterRows as Array<ClusterRegistryDoc & { id: string }>);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, [api]);

  useEffect(() => {
    load();
  }, [load]);

  const confirmDelete = async () => {
    if (!deleteTarget) return;
    setDeleting(true);
    try {
      await api.deleteLock(deleteTarget.id);
      notifications.toasts.addSuccess(`Deleted lock ${deleteTarget.id}`);
      setDeleteTarget(null);
      await load();
    } catch (err) {
      notifications.toasts.addDanger(err instanceof Error ? err.message : 'Delete failed');
    } finally {
      setDeleting(false);
    }
  };

  const runAfterLockChange = async () => {
    try {
      const res = await api.runWatchers({
        watchers: ['cpm-state-manager', 'cpm-pipeline-manager'],
      });
      const failed = res.results.filter((r) => !r.ok);
      if (failed.length) {
        notifications.toasts.addWarning(
          `Watchers completed with errors: ${failed.map((f) => f.id).join(', ')}`
        );
      } else {
        notifications.toasts.addSuccess('State manager and pipeline manager executed');
      }
    } catch (err) {
      notifications.toasts.addDanger(err instanceof Error ? err.message : 'Watcher run failed');
    }
  };

  const columns = [
    { field: 'id', name: 'Topic / doc ID' },
    { field: 'data_stream_type', name: 'Type' },
    { field: 'dataset', name: 'Dataset' },
    { field: 'namespace', name: 'Namespace' },
    { field: 'cluster_id', name: 'Cluster' },
    { field: 'pipeline_type', name: 'Pipeline' },
    {
      field: 'reason',
      name: 'Reason',
      render: (reason: string | undefined) => reason ?? '—',
    },
    {
      name: 'Actions',
      width: '60px',
      render: (row: LockRow) => (
        <EuiButtonIcon
          iconType="trash"
          color="danger"
          aria-label={`Delete lock ${row.id}`}
          onClick={() => setDeleteTarget(row)}
        />
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
          <EuiCallOut title="Could not load stream locks" color="danger" iconType="alert">
            <p>{error}</p>
          </EuiCallOut>
          <EuiSpacer />
        </>
      )}

      <EuiText size="s" color="subdued">
        Stream locks pin a data stream to a cluster (<code>cpm-routing-config</code>,{' '}
        <code>config_type: stream_lock</code>). Document ID = topic name, e.g.{' '}
        <code>logs-account-gateway-prd</code>.
      </EuiText>
      <EuiSpacer size="m" />

      <EuiButton fill iconType="plusInCircle" onClick={() => setShowWizard(true)}>
        Add stream lock
      </EuiButton>
      {' '}
      <EuiButton iconType="play" onClick={runAfterLockChange}>
        Apply locks (state + pipeline managers)
      </EuiButton>
      {' '}
      <EuiButton iconType="refresh" onClick={load} size="s">
        Refresh
      </EuiButton>

      <EuiSpacer size="m" />
      <EuiBasicTable items={locks} columns={columns} tableLayout="auto" rowHeader="id" />

      {showWizard && (
        <StreamLockWizard
          api={api}
          notifications={notifications}
          clusters={clusters}
          onComplete={load}
          onClose={() => setShowWizard(false)}
        />
      )}

      {deleteTarget && (
        <EuiConfirmModal
          title={`Delete stream lock ${deleteTarget.id}?`}
          onCancel={() => setDeleteTarget(null)}
          onConfirm={confirmDelete}
          cancelButtonText="Cancel"
          confirmButtonText="Delete"
          buttonColor="danger"
          isLoading={deleting}
        >
          <p>
            The stream will be managed by CPM routing again. Run state-manager and pipeline-manager
            after deleting.
          </p>
        </EuiConfirmModal>
      )}
    </>
  );
}
