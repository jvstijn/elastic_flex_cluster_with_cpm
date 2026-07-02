import React, { useState } from 'react';
import type { HttpSetup, NotificationsStart } from '@kbn/core/public';
import {
  EuiButton,
  EuiButtonEmpty,
  EuiModal,
  EuiModalBody,
  EuiModalFooter,
  EuiModalHeader,
  EuiModalHeaderTitle,
  EuiForm,
  EuiFormRow,
  EuiFieldText,
  EuiSelect,
  EuiTextArea,
  EuiText,
  EuiTitle,
  EuiCode,
  EuiSpacer,
  EuiCallOut,
} from '@elastic/eui';
import { streamLockDocId } from '../../common/constants';
import type { ClusterRegistryDoc, StreamLockDoc } from '../../common/types';
import type { CpmApi } from '../services/api';

interface Props {
  api: CpmApi;
  notifications: NotificationsStart;
  clusters: Array<ClusterRegistryDoc & { id: string }>;
  onComplete: () => void;
  onClose: () => void;
}

const STREAM_TYPES = [
  { value: 'logs', text: 'logs' },
  { value: 'metrics', text: 'metrics' },
  { value: 'traces', text: 'traces' },
];

const PIPELINE_TYPES = [
  { value: 'catchall', text: 'catchall' },
  { value: 'dedicated', text: 'dedicated' },
];

export function StreamLockWizard({ api, notifications, clusters, onComplete, onClose }: Props) {
  const [step, setStep] = useState(0);
  const [saving, setSaving] = useState(false);
  const [form, setForm] = useState({
    data_stream_type: 'logs',
    dataset: '',
    namespace: 'prd',
    cluster_id: '',
    pipeline_type: 'dedicated' as 'catchall' | 'dedicated',
    reason: '',
  });

  const activeClusters = clusters.filter((c) => c.active !== false);
  const lockId = streamLockDocId(form.data_stream_type, form.dataset, form.namespace);
  const canNextStep0 = form.dataset.trim().length > 0 && form.namespace.trim().length > 0;
  const canNextStep1 = form.cluster_id.length > 0;

  const steps = [
    { title: 'Stream identity', children: <StepStream form={form} setForm={setForm} lockId={lockId} /> },
    {
      title: 'Target cluster',
      children: (
        <StepTarget form={form} setForm={setForm} activeClusters={activeClusters} />
      ),
    },
    { title: 'Review', children: <StepReview form={form} lockId={lockId} /> },
  ];

  const save = async () => {
    setSaving(true);
    const doc: StreamLockDoc = {
      config_type: 'stream_lock',
      data_stream_type: form.data_stream_type,
      dataset: form.dataset.trim(),
      namespace: form.namespace.trim(),
      cluster_id: form.cluster_id,
      pipeline_type: form.pipeline_type,
      locked: true,
      ...(form.reason.trim() ? { reason: form.reason.trim() } : {}),
    };
    try {
      await api.saveLock(lockId, doc);
      notifications.toasts.addSuccess(`Stream lock ${lockId} saved`);
      onComplete();
      onClose();
    } catch (err) {
      notifications.toasts.addDanger(err instanceof Error ? err.message : 'Save failed');
    } finally {
      setSaving(false);
    }
  };

  return (
    <EuiModal onClose={onClose} maxWidth={640}>
      <EuiModalHeader>
        <EuiModalHeaderTitle>Add stream lock</EuiModalHeaderTitle>
      </EuiModalHeader>
      <EuiModalBody>
        <EuiTitle size="xs">
          <h3>
            Step {step + 1} of {steps.length}: {steps[step].title}
          </h3>
        </EuiTitle>
        <EuiSpacer size="m" />
        {steps[step].children}
      </EuiModalBody>
      <EuiModalFooter>
        <EuiButtonEmpty onClick={onClose}>Cancel</EuiButtonEmpty>
        {step > 0 && (
          <EuiButtonEmpty onClick={() => setStep((s) => s - 1)}>Back</EuiButtonEmpty>
        )}
        {step < 2 && (
          <EuiButton
            fill
            onClick={() => setStep((s) => s + 1)}
            disabled={(step === 0 && !canNextStep0) || (step === 1 && !canNextStep1)}
          >
            Next
          </EuiButton>
        )}
        {step === 2 && (
          <EuiButton fill onClick={save} isLoading={saving}>
            Create lock
          </EuiButton>
        )}
      </EuiModalFooter>
    </EuiModal>
  );
}

function StepStream({
  form,
  setForm,
  lockId,
}: {
  form: WizardForm;
  setForm: React.Dispatch<React.SetStateAction<WizardForm>>;
  lockId: string;
}) {
  return (
    <EuiForm component="div">
      <EuiFormRow label="Data stream type">
        <EuiSelect
          options={STREAM_TYPES}
          value={form.data_stream_type}
          onChange={(e) => setForm((f) => ({ ...f, data_stream_type: e.target.value }))}
        />
      </EuiFormRow>
      <EuiFormRow label="Dataset" helpText="e.g. account-gateway, beats, nginx">
        <EuiFieldText
          value={form.dataset}
          onChange={(e) => setForm((f) => ({ ...f, dataset: e.target.value }))}
          placeholder="dataset name"
        />
      </EuiFormRow>
      <EuiFormRow label="Namespace" helpText="e.g. prd, dev, raw">
        <EuiFieldText
          value={form.namespace}
          onChange={(e) => setForm((f) => ({ ...f, namespace: e.target.value }))}
        />
      </EuiFormRow>
      <EuiText size="s">
        Document ID (Kafka topic): <EuiCode>{lockId || '—'}</EuiCode>
      </EuiText>
    </EuiForm>
  );
}

function StepTarget({
  form,
  setForm,
  activeClusters,
}: {
  form: WizardForm;
  setForm: React.Dispatch<React.SetStateAction<WizardForm>>;
  activeClusters: Array<ClusterRegistryDoc & { id: string }>;
}) {
  const options = [
    { value: '', text: 'Select cluster…' },
    ...activeClusters.map((c) => ({
      value: c.cluster_id,
      text: `${c.cluster_name ?? c.cluster_id}${c.dc ? ` (${c.dc})` : ''}`,
    })),
  ];

  return (
    <EuiForm component="div">
      {activeClusters.length === 0 && (
        <>
          <EuiCallOut title="No active clusters" color="warning" iconType="alert">
            Enable at least one cluster on the Clusters tab first.
          </EuiCallOut>
          <EuiSpacer />
        </>
      )}
      <EuiFormRow label="Target cluster">
        <EuiSelect
          options={options}
          value={form.cluster_id}
          onChange={(e) => setForm((f) => ({ ...f, cluster_id: e.target.value }))}
        />
      </EuiFormRow>
      <EuiFormRow
        label="Pipeline type"
        helpText="dedicated = one stream per pipeline; catchall = shared catchall pipeline"
      >
        <EuiSelect
          options={PIPELINE_TYPES}
          value={form.pipeline_type}
          onChange={(e) =>
            setForm((f) => ({
              ...f,
              pipeline_type: e.target.value as 'catchall' | 'dedicated',
            }))
          }
        />
      </EuiFormRow>
      <EuiFormRow label="Reason (optional)">
        <EuiTextArea
          value={form.reason}
          onChange={(e) => setForm((f) => ({ ...f, reason: e.target.value }))}
          rows={2}
        />
      </EuiFormRow>
    </EuiForm>
  );
}

function StepReview({ form, lockId }: { form: WizardForm; lockId: string }) {
  return (
    <>
      <EuiText size="s">
        This creates a <strong>stream lock</strong> in <code>cpm-routing-config</code>. After saving,
        run <strong>cpm-state-manager</strong> then <strong>cpm-pipeline-manager</strong> from the Run
        CPM tab.
      </EuiText>
      <EuiSpacer />
      <EuiCode block>
        {JSON.stringify(
          {
            config_type: 'stream_lock',
            data_stream_type: form.data_stream_type,
            dataset: form.dataset,
            namespace: form.namespace,
            cluster_id: form.cluster_id,
            pipeline_type: form.pipeline_type,
            locked: true,
            ...(form.reason ? { reason: form.reason } : {}),
          },
          null,
          2
        )}
      </EuiCode>
      <EuiSpacer size="s" />
      <EuiText size="xs" color="subdued">
        ID: {lockId}
      </EuiText>
    </>
  );
}

type WizardForm = {
  data_stream_type: string;
  dataset: string;
  namespace: string;
  cluster_id: string;
  pipeline_type: 'catchall' | 'dedicated';
  reason: string;
};
