import { useCallback, useEffect, useState, type FormEvent } from 'react';
import { POLLING } from '../../config/polling.js';
import { useVisibilityPoll } from '../../hooks/useVisibilityPoll.js';
import { ApiResponseError, fetchJson } from '../../utils/api.js';
import { fetchOwnerCsrfToken, isOwnerSessionResponse, isRecord, lastHopSeriesCache,
  ownerLoginCredentials, ownerLoginValidationError, publishOwnerSession,
  type OwnerDashboard, type OwnerSessionResponse } from './model.js';

export function useOwnerSession() {
  const [mqttUsername, setMqttUsername] = useState('');
  const [mqttPassword, setMqttPassword] = useState('');
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [dashboard, setDashboard] = useState<OwnerDashboard | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState('');
  const [ownerSessionKey, setOwnerSessionKey] = useState<string | null>(null);
  const [ownerSessionGeneration, setOwnerSessionGeneration] = useState(0);
  const clearOwnerSession = useCallback(() => {
    lastHopSeriesCache.clear();
    setOwnerSessionGeneration(value => value + 1);
    setDashboard(null); setOwnerSessionKey(null); setSelectedNodeId(''); setError(null);
    publishOwnerSession(null);
  }, []);
  useEffect(() => {
    const controller = new AbortController();
    fetchJson<OwnerSessionResponse>(
      '/api/owner/session',
      { cache: 'no-store', signal: controller.signal },
      { timeoutMs: POLLING.ownerTimeout, maxBytes: 2 * 1024 * 1024, validate: isOwnerSessionResponse },
    )
      .then((json) => {
        if (controller.signal.aborted) return;
        setDashboard(json.dashboard);
        setOwnerSessionKey(json.mqttUsername ?? null);
        publishOwnerSession(json.mqttUsername ?? null);
        if (json.dashboard.nodes[0]?.node_id) {
          setSelectedNodeId(json.dashboard.nodes[0].node_id);
        }
        setLoading(false);
      })
      .catch(() => {
        if (controller.signal.aborted) return;
        clearOwnerSession();
        setLoading(false);
      });
    return () => controller.abort();
  // Session bootstrap runs once. Subsequent authenticated refreshes are owned
  // by the visibility-aware poller below.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Keep the node list in sync with keys learned from MQTT after login. The
  // backend resolves the username on every session request, so a newly attached
  // observer appears without making the owner log out and back in.
  const hasDashboard = dashboard !== null;
  useVisibilityPoll(async (signal) => {
    const json = await fetchJson<OwnerSessionResponse>(
      '/api/owner/session',
      { cache: 'no-store', signal },
      { timeoutMs: POLLING.ownerTimeout, maxBytes: 2 * 1024 * 1024, validate: isOwnerSessionResponse },
    );
    if (signal.aborted) return;
    setDashboard(json.dashboard);
    setOwnerSessionKey(json.mqttUsername ?? ownerSessionKey);
    publishOwnerSession(json.mqttUsername ?? ownerSessionKey);
    setSelectedNodeId((current) => (
      json.dashboard.nodes.some((node) => node.node_id === current)
        ? current
        : (json.dashboard.nodes[0]?.node_id ?? '')
    ));
  }, {
    enabled: hasDashboard,
    scopeKey: `owner-session:${ownerSessionKey ?? 'pending'}`,
    intervalMs: POLLING.ownerSession,
    timeoutMs: POLLING.ownerTimeout,
    onError: (pollError) => {
      if (pollError instanceof ApiResponseError && pollError.status === 401) {
        clearOwnerSession();
      }
    },
  });

  const handleLogin = (event: FormEvent) => {
    event.preventDefault();
    const validationError = ownerLoginValidationError(mqttUsername, mqttPassword);
    if (validationError) {
      setError(validationError);
      return;
    }
    setSubmitting(true);
    setError(null);
    const credentials = ownerLoginCredentials(mqttUsername, mqttPassword);
    fetchOwnerCsrfToken()
      .then((csrfToken) => fetchJson<OwnerSessionResponse>('/api/owner/login', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-CSRF-Token': csrfToken,
          },
          body: JSON.stringify(credentials),
        }, {
          timeoutMs: POLLING.ownerHistoryTimeout,
          maxBytes: 2 * 1024 * 1024,
          validate: isOwnerSessionResponse,
          readErrorMessage: true,
        }),
      )
      .then((json) => {
        lastHopSeriesCache.clear();
        setOwnerSessionGeneration(value => value + 1);
        setDashboard(json.dashboard);
        setOwnerSessionKey(json.mqttUsername ?? credentials.mqttUsername);
        publishOwnerSession(json.mqttUsername ?? credentials.mqttUsername);
        if (json.dashboard.nodes[0]?.node_id) {
          setSelectedNodeId(json.dashboard.nodes[0].node_id);
        }
        setMqttUsername('');
        setMqttPassword('');
      })
      .catch((err: Error) => {
        setError(err.message);
      })
      .finally(() => setSubmitting(false));
  };

  const handleLogout = () => {
    fetchOwnerCsrfToken()
      .then((csrfToken) => fetchJson<{ ok: boolean }>('/api/owner/logout', {
          method: 'POST',
          headers: { 'X-CSRF-Token': csrfToken },
        }, {
          timeoutMs: POLLING.ownerTimeout,
          maxBytes: 8 * 1024,
          validate: (value): value is { ok: boolean } => isRecord(value) && value['ok'] === true,
        }),
      )
      .finally(() => {
        clearOwnerSession();
      });
  };

  return { mqttUsername, setMqttUsername, mqttPassword, setMqttPassword, loading,
    submitting, error, dashboard, selectedNodeId, setSelectedNodeId, ownerSessionKey, ownerSessionGeneration,
    clearOwnerSession, handleLogin, handleLogout };
}
