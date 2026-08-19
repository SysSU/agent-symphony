"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

import { OrchestratorCard, StatusCard } from "./_components/status-card";
import ReconcileButton from "./_components/reconcile-button";
import { postWithReconciliationRetry } from "./actions.mjs";
import TerminalPanel from "./_components/terminal-panel";
import { overallHealth } from "./health.mjs";

const refreshEvery = 5000;

function relativeTime(value, now) {
  const seconds = Math.max(0, Math.floor((now - new Date(value).getTime()) / 1000));
  if (seconds < 5) return "just now";
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  return `${Math.floor(minutes / 60)}h ago`;
}

function attemptKey(status) {
  return `${status.repository}#${status.issue}/${status.attempt}`;
}

export default function Dashboard() {
  const [snapshot, setSnapshot] = useState(null);
  const [dashboardState, setDashboardState] = useState({ hidden: [] });
  const [error, setError] = useState("");
  const [actionNotice, setActionNotice] = useState("");
  const [busy, setBusy] = useState("");
  const [waiting, setWaiting] = useState(false);
  const [now, setNow] = useState(() => Date.now());
  const [tab, setTab] = useState("current");
  const [terminal, setTerminal] = useState(null);
  const [orchestratorStatus, setOrchestratorStatus] = useState(null);
  const [orchestratorError, setOrchestratorError] = useState("");
  const [orchestratorBusy, setOrchestratorBusy] = useState("");
  const [investigating, setInvestigating] = useState("");
  const [messageProposal, setMessageProposal] = useState(null);
  const [messageError, setMessageError] = useState("");
  const [messageBusy, setMessageBusy] = useState("");
  const closeTerminal = useCallback(() => setTerminal(null), []);

  useEffect(() => {
    let active = true;
    async function refresh() {
      try {
        const orchestratorRequest = fetch("/orchestrator.json", { cache: "no-store" })
          .then(async (response) => response.ok
            ? { status: await response.json(), error: "" }
            : { status: null, error: (await response.text()).trim() || `Orchestrator status failed (${response.status})` })
          .catch(() => ({ status: null, error: "Orchestrator status is unavailable" }));
        const proposalRequest = fetch("/orchestrator/proposal.json", { cache: "no-store" })
          .then(async (response) => response.status === 204
            ? { proposal: null, error: "" }
            : response.ok
              ? { proposal: await response.json(), error: "" }
              : { proposal: null, error: (await response.text()).trim() || `Message proposal failed (${response.status})` })
          .catch(() => ({ proposal: null, error: "Message proposal is unavailable" }));
        const [response, stateResponse, orchestratorResult, proposalResult] = await Promise.all([
          fetch("/status.json", { cache: "no-store" }),
          fetch("/dashboard-state.json", { cache: "no-store" }),
          orchestratorRequest,
          proposalRequest,
        ]);
        if (!response.ok) throw new Error(response.status === 404 ? "Waiting for the first reconciliation" : `Status request failed (${response.status})`);
        if (!stateResponse.ok) throw new Error(`Dashboard state request failed (${stateResponse.status})`);
        const [next, nextState] = await Promise.all([response.json(), stateResponse.json()]);
        if (active) {
          setSnapshot(next);
          setDashboardState(nextState);
          setOrchestratorStatus(orchestratorResult.status);
          setOrchestratorError(orchestratorResult.error);
          setMessageProposal(proposalResult.proposal);
          setMessageError(proposalResult.error);
          setError("");
          setNow(Date.now());
        }
      } catch (reason) {
        if (active) setError(reason instanceof Error ? reason.message : "Status request failed");
      }
    }
    refresh();
    const timer = setInterval(refresh, refreshEvery);
    return () => {
      active = false;
      clearInterval(timer);
    };
  }, []);

  const performAction = useCallback(async (action, status) => {
    const verb = action === "archive" ? "Archive" : action === "recover" ? "Recover" : "Abandon";
    const consequence = action === "archive"
      ? "This stops its tmux session if needed, deletes its local worktree, and hides it from Completed."
      : action === "recover"
        ? "If the attempt is stuck, this stops only its named tmux session. It preserves the worktree and diagnostics, records the failure on GitHub, and requests a new attempt."
      : "This stops its tmux session and permanently deletes its local worktree, log, and retained attempt record.";
    if (!window.confirm(`${verb} issue #${status.issue}, attempt ${status.attempt}?\n\n${consequence}`)) return;
    const key = attemptKey(status);
    setBusy(key);
    setWaiting(false);
    setActionNotice("");
    try {
      const query = new URLSearchParams({ issue: String(status.issue), attempt: String(status.attempt) });
      const response = await postWithReconciliationRetry(`/actions/${action}?${query}`, async () => {
        setWaiting(true);
        await new Promise((resolve) => setTimeout(resolve, 1000));
      });
      if (!response.ok) throw new Error((await response.text()).trim() || `${verb} failed (${response.status})`);
      const stateResponse = await fetch("/dashboard-state.json", { cache: "no-store" });
      if (!stateResponse.ok) throw new Error(`${verb} finished, but dashboard state could not be refreshed.`);
      setDashboardState(await stateResponse.json());
      const finished = action === "archive" ? "Archived" : action === "recover" ? "Recovery requested for" : "Abandoned";
      setActionNotice(`${finished} issue #${status.issue}, attempt ${status.attempt}.`);
    } catch (reason) {
      setActionNotice(reason instanceof Error ? reason.message : `${verb} failed.`);
    } finally {
      setBusy("");
      setWaiting(false);
    }
  }, []);

  const performOrchestratorAction = useCallback(async (action, status) => {
    const confirmations = {
      recover: "Recover or restart the orchestrator?\n\nThis ensures the supervised tmux session is running. The current context is kept when it can be adopted safely.",
      clear: "Clear orchestrator context?\n\nThis discards the current conversation and relaunches with role and safety rules only.",
      rebuild: "Rebuild orchestrator context?\n\nThis discards the current conversation and relaunches from a fresh authoritative projection.",
    };
    if (confirmations[action] && !window.confirm(confirmations[action])) return;
    const key = status ? attemptKey(status) : action;
    setOrchestratorBusy(action);
    if (action === "investigate") setInvestigating(key);
    setActionNotice("");
    try {
      const query = status ? `?${new URLSearchParams({ issue: String(status.issue), attempt: String(status.attempt) })}` : "";
      const response = await postWithReconciliationRetry(`/actions/orchestrator/${action}${query}`);
      if (!response.ok) throw new Error((await response.text()).trim() || `Orchestrator ${action} failed (${response.status})`);
      const result = await response.json();
      if (result.status) {
        setOrchestratorStatus(result.status);
        setOrchestratorError("");
      }
      setActionNotice(action === "investigate"
        ? `Asked the orchestrator to investigate issue #${status.issue}, attempt ${status.attempt}.`
        : `Orchestrator ${action} requested.`);
    } catch (reason) {
      setActionNotice(reason instanceof Error ? reason.message : `Orchestrator ${action} failed.`);
    } finally {
      setOrchestratorBusy("");
      setInvestigating("");
    }
  }, []);

  const performMessageAction = useCallback(async (action) => {
    if (!messageProposal) return;
    setMessageBusy(action);
    setActionNotice("");
    try {
      const response = await postWithReconciliationRetry(
        `/actions/orchestrator/message-${action}`,
        undefined,
        { headers: { "Content-Type": "application/json" }, body: JSON.stringify(messageProposal) },
      );
      if (!response.ok) throw new Error((await response.text()).trim() || `Message ${action} failed (${response.status})`);
      setMessageProposal(null);
      setMessageError("");
      setActionNotice(action === "confirm"
        ? `Queued the confirmed message for issue #${messageProposal.issue}, attempt ${messageProposal.attempt}.`
        : "Cancelled the orchestrator message proposal.");
    } catch (reason) {
      setActionNotice(reason instanceof Error ? reason.message : `Message ${action} failed.`);
    } finally {
      setMessageBusy("");
    }
  }, [messageProposal]);

  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);

  const projected = snapshot ? snapshot.statuses : [];
  const hidden = useMemo(() => new Set((dashboardState.hidden ?? []).map(attemptKey)), [dashboardState]);
  const statuses = projected.filter((status) => !hidden.has(attemptKey(status)));
  const repository = projected.length ? projected[0].repository : "Repository dashboard";
  const counts = useMemo(() => Object.entries(statuses.reduce((result, status) => {
    result[status.state] = (result[status.state] ?? 0) + 1;
    return result;
  }, {})).sort(([a], [b]) => a.localeCompare(b)), [statuses]);
  const current = statuses.filter((status) => status.state !== "completed");
  const completed = statuses.filter((status) => status.state === "completed");
  const visible = tab === "completed" ? completed : current;
  const health = overallHealth(snapshot, error, statuses, now);
  const openAttemptTerminal = useCallback((status) => {
    const query = new URLSearchParams({ issue: String(status.issue), attempt: String(status.attempt) });
    setTerminal({ endpoint: `/terminal?${query}`, title: status.session, eyebrow: "tmux session" });
  }, []);
  const openOrchestratorTerminal = useCallback(() => {
    if (!orchestratorStatus?.session) return;
    setTerminal({ endpoint: "/orchestrator/terminal", title: orchestratorStatus.session, eyebrow: "orchestrator tmux session" });
  }, [orchestratorStatus]);

  return (
    <main>
      <header className="hero">
        <div>
          <p className="eyebrow">Agent Symphony</p>
          <h1>{repository}</h1>
          <p className="freshness" aria-live="polite">
            {snapshot ? `Updated ${relativeTime(snapshot.updated_at, now)}` : "Loading status…"}
          </p>
        </div>
        <div className="counts" aria-label="Issue counts by state">
          {counts.map(([state, count]) => (
            <div className="count" key={state}>
              <strong>{count}</strong>
              <span>{state}</span>
            </div>
          ))}
        </div>
      </header>

      <section className={`health health-${health.state}`} role="status" aria-live="polite" aria-atomic="true">
        <span className="healthDot" aria-hidden="true" />
        <div>
          <h2>{health.title}</h2>
          <p>{health.detail}</p>
        </div>
        <ReconcileButton onNotice={setActionNotice} onSnapshot={setSnapshot} />
      </section>

      <OrchestratorCard
        status={orchestratorStatus}
        error={orchestratorError}
        busy={orchestratorBusy}
        onAction={(action) => performOrchestratorAction(action)}
        onOpenTerminal={openOrchestratorTerminal}
        proposal={messageProposal}
        messageError={messageError}
        messageBusy={messageBusy}
        onMessageAction={performMessageAction}
      />

      {actionNotice ? <p className="notice" role="status">{actionNotice}</p> : null}
      {!error && snapshot && statuses.length === 0 ? <p className="notice">No visible attempts in the current projection.</p> : null}

      {statuses.length > 0 ? (
        <div className="tabs" role="tablist" aria-label="Attempt status views">
          <button
            type="button"
            role="tab"
            aria-selected={tab === "current"}
            onClick={() => setTab("current")}
          >
            Current <span>{current.length}</span>
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={tab === "completed"}
            onClick={() => setTab("completed")}
          >
            Completed <span>{completed.length}</span>
          </button>
        </div>
      ) : null}

      {!error && snapshot && statuses.length > 0 && visible.length === 0 ? (
        <p className="notice">No {tab} attempts.</p>
      ) : null}

      <section className="grid" aria-label="Issue status">
        {visible.map((status) => (
          <StatusCard
            key={`${status.repository}-${status.issue}-${status.attempt}`}
            status={status}
            onOpenTerminal={openAttemptTerminal}
            onAction={performAction}
            onInvestigate={(status) => performOrchestratorAction("investigate", status)}
            investigationEnabled={Boolean(orchestratorStatus?.enabled && orchestratorStatus.state === "running")}
            investigationBusy={Boolean(orchestratorBusy)}
            busy={busy === attemptKey(status)}
            investigating={investigating === attemptKey(status)}
            waiting={waiting && busy === attemptKey(status)}
          />
        ))}
      </section>
      {terminal ? <TerminalPanel config={terminal} onClose={closeTerminal} /> : null}
    </main>
  );
}
