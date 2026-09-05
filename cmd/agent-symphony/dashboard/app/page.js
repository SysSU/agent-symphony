"use client";

import { useCallback, useEffect, useState } from "react";

import { StatusCard } from "./_components/status-card";
import AttemptHistory from "./_components/attempt-history";
import ProjectNavigation, { ProjectAgentConsole, ProjectHeader, ProjectHealthControl, projectBoard, projectView } from "./_components/project-navigation";
import { getMessageProposal, getOrchestratorStatus, getRelease, postWithReconciliationRetry } from "./actions.mjs";
import TerminalPanel from "./_components/terminal-panel";
import { attemptKey } from "./health.mjs";

export default function Dashboard() {
  const [snapshot, setSnapshot] = useState(null);
  const [dashboardState, setDashboardState] = useState({ hidden: [] });
  const [release, setRelease] = useState("");
  const [error, setError] = useState("");
  const [actionNotice, setActionNotice] = useState("");
  const [busy, setBusy] = useState("");
  const [waiting, setWaiting] = useState(false);
  const [now, setNow] = useState(() => Date.now());
  const [terminal, setTerminal] = useState(null);
  const [orchestratorStatus, setOrchestratorStatus] = useState(null);
  const [orchestratorError, setOrchestratorError] = useState("");
  const [orchestratorBusy, setOrchestratorBusy] = useState("");
  const [investigating, setInvestigating] = useState("");
  const [messageProposal, setMessageProposal] = useState(null);
  const [messageError, setMessageError] = useState("Loading worker-message controls…");
  const [messageBusy, setMessageBusy] = useState("");
  const [projects, setProjects] = useState([]);
  const [selectedRepository, setSelectedRepository] = useState("");
  const closeTerminal = useCallback(() => setTerminal(null), []);

  useEffect(() => {
    let active = true;
    async function refresh() {
      getRelease().then((value) => { if (active) setRelease(value); });
      try {
        const orchestratorRequest = getOrchestratorStatus();
        const proposalRequest = getMessageProposal();
        const [response, stateResponse, projectsResponse, orchestratorResult, proposalResult] = await Promise.all([
          fetch("/status.json", { cache: "no-store" }),
          fetch("/dashboard-state.json", { cache: "no-store" }),
          fetch("/projects.json", { cache: "no-store" }),
          orchestratorRequest,
          proposalRequest,
        ]);
        if (!response.ok) throw new Error(response.status === 404 ? "Waiting for the first reconciliation" : `Status request failed (${response.status})`);
        if (!stateResponse.ok) throw new Error(`Dashboard state request failed (${stateResponse.status})`);
        if (!projectsResponse.ok) throw new Error(`Project request failed (${projectsResponse.status})`);
        const [next, nextState, nextProjects] = await Promise.all([response.json(), stateResponse.json(), projectsResponse.json()]);
        if (active) {
          setSnapshot(next);
          setDashboardState(nextState);
          setProjects(nextProjects.projects ?? []);
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
    const timer = setInterval(refresh, 5000);
    return () => {
      active = false;
      clearInterval(timer);
    };
  }, []);

  const performAction = useCallback(async (action, status) => {
    const verb = action === "archive" ? "Archive" : action === "recover" ? "Recover" : "Abandon";
    const consequence = action === "archive"
      ? "This stops its tmux session if needed, deletes its local worktree, and hides it from the Done lane."
      : action === "recover"
        ? "If the attempt is stuck, this stops only its named tmux session. It preserves the worktree and diagnostics, records the failure on GitHub, and requests a new attempt."
      : "This stops its tmux session and permanently deletes its local worktree, log, and retained attempt record.";
    if (!window.confirm(`${verb} issue #${status.issue}, attempt ${status.attempt}?\n\n${consequence}`)) return;
    const key = attemptKey(status);
    setBusy(key);
    setWaiting(false);
    setActionNotice("");
    try {
      const query = new URLSearchParams({ repository: status.repository, issue: String(status.issue), attempt: String(status.attempt) });
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
      const query = status ? `?${new URLSearchParams({ repository: status.repository, issue: String(status.issue), attempt: String(status.attempt) })}` : "";
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
    const { confirmationNonce, ...proposal } = messageProposal;
    setMessageBusy(action);
    setActionNotice("");
    try {
      const response = await postWithReconciliationRetry(
        `/actions/orchestrator/message-${action}`,
        undefined,
        { headers: { "Content-Type": "application/json", "X-Agent-Symphony-Confirmation-Nonce": confirmationNonce }, body: JSON.stringify(proposal) },
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

  const view = projectView(projects, selectedRepository, snapshot, dashboardState, error);
  const { remote: remoteProject, snapshot: visibleSnapshot, error: visibleError } = view;
  const { statuses, historical, counts, title, lanes, health } = projectBoard(view, now);
  const remoteURL = remoteProject?.url || "";
  const openAttemptTerminal = (status, session) => {
    const selected = session ?? { role: "implementation", name: status.session }, route = { implementation: "/terminal", reviewer: "/reviewer/terminal" }[selected.role];
    if (!remoteURL && route && selected.name) setTerminal({ endpoint: `${route}?${new URLSearchParams({ repository: status.repository, issue: String(status.issue), attempt: String(status.attempt) })}`, title: selected.name, eyebrow: `${selected.mode ?? selected.role} tmux session` });
  };
  const openOrchestratorTerminal = useCallback(() => {
    if (!orchestratorStatus?.session) return;
    setTerminal({ endpoint: "/orchestrator/terminal", title: orchestratorStatus.session, eyebrow: "orchestrator tmux session" });
  }, [orchestratorStatus]);

  return (
    <main>
      <ProjectHeader title={title} snapshot={visibleSnapshot} release={release} counts={counts} now={now} />

      <ProjectNavigation projects={projects} remote={remoteProject} onSelect={setSelectedRepository} />

      <section className={`health health-${health.state}`} role="status" aria-live="polite" aria-atomic="true">
        <span className="healthDot" aria-hidden="true" />
        <div>
          <h2>{health.title}</h2>
          <p>{health.detail}</p>
        </div>
        <ProjectHealthControl remote={remoteProject} onNotice={setActionNotice} onSnapshot={setSnapshot} />
      </section>

      <ProjectAgentConsole
        remote={remoteProject}
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
      {!visibleError && visibleSnapshot && statuses.length === 0 ? <p className="notice">No visible attempts in the current projection.</p> : null}

      <section className="board" aria-label="Issue status board" tabIndex={0} onKeyDown={(event) => {
        if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
        event.preventDefault();
        event.currentTarget.scrollLeft += event.key === "ArrowLeft" ? -event.currentTarget.clientWidth : event.currentTarget.clientWidth;
      }}>
        {visibleSnapshot ? lanes.map((lane) => (
          <section className="lane" aria-labelledby={`lane-${lane.id}`} key={lane.id}>
            <header className="laneHeader">
              <h2 id={`lane-${lane.id}`}>{lane.title}
                <span className="laneCount" aria-label={`${lane.statuses.length} attempt${lane.statuses.length === 1 ? "" : "s"}`}>{lane.statuses.length}</span>
              </h2>
            </header>
            {lane.statuses.length ? (
              <ol className="laneCards">
                {lane.statuses.map((status) => (
                  <li key={`${status.repository}-${status.issue}-${status.attempt}`}>
                    <StatusCard
                      status={status}
                      onOpenTerminal={openAttemptTerminal}
                      onAction={performAction}
                      onInvestigate={(status) => performOrchestratorAction("investigate", status)}
                      investigationEnabled={Boolean(orchestratorStatus?.enabled && orchestratorStatus.state === "running")}
                      investigationBusy={Boolean(orchestratorBusy)}
                      busy={busy === attemptKey(status)}
                      investigating={investigating === attemptKey(status)}
                      waiting={waiting && busy === attemptKey(status)}
                      followUpEnabled={!messageError}
                      onNotice={setActionNotice}
                      readOnly={Boolean(remoteProject)}
                    />
                  </li>
                ))}
              </ol>
            ) : <p className="emptyLane">No attempts</p>}
          </section>
        )) : <p className="boardState" role="status">{visibleError ? "Issue status board unavailable." : "Loading issue status board…"}</p>}
      </section>
      <AttemptHistory statuses={historical} />
      {terminal ? <TerminalPanel config={terminal} onClose={closeTerminal} /> : null}
    </main>
  );
}
