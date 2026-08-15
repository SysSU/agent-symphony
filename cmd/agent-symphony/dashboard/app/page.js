"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { canInvestigate, orchestratorPresentation, overallHealth } from "./health.mjs";

const refreshEvery = 5000;

function githubURL(repository, kind, number) {
  const parts = repository.split("/");
  if (parts.length !== 2 || !number) return "";
  return `https://github.com/${parts.map(encodeURIComponent).join("/")}/${kind}/${number}`;
}

function relativeTime(value, now) {
  const seconds = Math.max(0, Math.floor((now - new Date(value).getTime()) / 1000));
  if (seconds < 5) return "just now";
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  return `${Math.floor(minutes / 60)}h ago`;
}

function worktreeName(path) {
  return path?.split(/[\\/]/).filter(Boolean).pop() ?? "";
}

function attemptKey(status) {
  return `${status.repository}#${status.issue}/${status.attempt}`;
}

function Detail({ label, children }) {
  if (!children || (Array.isArray(children) && children.length === 0)) return null;
  return (
    <div className="detail">
      <dt>{label}</dt>
      <dd>{children}</dd>
    </div>
  );
}

function TerminalPanel({ config, onClose }) {
  const container = useRef(null);
  const closeButton = useRef(null);
  const panel = useRef(null);
  const opener = useRef(null);
  const [connection, setConnection] = useState("Connecting…");

  useEffect(() => {
    let disposed = false;
    let socket;
    let terminal;
    let resizeObserver;
    let input;

    async function connect() {
      const [{ Terminal }, { FitAddon }] = await Promise.all([
        import("@xterm/xterm"),
        import("@xterm/addon-fit"),
      ]);
      if (disposed || !container.current) return;
      terminal = new Terminal({
        convertEol: true,
        cursorBlink: true,
        fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace",
        fontSize: 14,
        screenReaderMode: true,
        theme: { background: "#101719", foreground: "#e8efed", cursor: "#69c4b2" },
      });
      const fit = new FitAddon();
      terminal.loadAddon(fit);
      terminal.open(container.current);
      fit.fit();
      terminal.focus();

      const scheme = window.location.protocol === "https:" ? "wss:" : "ws:";
      socket = new WebSocket(`${scheme}//${window.location.host}${config.endpoint}`);
      socket.binaryType = "arraybuffer";
      const sendSize = () => {
        if (socket.readyState === WebSocket.OPEN) {
          socket.send(JSON.stringify({ type: "resize", cols: terminal.cols, rows: terminal.rows }));
        }
      };
      socket.addEventListener("open", () => {
        setConnection("Connected");
        sendSize();
      });
      socket.addEventListener("message", (event) => terminal.write(new Uint8Array(event.data)));
      socket.addEventListener("close", (event) => {
        if (disposed) return;
        const message = `Terminal disconnected${event.reason ? `: ${event.reason}` : "."}`;
        setConnection(message);
        terminal.writeln(`\r\n\x1b[33m${message}\x1b[0m`);
      });
      input = terminal.onData((data) => {
        if (socket.readyState === WebSocket.OPEN) socket.send(new TextEncoder().encode(data));
      });
      resizeObserver = new ResizeObserver(() => {
        fit.fit();
        sendSize();
      });
      resizeObserver.observe(container.current);
    }

    connect().catch(() => {
      setConnection("Terminal failed to load.");
      if (container.current) container.current.textContent = "Terminal failed to load.";
    });
    opener.current = document.activeElement;
    closeButton.current?.focus();
    const keyboard = (event) => {
      if (event.key === "Escape") {
        event.preventDefault();
        onClose();
        return;
      }
      if (event.key !== "Tab" || !panel.current) return;
      const focusable = [...panel.current.querySelectorAll("button, [href], input, textarea, [tabindex]:not([tabindex='-1'])")]
        .filter((element) => !element.disabled && element.getAttribute("aria-hidden") !== "true");
      if (!focusable.length) return;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (!panel.current.contains(document.activeElement)) {
        event.preventDefault();
        first.focus();
      } else if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };
    window.addEventListener("keydown", keyboard);
    document.body.classList.add("terminalOpen");
    return () => {
      disposed = true;
      window.removeEventListener("keydown", keyboard);
      document.body.classList.remove("terminalOpen");
      resizeObserver?.disconnect();
      input?.dispose();
      socket?.close();
      terminal?.dispose();
      opener.current?.focus();
    };
  }, [config.endpoint, onClose]);

  return (
    <div className="terminalBackdrop" role="dialog" aria-modal="true" aria-labelledby="terminalTitle" aria-describedby="terminalConnection">
      <section className="terminalPanel" ref={panel}>
        <header>
          <div>
            <p className="eyebrow">{config.eyebrow}</p>
            <h2 id="terminalTitle">{config.title}</h2>
            <p className="terminalConnection" id="terminalConnection" role="status" aria-live="polite">{connection}</p>
          </div>
          <button ref={closeButton} type="button" onClick={onClose}>Close</button>
        </header>
        <div className="terminal" ref={container} aria-label={`Terminal for ${config.title}`} />
      </section>
    </div>
  );
}

function StatusCard({ status, onOpenTerminal, onAction, onInvestigate, investigationEnabled, investigationBusy, busy, investigating, waiting }) {
  const issueURL = githubURL(status.repository, "issues", status.issue);
  const prURL = githubURL(status.repository, "pull", status.pr);
  const issueLabel = status.title ? `#${status.issue} ${status.title}` : `Issue #${status.issue}`;

  return (
    <article className="card">
      <header className="cardHeader">
        <div>
          <a className="issue" href={issueURL} target="_blank" rel="noreferrer">
            {issueLabel}
          </a>
          <p className="identity">
            Attempt {status.attempt || "—"}
            {status.pr ? (
              <>
                {" · "}
                <a href={prURL} target="_blank" rel="noreferrer">PR #{status.pr}</a>
              </>
            ) : null}
          </p>
        </div>
        <span className={`state state-${status.state}`}>{status.state}</span>
      </header>
      <dl>
        <Detail label="Priority">{status.priority ? `P${status.priority}` : "—"}</Detail>
        <Detail label="tmux session">
          {status.session ? (
            <button className="terminalLink" type="button" onClick={() => onOpenTerminal(status)}>
              <code>{status.session}</code>
            </button>
          ) : null}
        </Detail>
        <Detail label="Worktree"><code>{worktreeName(status.worktree)}</code></Detail>
        <Detail label="Branch"><code>{status.branch}</code></Detail>
        <Detail label="Blockers">
          {status.blockers?.map((blocker) => <span className="line" key={blocker}>{blocker}</span>)}
        </Detail>
        <Detail label="Checks">
          {status.checks?.map((check) => <span className="line" key={check}>{check}</span>)}
        </Detail>
        <Detail label="Diagnostic">{status.diagnostic}</Detail>
        <Detail label="Next action">{status.next_action}</Detail>
      </dl>
      {status.state === "completed" || status.state === "orphaned" || investigationEnabled && canInvestigate(status) ? (
        <footer className="cardActions">
          {investigationEnabled && canInvestigate(status) ? (
            <button className="primaryAction" type="button" disabled={investigationBusy} onClick={() => onInvestigate(status)}>
              {investigating ? "Requesting…" : "Ask orchestrator to investigate"}
            </button>
          ) : null}
          {status.state === "completed" || status.state === "orphaned" ? (
            <button
              className={status.state === "orphaned" ? "dangerAction" : "secondaryAction"}
              type="button"
              disabled={busy}
              onClick={() => onAction(status.state === "completed" ? "archive" : "abandon", status)}
            >
              {waiting ? "Waiting for reconciliation…" : busy ? "Working…" : status.state === "completed" ? "Archive" : "Abandon attempt"}
            </button>
          ) : null}
        </footer>
      ) : null}
    </article>
  );
}

function Timestamp({ value }) {
  if (!value || value.startsWith("0001-")) return "—";
  const parsed = new Date(value);
  return Number.isFinite(parsed.getTime()) ? <time dateTime={value}>{parsed.toLocaleString()}</time> : "—";
}

function OrchestratorCard({ status, error, busy, onAction, onOpenTerminal }) {
  const presentation = orchestratorPresentation(status, error);
  const enabled = Boolean(status?.enabled);
  const working = Boolean(busy);

  return (
    <section className="orchestratorCard" aria-labelledby="orchestratorTitle">
      <header className="cardHeader">
        <div>
          <p className="eyebrow">Supervised agent</p>
          <h2 id="orchestratorTitle">Orchestrator</h2>
          <p className="identity">Long-lived operator and diagnostic console</p>
        </div>
        <span className={`state state-${presentation.state}`} role="status" aria-live="polite">{presentation.label}</span>
      </header>
      <dl>
        <Detail label="tmux session">
          {status?.session ? (
            <button className="terminalLink" type="button" disabled={status.state !== "running"} onClick={onOpenTerminal}>
              <code>{status.session}</code>
            </button>
          ) : "—"}
        </Detail>
        <Detail label="Context">{status?.generation ? `Generation ${status.generation} · ${status.context_mode || "unknown"}` : "—"}</Detail>
        <Detail label="Started"><Timestamp value={status?.started_at} /></Detail>
        <Detail label="Rebuilt"><Timestamp value={status?.rebuilt_at} /></Detail>
        <Detail label="Last healthy"><Timestamp value={status?.last_healthy_at} /></Detail>
        <Detail label="Retry"><Timestamp value={status?.retry_at} /></Detail>
        <Detail label="Pending notices">{String(status?.pending_attention ?? 0)}</Detail>
        <Detail label="Diagnostic">{error || status?.diagnostic}</Detail>
        <Detail label="Next action">{status?.next_action}</Detail>
      </dl>
      {enabled ? (
        <footer className="cardActions orchestratorActions">
          <button className="primaryAction" type="button" disabled={status.state !== "running" || working} onClick={onOpenTerminal}>Open terminal</button>
          <button className="secondaryAction" type="button" disabled={working} onClick={() => onAction("recover")}>{busy === "recover" ? "Recovering…" : "Recover/restart"}</button>
          <button className="dangerAction" type="button" disabled={working} onClick={() => onAction("clear")}>{busy === "clear" ? "Clearing…" : "Clear context"}</button>
          <button className="secondaryAction" type="button" disabled={working} onClick={() => onAction("rebuild")}>{busy === "rebuild" ? "Rebuilding…" : "Rebuild context"}</button>
        </footer>
      ) : status ? (
        <p className="orchestratorDisabled">Configure an orchestrator command to enable this console.</p>
      ) : (
        <p className="orchestratorDisabled">{error || "Loading orchestrator status…"}</p>
      )}
    </section>
  );
}

export default function Dashboard() {
  const [snapshot, setSnapshot] = useState(null);
  const [dashboardState, setDashboardState] = useState({ hidden: [] });
  const [error, setError] = useState("");
  const [actionNotice, setActionNotice] = useState("");
  const [busy, setBusy] = useState("");
  const [waiting, setWaiting] = useState(false);
  const [now, setNow] = useState(Date.now());
  const [tab, setTab] = useState("current");
  const [terminal, setTerminal] = useState(null);
  const [orchestratorStatus, setOrchestratorStatus] = useState(null);
  const [orchestratorError, setOrchestratorError] = useState("");
  const [orchestratorBusy, setOrchestratorBusy] = useState("");
  const [investigating, setInvestigating] = useState("");
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
        const [response, stateResponse, orchestratorResult] = await Promise.all([
          fetch("/status.json", { cache: "no-store" }),
          fetch("/dashboard-state.json", { cache: "no-store" }),
          orchestratorRequest,
        ]);
        if (!response.ok) throw new Error(response.status === 404 ? "Waiting for the first reconciliation" : `Status request failed (${response.status})`);
        if (!stateResponse.ok) throw new Error(`Dashboard state request failed (${stateResponse.status})`);
        const [next, nextState] = await Promise.all([response.json(), stateResponse.json()]);
        if (active) {
          setSnapshot(next);
          setDashboardState(nextState);
          setOrchestratorStatus(orchestratorResult.status);
          setOrchestratorError(orchestratorResult.error);
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
    const verb = action === "archive" ? "Archive" : "Abandon";
    const consequence = action === "archive"
      ? "This stops its tmux session if needed, deletes its local worktree, and hides it from Completed."
      : "This stops its tmux session and permanently deletes its local worktree, log, and retained attempt record.";
    if (!window.confirm(`${verb} issue #${status.issue}, attempt ${status.attempt}?\n\n${consequence}`)) return;
    const key = attemptKey(status);
    setBusy(key);
    setWaiting(false);
    setActionNotice("");
    try {
      const query = new URLSearchParams({ issue: String(status.issue), attempt: String(status.attempt) });
      let response;
      do {
        response = await fetch(`/actions/${action}?${query}`, { method: "POST" });
        if (response.status === 503) {
          await response.text();
          setWaiting(true);
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
      } while (response.status === 503);
      if (!response.ok) throw new Error((await response.text()).trim() || `${verb} failed (${response.status})`);
      const stateResponse = await fetch("/dashboard-state.json", { cache: "no-store" });
      if (!stateResponse.ok) throw new Error(`${verb} finished, but dashboard state could not be refreshed.`);
      setDashboardState(await stateResponse.json());
      setActionNotice(`${action === "archive" ? "Archived" : "Abandoned"} issue #${status.issue}, attempt ${status.attempt}.`);
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
      const response = await fetch(`/actions/orchestrator/${action}${query}`, { method: "POST" });
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

  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);

  const projected = snapshot?.statuses ?? [];
  const hidden = useMemo(() => new Set((dashboardState.hidden ?? []).map(attemptKey)), [dashboardState]);
  const statuses = projected.filter((status) => !hidden.has(attemptKey(status)));
  const repository = projected[0]?.repository ?? "Repository dashboard";
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
  }, [orchestratorStatus?.session]);

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
      </section>

      <OrchestratorCard
        status={orchestratorStatus}
        error={orchestratorError}
        busy={orchestratorBusy}
        onAction={(action) => performOrchestratorAction(action)}
        onOpenTerminal={openOrchestratorTerminal}
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
