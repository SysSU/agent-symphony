"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { overallHealth } from "./health.mjs";

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

function TerminalPanel({ status, onClose }) {
  const container = useRef(null);
  const closeButton = useRef(null);

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
      const query = new URLSearchParams({ issue: String(status.issue), attempt: String(status.attempt) });
      socket = new WebSocket(`${scheme}//${window.location.host}/terminal?${query}`);
      socket.binaryType = "arraybuffer";
      const sendSize = () => {
        if (socket.readyState === WebSocket.OPEN) {
          socket.send(JSON.stringify({ type: "resize", cols: terminal.cols, rows: terminal.rows }));
        }
      };
      socket.addEventListener("open", sendSize);
      socket.addEventListener("message", (event) => terminal.write(new Uint8Array(event.data)));
      socket.addEventListener("close", (event) => {
        terminal.writeln(`\r\n\x1b[33mTerminal disconnected${event.reason ? `: ${event.reason}` : "."}\x1b[0m`);
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
      if (container.current) container.current.textContent = "Terminal failed to load.";
    });
    closeButton.current?.focus();
    const escape = (event) => { if (event.key === "Escape") onClose(); };
    window.addEventListener("keydown", escape);
    document.body.classList.add("terminalOpen");
    return () => {
      disposed = true;
      window.removeEventListener("keydown", escape);
      document.body.classList.remove("terminalOpen");
      resizeObserver?.disconnect();
      input?.dispose();
      socket?.close();
      terminal?.dispose();
    };
  }, [onClose, status.attempt, status.issue]);

  return (
    <div className="terminalBackdrop" role="dialog" aria-modal="true" aria-labelledby="terminalTitle">
      <section className="terminalPanel">
        <header>
          <div>
            <p className="eyebrow">tmux session</p>
            <h2 id="terminalTitle">{status.session}</h2>
          </div>
          <button ref={closeButton} type="button" onClick={onClose}>Close</button>
        </header>
        <div className="terminal" ref={container} aria-label={`Terminal for ${status.session}`} />
      </section>
    </div>
  );
}

function StatusCard({ status, onOpenTerminal, onAction, busy, waiting }) {
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
      {status.state === "completed" || status.state === "orphaned" || status.retryable ? (
		<footer className="cardActions">
		  <button
			className={status.state === "completed" ? "secondaryAction" : "dangerAction"}
			type="button"
			disabled={busy}
			onClick={() => onAction(status.retryable ? "recover" : status.state === "completed" ? "archive" : "abandon", status)}
		  >
			{waiting ? "Waiting for reconciliation…" : busy ? "Working…" : status.retryable ? "Recover attempt" : status.state === "completed" ? "Archive" : "Abandon attempt"}
		  </button>
        </footer>
      ) : null}
    </article>
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
  const closeTerminal = useCallback(() => setTerminal(null), []);

  useEffect(() => {
    let active = true;
    async function refresh() {
      try {
        const [response, stateResponse] = await Promise.all([
          fetch("/status.json", { cache: "no-store" }),
          fetch("/dashboard-state.json", { cache: "no-store" }),
        ]);
        if (!response.ok) throw new Error(response.status === 404 ? "Waiting for the first reconciliation" : `Status request failed (${response.status})`);
        if (!stateResponse.ok) throw new Error(`Dashboard state request failed (${stateResponse.status})`);
        const [next, nextState] = await Promise.all([response.json(), stateResponse.json()]);
        if (active) {
          setSnapshot(next);
          setDashboardState(nextState);
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
      const finished = action === "archive" ? "Archived" : action === "recover" ? "Recovery requested for" : "Abandoned";
      setActionNotice(`${finished} issue #${status.issue}, attempt ${status.attempt}.`);
    } catch (reason) {
      setActionNotice(reason instanceof Error ? reason.message : `${verb} failed.`);
    } finally {
      setBusy("");
      setWaiting(false);
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
            onOpenTerminal={setTerminal}
            onAction={performAction}
            busy={busy === attemptKey(status)}
            waiting={waiting && busy === attemptKey(status)}
          />
        ))}
      </section>
      {terminal ? <TerminalPanel status={terminal} onClose={closeTerminal} /> : null}
    </main>
  );
}
