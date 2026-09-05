import { useEffect, useRef, useState } from "react";

import { postWithReconciliationRetry } from "../actions.mjs";
import { canInvestigate, orchestratorPresentation } from "../health.mjs";

const attachableSessionRoles = new Set(["implementation", "reviewer"]);

function sessionDisabled(readOnly, role) {
  return readOnly || !attachableSessionRoles.has(role);
}

function githubURL(repository, kind, number) {
  const parts = repository.split("/");
  if (parts.length !== 2 || !number) return "";
  return `https://github.com/${parts.map(encodeURIComponent).join("/")}/${kind}/${number}`;
}

function worktreeName(path) {
  return path?.split(/[\\/]/).filter(Boolean).pop() ?? "";
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

function AttentionChip({ needed }) {
  return needed ? <span className="state state-blocked">needs attention</span> : null;
}

function actionFor(status) {
  if (status.retryable) return "recover";
  return status.state === "completed" ? "archive" : "abandon";
}

function actionLabel(status, busy, waiting) {
  if (waiting) return "Waiting for reconciliation…";
  if (busy) return "Working…";
  if (status.retryable) return "Recover attempt";
  return status.state === "completed" ? "Archive" : "Abandon attempt";
}

function StatusActions({ status, onAction, onInvestigate, investigationEnabled, investigationBusy, busy, investigating, waiting, readOnly }) {
  const investigationAvailable = investigationEnabled && canInvestigate(status);
  const actionAvailable = status.state === "completed" || status.state === "orphaned" || status.retryable;
  if (readOnly || (!investigationAvailable && !actionAvailable)) return null;
  const action = actionFor(status);
  const label = actionLabel(status, busy, waiting);
  return (
    <footer className="cardActions">
      {investigationAvailable ? (
        <button className="primaryAction" type="button" disabled={investigationBusy} onClick={() => onInvestigate(status)}>
          {investigating ? "Requesting…" : "Ask orchestrator to investigate"}
        </button>
      ) : null}
      {actionAvailable ? (
        <button className={status.state === "completed" ? "secondaryAction" : "dangerAction"} type="button" disabled={busy} onClick={() => onAction(action, status)}>
          {label}
        </button>
      ) : null}
    </footer>
  );
}

function OperatorFollowUp({ status, enabled, onNotice, readOnly }) {
	const [message, setMessage] = useState("");
	const [busy, setBusy] = useState(false);
	const retryable = status.retryable && ["blocked", "orphaned"].includes(status.state);
	if (readOnly || !enabled || (!retryable && !["active", "review-ready"].includes(status.state))) return null;
  const id = `follow-up-${status.issue}-${status.attempt}`;
  async function submit(event) {
    event.preventDefault();
    const body = message.trim();
    if (!body) return;
    setBusy(true);
    onNotice("");
    try {
      const query = new URLSearchParams({ issue: String(status.issue), attempt: String(status.attempt) });
      const response = await postWithReconciliationRetry(`/actions/attempt/message?${query}`, undefined, { headers: { "Content-Type": "application/json" }, body: JSON.stringify({ message: body }) });
      if (!response.ok) throw new Error((await response.text()).trim() || `Worker follow-up failed (${response.status})`);
      setMessage("");
      onNotice(`Queued a follow-up for issue #${status.issue}, attempt ${status.attempt}. The coordinator will start its verified follow-up when safe.`);
    } catch (reason) {
      onNotice(reason instanceof Error ? reason.message : "Worker follow-up failed.");
    } finally {
      setBusy(false);
    }
  }
  return (
    <form className="followUp" onSubmit={submit}>
      <label htmlFor={id}>Tell this agent what to do next</label>
      <textarea id={id} value={message} maxLength={8192} required rows={4} disabled={busy} onChange={(event) => setMessage(event.target.value)} aria-describedby={`${id}-help`} />
      <p id={`${id}-help`}>Queues one verified fresh turn in this exact worktree. Open tmux above to watch after it starts.</p>
      <button className="primaryAction" type="submit" disabled={busy || !message.trim()}>{busy ? "Queueing…" : "Queue follow-up"}</button>
    </form>
  );
}

export function StatusCard(props) {
  const { status, onOpenTerminal, readOnly } = props;
  const issueURL = githubURL(status.repository, "issues", status.issue);
  const prURL = githubURL(status.repository, "pull", status.pr);
  const issueLabel = status.title ? `#${status.issue} ${status.title}` : `Issue #${status.issue}`;
  const sessions = status.sessions?.length ? status.sessions : status.session ? [{ role: "implementation", name: status.session, state: status.state, current: true }] : [];
  const blockers = status.blockers || [];
  const checks = status.checks || [];
  const messages = status.operator_messages || [];
  const state = status.state || "unknown";
  const priority = status.priority ? `P${status.priority}` : "P—";

  return (
    <article className="card">
      <header className="cardHeader">
        <div>
          <a className="issue" href={issueURL} target="_blank" rel="noreferrer">{issueLabel}</a>
          <p className="identity">
            Attempt {status.attempt || "—"}
            {status.pr ? <>{" · "}<a href={prURL} target="_blank" rel="noreferrer">PR #{status.pr}</a></> : null}
          </p>
        </div>
        <div className="cardChips">
          <AttentionChip needed={status.needs_attention} />
          <span className={`priority priority-${status.priority || "unset"}`} aria-label={`Priority ${status.priority || "not set"}`}>{priority}</span>
          <span className={`state state-${state}`}>{state}</span>
        </div>
      </header>
      <dl>
        <Detail label="tmux session">
          {status.session ? <button className="terminalLink" type="button" disabled={readOnly} onClick={() => onOpenTerminal(status)}><code>{status.session}</code></button> : null}
        </Detail>
        <Detail label="Worktree"><code>{worktreeName(status.worktree)}</code></Detail>
        <Detail label="Branch"><code>{status.branch}</code></Detail>
        <Detail label="Current phase">{status.current_phase}</Detail>
        <Detail label="Session lifecycle">{sessions.map((session) => (
          <span className="line" key={session.role}>
            {session.role === "implementation" ? null : <><button className="terminalLink" type="button" disabled={sessionDisabled(readOnly, session.role)} onClick={() => onOpenTerminal(status, session)} aria-label={`Open ${session.role} terminal`}><code>{session.name}</code></button>{" · "}</>}
            {`${session.role}${session.mode ? ` · ${session.mode}` : ""} · ${session.state}${session.current ? " · current" : ""}`}
            {session.target ? <> {" · target "}<code>{session.target}</code></> : null}
            {session.updated_at ? <> {" · "}<Timestamp value={session.updated_at} /></> : null}
          </span>
        ))}</Detail>
        <Detail label="Blockers">{blockers.map((blocker) => <span className="line" key={blocker}>{blocker}</span>)}</Detail>
        <Detail label="Checks">{checks.map((check) => <span className="line" key={check}>{check}</span>)}</Detail>
        <Detail label="Worker messages">{messages.map((message) => (
          <span className="line messageStatus" key={message.id}>
            <span className={`state state-${message.state}`}>{message.state}</span>{" "}
            <code>{message.id.slice(0, 12)}</code>
            {message.diagnostic ? ` · ${message.diagnostic}` : ""}
          </span>
        ))}</Detail>
        <Detail label="Diagnostic">{status.diagnostic}</Detail>
        <Detail label="Next action">{status.next_action}</Detail>
      </dl>
      <OperatorFollowUp status={status} enabled={props.followUpEnabled} onNotice={props.onNotice} readOnly={readOnly} />
      <StatusActions {...props} readOnly={readOnly} />
    </article>
  );
}

function MessageConfirmation({ proposal, error, busy, onAction }) {
  const confirmationRef = useRef(null);
  const proposalKey = proposal ? `${proposal.repository}#${proposal.issue}/${proposal.attempt}:${proposal.message}` : "";
  useEffect(() => {
    if (proposalKey) confirmationRef.current?.focus();
  }, [proposalKey]);
  if (!proposal && !error) return null;
  if (!proposal) return <p className="orchestratorDisabled" role="alert">{error}</p>;
  return (
    <section
      ref={confirmationRef}
      className="messageConfirmation"
      role="region"
      aria-live="assertive"
      aria-atomic="true"
      aria-labelledby="messageConfirmationTitle"
      tabIndex={-1}
    >
      <p className="eyebrow">Confirmation required</p>
      <h3 id="messageConfirmationTitle">Queue a worker message</h3>
      <p><strong>Exact target:</strong> <code>{proposal.repository}#{proposal.issue}, attempt {proposal.attempt}</code></p>
      <pre aria-label="Exact proposed worker message">{proposal.message}</pre>
      <p className="messageSemantics">This is not live chat. Confirmation records the message on GitHub, queues it behind active work, and starts one bounded follow-up turn when safe.</p>
      <footer className="cardActions orchestratorActions">
        <button className="primaryAction" type="button" disabled={Boolean(busy)} onClick={() => onAction("confirm")}>{busy === "confirm" ? "Queueing…" : "Confirm and queue"}</button>
        <button className="secondaryAction" type="button" disabled={Boolean(busy)} onClick={() => onAction("cancel")}>{busy === "cancel" ? "Cancelling…" : "Cancel proposal"}</button>
      </footer>
    </section>
  );
}

function Timestamp({ value }) {
  if (!value || value.startsWith("0001-")) return "—";
  const parsed = new Date(value);
  return Number.isFinite(parsed.getTime()) ? <time dateTime={value}>{parsed.toLocaleString()}</time> : "—";
}

function OrchestratorActions({ busy, onAction, onOpenTerminal, state }) {
  const working = Boolean(busy);
  return (
    <footer className="cardActions orchestratorActions">
      <button className="primaryAction" type="button" disabled={state !== "running" || working} onClick={onOpenTerminal}>Open terminal</button>
      <button className="secondaryAction" type="button" disabled={working} onClick={() => onAction("recover")}>{busy === "recover" ? "Recovering…" : "Recover/restart"}</button>
      <button className="dangerAction" type="button" disabled={working} onClick={() => onAction("clear")}>{busy === "clear" ? "Clearing…" : "Clear context"}</button>
      <button className="secondaryAction" type="button" disabled={working} onClick={() => onAction("rebuild")}>{busy === "rebuild" ? "Rebuilding…" : "Rebuild context"}</button>
    </footer>
  );
}

export function OrchestratorCard({ status, error, busy, onAction, onOpenTerminal, proposal, messageError, messageBusy, onMessageAction }) {
  const current = status || {};
  const presentation = orchestratorPresentation(status, error);
  let controls = <p className="orchestratorDisabled">{error || "Loading orchestrator status…"}</p>;
  if (current.enabled) controls = <OrchestratorActions busy={busy} onAction={onAction} onOpenTerminal={onOpenTerminal} state={current.state} />;
  else if (status) controls = <p className="orchestratorDisabled">Configure an orchestrator command to enable this console.</p>;

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
          {current.session ? <button className="terminalLink" type="button" disabled={current.state !== "running"} onClick={onOpenTerminal}><code>{current.session}</code></button> : "—"}
        </Detail>
        <Detail label="Context">{current.generation ? `Generation ${current.generation} · ${current.context_mode || "unknown"}` : "—"}</Detail>
        <Detail label="Started"><Timestamp value={current.started_at} /></Detail>
        <Detail label="Rebuilt"><Timestamp value={current.rebuilt_at} /></Detail>
        <Detail label="Last healthy"><Timestamp value={current.last_healthy_at} /></Detail>
        <Detail label="Retry"><Timestamp value={current.retry_at} /></Detail>
        <Detail label="Pending notices">{String(current.pending_attention ?? 0)}</Detail>
        <Detail label="Diagnostic">{error || current.diagnostic}</Detail>
        <Detail label="Next action">{current.next_action}</Detail>
      </dl>
      {controls}
      <MessageConfirmation proposal={proposal} error={messageError} busy={messageBusy} onAction={onMessageAction} />
    </section>
  );
}
