import { canInvestigate, orchestratorPresentation } from "../health.mjs";
import PlanReviewButton from "./plan-review-button";

const attachableSessionRoles = new Set(["implementation", "reviewer"]);
const dismissibleStates = new Set(["completed", "failed", "orphaned", "cancelled"]);

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

function actionLabel(status, busy) {
  if (busy) return "Working…";
  if (status.retryable) return "Recover attempt";
  return status.state === "completed" ? "Archive" : "Abandon attempt";
}

function canPlanReview(status) {
  return status.state === "active" && !status.sessions?.some((session) => session.role === "reviewer" && ["preparing", "running"].includes(session.state));
}

function attemptActionAvailable(status, historical) {
  return !historical && (status.state === "completed" || status.state === "orphaned" || status.retryable);
}

function canDismiss(status) {
  return status.issue_closed && dismissibleStates.has(status.state);
}

function DismissButton({ status, onAction, busy }) {
  if (!canDismiss(status)) return null;
  return (
    <button className="secondaryAction" type="button" disabled={busy} onClick={() => onAction("dismiss", status)} aria-label={`Dismiss issue #${status.issue}, attempt ${status.attempt}; keep diagnostics`}>
      Dismiss (keep diagnostics)
    </button>
  );
}

function canPermanentlyRemove(status, historical) {
  return historical && ["failed", "orphaned", "cancelled"].includes(status.state) && !status.retryable;
}

function PermanentRemoveButton({ status, onAction, busy, historical }) {
  if (!canPermanentlyRemove(status, historical)) return null;
  return (
    <button className="dangerAction" type="button" disabled={busy} onClick={() => onAction("remove", status)} aria-label={`Permanently remove issue #${status.issue}, attempt ${status.attempt}`}>
      Permanently remove
    </button>
  );
}

function StatusActions({ status, onAction, onInvestigate, onNotice, investigationEnabled, investigationBusy, busy, investigating, readOnly, historical }) {
  const investigationAvailable = investigationEnabled && canInvestigate(status);
  const actionAvailable = attemptActionAvailable(status, historical);
  const planReviewAvailable = canPlanReview(status);
  if (readOnly || (!investigationAvailable && !actionAvailable && !canDismiss(status) && !canPermanentlyRemove(status, historical) && !planReviewAvailable)) return null;
  const action = actionFor(status);
  const label = actionLabel(status, busy);
  return (
    <footer className="cardActions">
      {investigationAvailable ? (
        <button className="primaryAction" type="button" disabled={investigationBusy} onClick={() => onInvestigate(status)}>
          {investigating ? "Requesting…" : "Ask orchestrator to investigate"}
        </button>
      ) : null}
      {planReviewAvailable ? <PlanReviewButton status={status} onNotice={onNotice} /> : null}
      <DismissButton status={status} onAction={onAction} busy={busy} />
      <PermanentRemoveButton status={status} onAction={onAction} busy={busy} historical={historical} />
      {actionAvailable ? (
        <button className={status.state === "completed" ? "secondaryAction" : "dangerAction"} type="button" disabled={busy} onClick={() => onAction(action, status)}>
          {label}
        </button>
      ) : null}
    </footer>
  );
}

export function StatusCard(props) {
  const { status, onOpenTerminal, readOnly, historical } = props;
  const issueURL = githubURL(status.repository, "issues", status.issue);
  const prURL = githubURL(status.repository, "pull", status.pr);
  const issueLabel = status.title ? `#${status.issue} ${status.title}` : `Issue #${status.issue}`;
  const sessions = status.sessions?.length ? status.sessions : status.session ? [{ role: "implementation", name: status.session, state: status.state, current: true }] : [];
  const blockers = status.blockers || [];
  const checks = status.checks || [];
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
          {status.session ? <button className="terminalLink" type="button" disabled={readOnly || historical} onClick={() => onOpenTerminal(status)}><code>{status.session}</code></button> : null}
        </Detail>
        <Detail label="Worktree"><code>{worktreeName(status.worktree)}</code></Detail>
        <Detail label="Branch"><code>{status.branch}</code></Detail>
        <Detail label="Current phase">{status.current_phase}</Detail>
        <Detail label="Session lifecycle">{sessions.map((session) => (
          <span className="line" key={session.role}>
            {session.role === "implementation" ? null : <><button className="terminalLink" type="button" disabled={historical || sessionDisabled(readOnly, session.role)} onClick={() => onOpenTerminal(status, session)} aria-label={`Open ${session.role} terminal`}><code>{session.name}</code></button>{" · "}</>}
            {`${session.role}${session.mode ? ` · ${session.mode}` : ""} · ${session.state}${session.current ? " · current" : ""}`}
            {session.target ? <> {" · target "}<code>{session.target}</code></> : null}
            {session.updated_at ? <> {" · "}<Timestamp value={session.updated_at} /></> : null}
          </span>
        ))}</Detail>
        <Detail label="Blockers">{blockers.map((blocker) => <span className="line" key={blocker}>{blocker}</span>)}</Detail>
        <Detail label="Checks">{checks.map((check) => <span className="line" key={check}>{check}</span>)}</Detail>
        <Detail label="Diagnostic">{status.diagnostic}</Detail>
        <Detail label="Next action">{status.next_action}</Detail>
      </dl>
      <StatusActions {...props} readOnly={readOnly} />
    </article>
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

export function OrchestratorCard({ status, error, busy, onAction, onOpenTerminal }) {
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
    </section>
  );
}
