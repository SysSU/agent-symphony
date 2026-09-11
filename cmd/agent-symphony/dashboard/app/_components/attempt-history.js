import { StatusCard } from "./status-card";

export default function AttemptHistory({ statuses, onAction, busy, readOnly }) {
  if (!statuses.length) return null;
  return (
    <details className="attemptHistory">
      <summary>Previous attempts <span className="laneCount" aria-label={`${statuses.length} previous attempt${statuses.length === 1 ? "" : "s"}`}>{statuses.length}</span></summary>
      <ol className="historyCards">
        {statuses.map((status) => (
          <li key={`${status.repository}-${status.issue}-${status.attempt}`}>
            <StatusCard status={status} historical readOnly={readOnly} onAction={onAction} busy={busy === `${status.repository}#${status.issue}/${status.attempt}`} />
          </li>
        ))}
      </ol>
    </details>
  );
}
