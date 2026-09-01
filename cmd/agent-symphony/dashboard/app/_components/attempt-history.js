import { StatusCard } from "./status-card";

export default function AttemptHistory({ statuses }) {
  if (!statuses.length) return null;
  return (
    <details className="attemptHistory">
      <summary>Previous attempts <span className="laneCount" aria-label={`${statuses.length} previous attempt${statuses.length === 1 ? "" : "s"}`}>{statuses.length}</span></summary>
      <ol className="historyCards">
        {statuses.map((status) => (
          <li key={`${status.repository}-${status.issue}-${status.attempt}`}>
            <StatusCard status={status} readOnly />
          </li>
        ))}
      </ol>
    </details>
  );
}
