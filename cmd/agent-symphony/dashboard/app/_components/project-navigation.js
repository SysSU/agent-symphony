import { attemptKey, groupStatusesByLane, overallHealth, partitionAttemptHistory, relativeTime } from "../health.mjs";
import ReconcileButton from "./reconcile-button";
import { OrchestratorCard } from "./status-card";

export function projectView(projects, selectedRepository, localSnapshot, localState, localError) {
  const local = projects.find((project) => project.local);
  const remote = projects.find((project) => project.repository === selectedRepository && !project.local) ?? null;
  return {
    local,
    remote,
    snapshot: remote ? remote.snapshot : localSnapshot,
    state: remote ? remote.state ?? { hidden: [] } : localState,
    error: remote ? remote.error ?? "" : localError,
  };
}

export function projectBoard(view, now) {
  const projected = view.snapshot?.statuses ?? [];
  const hidden = new Set((view.state.hidden ?? []).map(attemptKey));
  const partitioned = partitionAttemptHistory(projected);
  const statuses = partitioned.current.filter((status) => !hidden.has(attemptKey(status)));
  const historical = partitioned.historical.filter((status) => !hidden.has(attemptKey(status)));
  const counts = Object.entries(statuses.reduce((result, status) => {
    result[status.state] = (result[status.state] ?? 0) + 1;
    return result;
  }, {})).sort(([a], [b]) => a.localeCompare(b));
  const title = view.remote?.repository || view.local?.repository || projected[0]?.repository || "Repository dashboard";
  return { projected, statuses, historical, counts, title, lanes: groupStatusesByLane(statuses), health: overallHealth(view.snapshot, view.error, statuses, now) };
}

export default function ProjectNavigation({ projects, remote, onSelect }) {
  if (projects.length < 2) return null;
  return (
    <nav className="projects" aria-label="Project deployments">
      {projects.map((project, index) => (
        <button
          type="button"
          key={`${project.url || "local"}-${index}`}
          aria-pressed={project.local ? !remote : remote?.url === project.url}
          onClick={() => onSelect(project.local ? "" : project.repository)}
          disabled={!project.repository}
        >
          {project.repository || project.url}
        </button>
      ))}
    </nav>
  );
}

export function ProjectHeader({ title, snapshot, release, counts, now }) {
  return (
    <header className="hero">
      <div>
        <p className="eyebrow">Agent Symphony</p>
        <h1>{title}</h1>
        <p className="freshness" aria-live="polite">{snapshot ? `Updated ${relativeTime(snapshot.updated_at, now)}` : "Loading status…"}</p>
        <p className="release" aria-live="polite">{release ? <>Release <code>{release}</code></> : "Loading release…"}</p>
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
  );
}

export function ProjectHealthControl({ remote, onNotice, onSnapshot }) {
  if (remote) return <a href={remote.url}>Open project dashboard</a>;
  return <ReconcileButton onNotice={onNotice} onSnapshot={onSnapshot} />;
}

export function ProjectAgentConsole({ remote, ...props }) {
  if (remote) return <p className="notice">Peer status is read-only here. Open its project dashboard to use terminals or controls.</p>;
  return <OrchestratorCard {...props} />;
}
