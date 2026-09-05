const staleAfter = 2 * 60 * 1000;
const attentionStates = new Set(["blocked", "failed", "conflicting", "orphaned"]);
const historicalStates = new Set(["failed", "orphaned", "cancelled"]);
const laneDefinitions = [
  { id: "queue", title: "Queue", states: ["runnable", "queued"] },
  { id: "in-progress", title: "In progress", states: ["active"] },
  { id: "in-review", title: "In review", states: ["review-ready"] },
  { id: "needs-attention", title: "Needs attention", states: ["blocked", "failed", "conflicting", "orphaned", "cancelled"] },
  { id: "done", title: "Done", states: ["completed"] },
];
const laneByState = new Map(laneDefinitions.flatMap((lane, index) => lane.states.map((state) => [state, index])));

export function relativeTime(value, now) {
  const seconds = Math.max(0, Math.floor((now - new Date(value).getTime()) / 1000));
  if (seconds < 5) return "just now";
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  return `${Math.floor(minutes / 60)}h ago`;
}

export function attemptKey(status) {
  return `${status.repository}#${status.issue}/${status.attempt}`;
}

export function partitionAttemptHistory(statuses) {
  const latest = new Map();
  for (const status of statuses) {
    if (!status.repository || !Number.isInteger(status.issue) || !Number.isInteger(status.attempt)) continue;
    const key = `${status.repository}#${status.issue}`;
    latest.set(key, Math.max(latest.get(key) ?? 0, status.attempt));
  }
  return statuses.reduce((result, status) => {
    const key = `${status.repository}#${status.issue}`;
    const historical = historicalStates.has(status.state) && Number.isInteger(status.attempt) && status.attempt < (latest.get(key) ?? status.attempt);
    result[historical ? "historical" : "current"].push(status);
    return result;
  }, { current: [], historical: [] });
}

export function groupStatusesByLane(statuses) {
  const lanes = laneDefinitions.map(({ id, title }) => ({ id, title, statuses: [] }));
  for (const status of statuses) lanes[status.needs_attention ? 3 : laneByState.get(status.state) ?? 3].statuses.push(status);
  return lanes;
}

export function canInvestigate(status) {
  return attentionStates.has(status?.state);
}

export function orchestratorPresentation(status, error) {
  if (error) return { state: "unavailable", label: "Unavailable" };
  if (!status) return { state: "loading", label: "Loading" };
  if (!status.enabled || status.state === "disabled") return { state: "disabled", label: "Disabled" };
  if (status.state === "starting") return { state: "recovering", label: "Recovering" };
  if (status.state === "degraded") return { state: "failed", label: "Failed" };
  return { state: status.state, label: status.state === "running" ? "Running" : status.state };
}

export function overallHealth(snapshot, error, statuses, now) {
  if (error) return { state: "unavailable", title: "Agent Symphony is unavailable", detail: error };
  if (!snapshot) return { state: "loading", title: "Checking Agent Symphony", detail: "Waiting for status…" };

  const updatedAt = new Date(snapshot.updated_at).getTime();
  if (!Number.isFinite(updatedAt) || now - updatedAt > staleAfter) {
    return { state: "stale", title: "Status updates are stale", detail: "No fresh status has been posted for more than two minutes." };
  }

  const attention = statuses.filter((status) => status.needs_attention || status.state !== "completed" && (
    (laneByState.get(status.state) ?? 3) === 3 || status.blockers?.length || status.diagnostic
  )).length;
  if (attention) {
    return {
      state: "attention",
      title: "Agent Symphony needs attention",
      detail: `${attention} visible attempt${attention === 1 ? "" : "s"} need attention. See the status cards below.`,
    };
  }

  return { state: "good", title: "Everything is okay", detail: "Status is current and no visible attempts need attention." };
}
