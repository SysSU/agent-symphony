const staleAfter = 2 * 60 * 1000;
const attentionStates = new Set(["blocked", "failed", "conflicting", "orphaned"]);

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

  const attention = statuses.filter((status) => status.state !== "completed" && (
    attentionStates.has(status.state) || status.blockers?.length || status.diagnostic
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
