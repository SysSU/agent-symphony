export async function postWithReconciliationRetry(url, _onRetry, options = {}) {
  return fetch(url, { ...options, method: "POST" });
}

export async function operatorActionNotice(response, verb, finished, issue, attempt) {
  if (response.status !== 202) return `${finished} issue #${issue}, attempt ${attempt}.`;
  let phase = "";
  try {
    phase = (await response.json())?.data?.phase ?? "";
  } catch {
    // Admission is durable even when an older server omits the phase body.
  }
  const progress = phase ? ` Current phase: ${phase}.` : " Work is in progress.";
  return `${verb} accepted for issue #${issue}, attempt ${attempt}.${progress}`;
}

export async function getOrchestratorStatus() {
  try {
    const response = await fetch("/orchestrator.json", { cache: "no-store" });
    if (response.ok) return { status: await response.json(), error: "" };
    return { status: null, error: (await response.text()).trim() || `Orchestrator status failed (${response.status})` };
  } catch {
    return { status: null, error: "Orchestrator status is unavailable" };
  }
}

export async function getRelease() {
  try {
    const response = await fetch("/release.json", { cache: "no-store" });
    if (!response.ok) return "unavailable";
    const metadata = await response.json();
    return typeof metadata.release === "string" && metadata.release ? metadata.release : "unavailable";
  } catch {
    return "unavailable";
  }
}
