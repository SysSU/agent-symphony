export async function postWithReconciliationRetry(url, onRetry = () => new Promise((resolve) => setTimeout(resolve, 1000)), options = {}) {
  for (let retries = 0; ; retries++) {
    const response = await fetch(url, { ...options, method: "POST" });
    if (response.status !== 503 || retries === 9) return response;
    await response.text();
    await onRetry();
  }
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
