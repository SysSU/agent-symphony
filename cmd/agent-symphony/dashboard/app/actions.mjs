export async function postWithReconciliationRetry(url, onRetry = () => new Promise((resolve) => setTimeout(resolve, 1000)), options = {}) {
  let response;
  do {
    response = await fetch(url, { ...options, method: "POST" });
    if (response.status === 503) {
      await response.text();
      await onRetry();
    }
  } while (response.status === 503);
  return response;
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

export async function getMessageProposal() {
  try {
    const response = await fetch("/orchestrator/proposal.json", { cache: "no-store" });
    if (response.status === 204) return { proposal: null, error: "" };
    if (!response.ok) return { proposal: null, error: (await response.text()).trim() || `Message proposal failed (${response.status})` };
    return { proposal: { ...await response.json(), confirmationNonce: response.headers.get("X-Agent-Symphony-Confirmation-Nonce") }, error: "" };
  } catch {
    return { proposal: null, error: "Message proposal is unavailable" };
  }
}
