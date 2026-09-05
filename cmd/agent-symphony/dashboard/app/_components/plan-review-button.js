import { useState } from "react";

import { postWithReconciliationRetry } from "../actions.mjs";

export default function PlanReviewButton({ status, onNotice }) {
  const [busy, setBusy] = useState(false);
  async function start() {
    setBusy(true);
    onNotice("");
    try {
      const query = new URLSearchParams({ repository: status.repository, issue: String(status.issue), attempt: String(status.attempt) });
      const response = await postWithReconciliationRetry(`/actions/review-plan?${query}`);
      if (!response.ok) throw new Error((await response.text()).trim() || `Plan review failed (${response.status})`);
      onNotice(`Started plan review for issue #${status.issue}, attempt ${status.attempt}.`);
    } catch (reason) {
      onNotice(reason instanceof Error ? reason.message : "Plan review failed.");
    } finally {
      setBusy(false);
    }
  }
  return <button className="secondaryAction" type="button" disabled={busy} onClick={start}>{busy ? "Starting plan review…" : "Start plan review"}</button>;
}
