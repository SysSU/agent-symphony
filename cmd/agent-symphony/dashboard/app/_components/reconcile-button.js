"use client";

import { useState } from "react";

const wait = () => new Promise((resolve) => setTimeout(resolve, 1000));

export default function ReconcileButton({ onNotice, onSnapshot }) {
  const [checking, setChecking] = useState(false);

  async function checkNow() {
    setChecking(true);
    onNotice("");
    try {
      let response;
      do {
        response = await fetch("/actions/reconcile", { method: "POST" });
        if (response.status === 503) {
          await response.text();
          await wait();
        }
      } while (response.status === 503);
      if (!response.ok) throw new Error((await response.text()).trim() || `Reconciliation failed (${response.status})`);
      const status = await fetch("/status.json", { cache: "no-store" });
      if (!status.ok) throw new Error(`Reconciliation completed, but status refresh failed (${status.status})`);
      onSnapshot(await status.json());
      onNotice("Reconciliation completed.");
    } catch (reason) {
      onNotice(reason instanceof Error ? reason.message : "Reconciliation failed.");
    } finally {
      setChecking(false);
    }
  }

  return (
    <button className="checkNow" type="button" disabled={checking} onClick={checkNow}>
      {checking ? "Checking…" : "Check now"}
    </button>
  );
}
