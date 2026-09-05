import assert from "node:assert/strict";
import test from "node:test";

import { postWithReconciliationRetry } from "./actions.mjs";

test("dashboard actions retry transient reconciliation 503 responses", async (t) => {
  const originalFetch = globalThis.fetch;
  t.after(() => { globalThis.fetch = originalFetch; });

  const responses = [
    { status: 503, text: async () => "reconciliation is in progress" },
    { status: 200 },
  ];
  let requests = 0;
  globalThis.fetch = async () => {
    requests++;
    return responses.shift();
  };
  let retries = 0;
  assert.equal((await postWithReconciliationRetry("/actions/orchestrator/investigate", async () => { retries++; })).status, 200);
  assert.equal(requests, 2);
  assert.equal(retries, 1);

  requests = 0;
  globalThis.fetch = async () => {
    requests++;
    return { status: 500 };
  };
  assert.equal((await postWithReconciliationRetry("/actions/orchestrator/investigate")).status, 500);
  assert.equal(requests, 1);

  let init;
  globalThis.fetch = async (_url, options) => {
    init = options;
    return { status: 204 };
  };
  await postWithReconciliationRetry("/actions/reconcile", undefined, { headers: { "Content-Type": "application/json" }, body: "{}" });
  assert.deepEqual(init, { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });
});
