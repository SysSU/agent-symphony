import assert from "node:assert/strict";
import test from "node:test";

import { operatorActionNotice, postWithReconciliationRetry } from "./actions.mjs";

test("dashboard actions submit exactly one mutation request", async (t) => {
  const originalFetch = globalThis.fetch;
  t.after(() => { globalThis.fetch = originalFetch; });

  let requests = 0;
  globalThis.fetch = async () => {
    requests++;
    return { status: 503, text: async () => "mutation was refused" };
  };
  let retries = 0;
  assert.equal((await postWithReconciliationRetry("/actions/orchestrator/investigate", async () => { retries++; })).status, 503);
  assert.equal(requests, 1);
  assert.equal(retries, 0);

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

test("operator action notices distinguish accepted work from completed work", async () => {
  const pending = { status: 202, json: async () => ({ data: { phase: "cleanup-pending" } }) };
  assert.equal(await operatorActionNotice(pending, "Archive", "Archived", 160, 2), "Archive accepted for issue #160, attempt 2. Current phase: cleanup-pending.");
  assert.equal(await operatorActionNotice({ status: 200 }, "Archive", "Archived", 160, 2), "Archived issue #160, attempt 2.");
});
