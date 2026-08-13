import assert from "node:assert/strict";
import test from "node:test";

import { overallHealth } from "./health.mjs";

const now = new Date("2026-08-13T12:00:00Z").getTime();
const fresh = { updated_at: "2026-08-13T11:59:30Z" };

test("overall dashboard health", () => {
  assert.equal(overallHealth(fresh, "", [], now).state, "good");
  for (const status of [{ state: "blocked" }, { state: "active", blockers: ["waiting"] }, { state: "active", diagnostic: "worker stopped" }]) {
    assert.equal(overallHealth(fresh, "", [status], now).state, "attention");
  }
  assert.equal(overallHealth(fresh, "", [{ state: "completed", diagnostic: "old" }], now).state, "good");
  assert.equal(overallHealth({ updated_at: "2026-08-13T11:57:59Z" }, "", [], now).state, "stale");
  assert.equal(overallHealth(fresh, "Status request failed", [], now).state, "unavailable");
});
