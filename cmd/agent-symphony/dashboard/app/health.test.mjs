import assert from "node:assert/strict";
import test from "node:test";

import { canInvestigate, groupStatusesByLane, orchestratorPresentation, overallHealth } from "./health.mjs";

const now = new Date("2026-08-13T12:00:00Z").getTime();
const fresh = { updated_at: "2026-08-13T11:59:30Z" };

test("overall dashboard health", () => {
  assert.equal(overallHealth(fresh, "", [], now).state, "good");
  for (const status of [{ state: "blocked" }, { state: "cancelled" }, { state: "future-state" }, { state: "active", blockers: ["waiting"] }, { state: "active", diagnostic: "worker stopped" }]) {
    assert.equal(overallHealth(fresh, "", [status], now).state, "attention");
  }
  assert.equal(overallHealth(fresh, "", [{ state: "completed", diagnostic: "old" }], now).state, "good");
  assert.equal(overallHealth({ updated_at: "2026-08-13T11:57:59Z" }, "", [], now).state, "stale");
  assert.equal(overallHealth(fresh, "Status request failed", [], now).state, "unavailable");
});

test("groups every status into one ordered dashboard lane", () => {
  const statuses = [
    "queued", "runnable", "active", "review-ready", "blocked", "failed", "conflicting", "orphaned", "cancelled", "completed", "future-state",
  ].map((state, issue) => ({ issue, state }));
  const lanes = groupStatusesByLane(statuses);

  assert.deepEqual(Object.fromEntries(lanes.map((lane) => [lane.title, lane.statuses.map(({ state }) => state)])), {
    Queue: ["queued", "runnable"],
    "In progress": ["active"],
    "In review": ["review-ready"],
    "Needs attention": ["blocked", "failed", "conflicting", "orphaned", "cancelled", "future-state"],
    Done: ["completed"],
  });
  assert.deepEqual(lanes.flatMap((lane) => lane.statuses), statuses);
  assert.deepEqual(groupStatusesByLane([]).map((lane) => lane.statuses.length), [0, 0, 0, 0, 0]);
});

test("orchestrator presentation and investigation eligibility", () => {
  assert.deepEqual(orchestratorPresentation(null, "offline"), { state: "unavailable", label: "Unavailable" });
  assert.deepEqual(orchestratorPresentation({ enabled: false, state: "disabled" }, ""), { state: "disabled", label: "Disabled" });
  assert.deepEqual(orchestratorPresentation({ enabled: true, state: "starting" }, ""), { state: "recovering", label: "Recovering" });
  assert.deepEqual(orchestratorPresentation({ enabled: true, state: "degraded" }, ""), { state: "failed", label: "Failed" });
  assert.deepEqual(orchestratorPresentation({ enabled: true, state: "running" }, ""), { state: "running", label: "Running" });

  for (const state of ["blocked", "failed", "conflicting", "orphaned"]) assert.equal(canInvestigate({ state }), true);
  for (const state of ["active", "completed", "queued"]) assert.equal(canInvestigate({ state }), false);
});
