import assert from "node:assert/strict";
import test from "node:test";

import { canInvestigate, groupStatusesByLane, orchestratorPresentation, overallHealth, ownerVersionAtLeast, partitionAttemptHistory } from "./health.mjs";

const now = new Date("2026-08-13T12:00:00Z").getTime();
const fresh = { updated_at: "2026-08-13T11:59:30Z" };

test("overall dashboard health", () => {
  assert.equal(overallHealth(fresh, "", [], now).state, "good");
  for (const status of [{ state: "blocked" }, { state: "cancelled" }, { state: "future-state" }, { state: "active", blockers: ["waiting"] }, { state: "active", diagnostic: "worker stopped" }]) {
    assert.equal(overallHealth(fresh, "", [status], now).state, "attention");
  }
  assert.equal(overallHealth(fresh, "", [{ state: "active", needs_attention: true }, { state: "completed", needs_attention: true }], now).state, "attention");
  assert.equal(overallHealth(fresh, "", [{ state: "completed", diagnostic: "old" }], now).state, "good");
  assert.equal(overallHealth({ updated_at: "2026-08-13T11:57:59Z" }, "", [], now).state, "stale");
  assert.deepEqual(overallHealth({ ...fresh, reconciliation_error: "GitHub refresh timed out" }, "", [], now), { state: "stale", title: "Status refresh failed", detail: "GitHub refresh timed out" });
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
  assert.deepEqual(lanes.flatMap((lane) => lane.statuses).map(({ issue }) => issue).sort((a, b) => a - b), statuses.map(({ issue }) => issue));
  assert.deepEqual(groupStatusesByLane([]).map((lane) => lane.statuses.length), [0, 0, 0, 0, 0]);
  assert.deepEqual(groupStatusesByLane([{ issue: 218, state: "active", needs_attention: true }]).map((lane) => lane.statuses.length), [0, 0, 0, 1, 0]);
});

test("partitions superseded terminal attempts by repository and issue", () => {
  const statuses = [
    { repository: "o/r", issue: 187, attempt: 1, state: "failed" },
    { repository: "o/r", issue: 187, attempt: 2, state: "review-ready" },
    { repository: "o/r", issue: 188, attempt: 1, state: "orphaned" },
    { repository: "o/r", issue: 188, attempt: 2, state: "active" },
    { repository: "x/r", issue: 187, attempt: 1, state: "cancelled" },
    { repository: "o/r", issue: 189, attempt: 1, state: "failed" },
    { repository: "o/r", issue: 190, attempt: 1, state: "cancelled" },
    { repository: "o/r", issue: 190, attempt: 2, state: "queued" },
  ];

  const partitioned = partitionAttemptHistory(statuses);
  assert.deepEqual(partitioned.historical, [statuses[0], statuses[2], statuses[6]]);
  assert.deepEqual(partitioned.current, [statuses[1], statuses[3], statuses[4], statuses[5], statuses[7]]);
});

test("hidden highest attempts do not promote older terminal attempts", () => {
  for (const reason of ["abandoned", "dismissed", "archived", "removed"]) {
    for (const state of ["failed", "orphaned", "cancelled", "completed"]) {
      const older = { repository: "o/r", issue: 191, attempt: 8, state };
      const hidden = { repository: "o/r", issue: 191, attempt: 9, reason };
      const partitioned = partitionAttemptHistory([older], [hidden]);
      assert.deepEqual(partitioned.current, [], `${reason} after ${state}`);
      assert.deepEqual(partitioned.historical, [older], `${reason} after ${state}`);
      const newer = { repository: "o/r", issue: 191, attempt: 10, state: "active" };
      assert.deepEqual(partitionAttemptHistory([older, newer], [hidden]).current, [newer], `${reason} permits a new retry`);
    }
  }
});

test("owner-backed refreshes never replace a newer committed state", () => {
  assert.equal(ownerVersionAtLeast({ owner_revision: 9 }, { owner_revision: 10 }), false);
  assert.equal(ownerVersionAtLeast({ owner_revision: 10 }, { owner_revision: 10 }), true);
  assert.equal(ownerVersionAtLeast({ owner_epoch: 2, owner_revision: 1 }, { owner_epoch: 1, owner_revision: 10 }), true);
  assert.equal(ownerVersionAtLeast({ owner_epoch: 1, owner_revision: 20 }, { owner_epoch: 2, owner_revision: 1 }), false);
  assert.equal(ownerVersionAtLeast({}, { owner_revision: 10 }), false);
  assert.equal(ownerVersionAtLeast({}, {}), true);
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
