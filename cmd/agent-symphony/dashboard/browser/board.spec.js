import { expect, test } from "@playwright/test";

const statuses = [
  {
    repository: "SysSU/agent-symphony",
    issue: 161,
    attempt: 1,
    title: "Show attempt lanes",
    state: "active",
    priority: 1,
    pr: 162,
    session: "as-agent-symphony-161-1",
    worktree: "/attempts/agent-symphony-161-1",
    branch: "issue-161-attempt-lanes",
    blockers: ["Waiting for review"],
    checks: ["npm test: passed"],
    diagnostic: "Worker is healthy",
    next_action: "Review the pull request",
  },
  {
    repository: "SysSU/agent-symphony",
    issue: 160,
    attempt: 2,
    title: "Completed attempt",
    state: "completed",
  },
  {
    repository: "SysSU/agent-symphony",
    issue: 159,
    attempt: 3,
    title: "Unrecognized attempt",
    retryable: true,
  },
];

test("renders every board lane and keeps overflowing lanes keyboard reachable", async ({ page }) => {
  await page.setViewportSize({ width: 1000, height: 900 });
  await page.route("**/orchestrator.json", (route) => route.fulfill({ json: { enabled: true, state: "running", session: "orchestrator" } }));
  await page.route("**/orchestrator/proposal.json", (route) => route.fulfill({ status: 204 }));
  await page.route("**/dashboard-state.json", (route) => route.fulfill({ json: { hidden: [] } }));
  await page.route("**/status.json", (route) => route.fulfill({ json: { updated_at: new Date().toISOString(), statuses } }));
  await page.goto("/");

  const board = page.getByRole("region", { name: "Issue status board" });
  await expect(board).toBeVisible();
  for (const [id, title, count] of [
    ["queue", "Queue", 0],
    ["in-progress", "In progress", 1],
    ["in-review", "In review", 0],
    ["needs-attention", "Needs attention", 1],
    ["done", "Done", 1],
  ]) {
    await expect(page.locator(`#lane-${id}`)).toContainText(title);
    await expect(page.locator(`#lane-${id} .laneCount`)).toHaveText(String(count));
  }

  await expect(board.getByRole("link", { name: "#161 Show attempt lanes" })).toHaveAttribute("href", "https://github.com/SysSU/agent-symphony/issues/161");
  await expect(board.getByRole("link", { name: "PR #162" })).toHaveAttribute("href", "https://github.com/SysSU/agent-symphony/pull/162");
  await expect(board).toContainText("as-agent-symphony-161-1");
  await expect(board).toContainText("agent-symphony-161-1");
  await expect(board).toContainText("issue-161-attempt-lanes");
  await expect(board).toContainText("Waiting for review");
  await expect(board).toContainText("npm test: passed");
  await expect(board).toContainText("Worker is healthy");
  await expect(board).toContainText("Review the pull request");
  await expect(board.locator(".state-unknown")).toHaveText("unknown");
  await expect(board.getByRole("button", { name: "Archive" })).toBeVisible();
  await expect(board.getByRole("button", { name: "Recover attempt" })).toBeVisible();

  expect(await board.evaluate((element) => element.scrollWidth > element.clientWidth)).toBe(true);
  await board.focus();
  await expect(board).toBeFocused();
  await expect(board).toHaveCSS("outline-style", "solid");
  await page.keyboard.press("ArrowRight");
  await expect.poll(() => board.evaluate((element) => element.scrollLeft)).toBeGreaterThan(0);
});
