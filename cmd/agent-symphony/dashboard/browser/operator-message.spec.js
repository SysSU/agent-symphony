import { expect, test } from "@playwright/test";

const proposal = {
  version: 1,
  repository: "SysSU/agent-symphony",
  issue: 131,
  attempt: 3,
  message: "Run the focused race test.",
};

const attempt = {
  repository: proposal.repository,
  issue: proposal.issue,
  attempt: proposal.attempt,
  title: "Queue confirmed worker messages",
  state: "running",
  session: "as-agent-symphony-131-3",
  worktree: "/attempts/agent-symphony-131-3",
  branch: "issue-131-worker-messages",
  operator_messages: [{ id: "message-1234567890", state: "queued" }],
};

async function mockDashboard(page) {
  let currentProposal = proposal;
  let messageState = "queued";
  const actions = [];
  await page.route("**/orchestrator.json", (route) => route.fulfill({ json: { enabled: true, state: "running", session: "orchestrator" } }));
  await page.route("**/orchestrator/proposal.json", (route) => currentProposal
    ? route.fulfill({ json: currentProposal })
    : route.fulfill({ status: 204 }));
  await page.route("**/dashboard-state.json", (route) => route.fulfill({ json: { hidden: [] } }));
  await page.route("**/status.json", (route) => {
    route.fulfill({ json: { updated_at: new Date().toISOString(), statuses: [{ ...attempt, operator_messages: [{ ...attempt.operator_messages[0], state: messageState }] }] } });
  });
  await page.route("**/actions/orchestrator/message-*", async (route) => {
    const action = route.request().url().split("message-").pop();
    actions.push(action);
    if (action === "confirm") messageState = "delivered";
    currentProposal = null;
    await route.fulfill({ json: {} });
  });
  return actions;
}

test("announces and focuses a proposal, confirms it, and refreshes delivery", async ({ page }) => {
  const actions = await mockDashboard(page);
  await page.goto("/");

  const confirmation = page.getByRole("region", { name: "Queue a worker message" });
  await expect(confirmation).toBeVisible();
  await expect(confirmation).toBeFocused();
  await expect(confirmation).toHaveAttribute("aria-live", "assertive");
  await expect(page.getByLabel("Exact proposed worker message")).toHaveText(proposal.message);
  await expect(page.locator(".messageStatus .state-queued")).toBeVisible();

  await page.keyboard.press("Tab");
  await expect(page.getByRole("button", { name: "Confirm and queue" })).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(page.getByRole("status").filter({ hasText: "Queued the confirmed message" })).toBeVisible();
  expect(actions).toEqual(["confirm"]);

  await expect(page.locator(".messageStatus .state-delivered")).toBeVisible({ timeout: 6500 });
});

test("cancels a proposal and keeps the confirmation inside a mobile viewport", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const actions = await mockDashboard(page);
  await page.goto("/");

  const confirmation = page.getByRole("region", { name: "Queue a worker message" });
  const box = await confirmation.boundingBox();
  expect(box).not.toBeNull();
  expect(box.x).toBeGreaterThanOrEqual(0);
  expect(box.x + box.width).toBeLessThanOrEqual(390);

  await page.getByRole("button", { name: "Cancel proposal" }).click();
  await expect(page.getByRole("status").filter({ hasText: "Cancelled the orchestrator message proposal" })).toBeVisible();
  await expect(confirmation).toBeHidden();
  expect(actions).toEqual(["cancel"]);
});
