import { expect, test } from "@playwright/test";

const proposal = {
  version: 1,
  repository: "SysSU/agent-symphony",
  issue: 131,
  attempt: 3,
  message: "Run the focused race test.",
  binding: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
};
const confirmationNonce = "browser-bound-confirmation-nonce";
const browserErrors = new WeakMap();

test.beforeEach(async ({ page }) => {
  const errors = [];
  browserErrors.set(page, errors);
  page.on("console", (message) => {
    if (message.type() === "error") errors.push(`console: ${message.text()}`);
  });
  page.on("pageerror", (error) => errors.push(`page: ${error.message}`));
});

test.afterEach(async ({ page }) => {
  expect(browserErrors.get(page)).toEqual([]);
});

const attempt = {
  repository: proposal.repository,
  issue: proposal.issue,
  attempt: proposal.attempt,
  title: "Queue confirmed worker messages",
  state: "active",
  session: "as-agent-symphony-131-3",
  worktree: "/attempts/agent-symphony-131-3",
  branch: "issue-131-worker-messages",
  operator_messages: [{ id: "message-1234567890", state: "queued" }],
};

async function mockDashboard(page, projectedAttempt = attempt) {
  let currentProposal = proposal;
  let messageState = "queued";
  let messageDiagnostic = "";
  const actions = [];
  const requests = [];
  const directRequests = [];
  await page.route("**/release.json", (route) => route.fulfill({ json: { release: "0.5.0" } }));
  await page.route("**/orchestrator.json", (route) => route.fulfill({ json: { enabled: true, state: "running", session: "orchestrator" } }));
  await page.route("**/orchestrator/proposal.json", (route) => currentProposal
    ? route.fulfill({ json: currentProposal, headers: { "X-Agent-Symphony-Confirmation-Nonce": confirmationNonce } })
    : route.fulfill({ status: 204 }));
  await page.route("**/dashboard-state.json", (route) => route.fulfill({ json: { hidden: [] } }));
  await page.route("**/status.json", (route) => {
    route.fulfill({ json: { updated_at: new Date().toISOString(), statuses: [{ ...projectedAttempt, operator_messages: [{ ...projectedAttempt.operator_messages[0], state: messageState, diagnostic: messageDiagnostic }] }] } });
  });
  await page.route("**/actions/orchestrator/message-*", async (route) => {
    const action = route.request().url().split("message-").pop();
    actions.push(action);
    const body = route.request().postDataJSON();
    requests.push({ action, body, nonce: route.request().headers()["x-agent-symphony-confirmation-nonce"] });
    if (JSON.stringify(body) !== JSON.stringify(currentProposal)) {
      await route.fulfill({ status: 409, body: "orchestrator message proposal changed before confirmation\n" });
      return;
    }
    if (action === "confirm") messageState = "delivered";
    currentProposal = null;
    await route.fulfill({ json: {} });
  });
  await page.route("**/actions/attempt/message?*", async (route) => {
    directRequests.push({ url: route.request().url(), body: route.request().postDataJSON() });
    await route.fulfill({ json: { id: "direct-message", state: "queued" } });
  });
  return {
    actions,
    requests,
    directRequests,
    replaceProposal(next) {
      currentProposal = next;
    },
    setMessageOutcome(state, diagnostic) {
      messageState = state;
      messageDiagnostic = diagnostic;
    },
  };
}

test("queues a direct follow-up for the exact retryable attempt", async ({ page }) => {
	const dashboard = await mockDashboard(page, { ...attempt, state: "blocked", retryable: true });
  await page.goto("/");

  await page.getByLabel("Tell this agent what to do next").fill("Inspect the failing test and continue the fix.");
  await page.getByRole("button", { name: "Queue follow-up" }).click();

  await expect(page.getByRole("status").filter({ hasText: "Queued a follow-up for issue #131, attempt 3" })).toBeVisible();
  expect(dashboard.directRequests).toHaveLength(1);
  expect(dashboard.directRequests[0].url).toContain("/actions/attempt/message?issue=131&attempt=3");
  expect(dashboard.directRequests[0].body).toEqual({ message: "Inspect the failing test and continue the fix." });
  await expect(page.getByLabel("Tell this agent what to do next")).toHaveValue("");
});

test("announces and focuses a proposal, confirms it, and refreshes delivery", async ({ page }) => {
  const dashboard = await mockDashboard(page);
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
  expect(dashboard.actions).toEqual(["confirm"]);
  expect(dashboard.requests).toEqual([{ action: "confirm", body: proposal, nonce: confirmationNonce }]);

  await expect(page.locator(".messageStatus .state-delivered")).toBeVisible({ timeout: 6500 });
});

test("cancels a proposal and keeps the confirmation inside a mobile viewport", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const dashboard = await mockDashboard(page);
  await page.goto("/");

  const confirmation = page.getByRole("region", { name: "Queue a worker message" });
  const box = await confirmation.boundingBox();
  expect(box).not.toBeNull();
  expect(box.x).toBeGreaterThanOrEqual(0);
  expect(box.x + box.width).toBeLessThanOrEqual(390);

  await page.getByRole("button", { name: "Cancel proposal" }).click();
  await expect(page.getByRole("status").filter({ hasText: "Cancelled the orchestrator message proposal" })).toBeVisible();
  await expect(confirmation).toBeHidden();
  expect(dashboard.actions).toEqual(["cancel"]);
  expect(dashboard.requests).toEqual([{ action: "cancel", body: proposal, nonce: confirmationNonce }]);
});

test("keeps the reviewed proposal visible when its binding changes before confirmation", async ({ page }) => {
  const dashboard = await mockDashboard(page);
  await page.goto("/");

  const confirmation = page.getByRole("region", { name: "Queue a worker message" });
  await expect(confirmation).toBeVisible();
  dashboard.replaceProposal({ ...proposal, binding: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789" });

  await page.getByRole("button", { name: "Confirm and queue" }).click();
  await expect(page.getByRole("status").filter({ hasText: "orchestrator message proposal changed before confirmation" })).toBeVisible();
  await expect(confirmation).toBeVisible();
  await expect(page.locator(".messageStatus .state-queued")).toBeVisible();
  expect(dashboard.requests).toEqual([{ action: "confirm", body: proposal, nonce: confirmationNonce }]);
  expect(browserErrors.get(page)).toEqual(["console: Failed to load resource: the server responded with a status of 409 (Conflict)"]);
  browserErrors.set(page, []);
});

test("renders rejected and failed worker message outcomes", async ({ page }) => {
  const dashboard = await mockDashboard(page);
  await page.goto("/");

  dashboard.setMessageOutcome("rejected", "attempt completed before delivery");
  await expect(page.locator(".messageStatus .state-rejected")).toBeVisible({ timeout: 6500 });
  await expect(page.locator(".messageStatus")).toContainText("attempt completed before delivery");

  dashboard.setMessageOutcome("failed", "attempt runtime failed before delivery");
  await expect(page.locator(".messageStatus .state-failed")).toBeVisible({ timeout: 6500 });
  await expect(page.locator(".messageStatus")).toContainText("attempt runtime failed before delivery");
});
