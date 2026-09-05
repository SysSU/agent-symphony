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

const browserErrors = new WeakMap();

test.beforeEach(async ({ page }) => {
  const errors = [];
  browserErrors.set(page, errors);
  page.on("console", (message) => {
    if (message.type() === "error") errors.push(`console: ${message.text()}`);
  });
  page.on("pageerror", (error) => errors.push(`page: ${error.message}`));
  await page.route("**/release.json", (route) => route.fulfill({ json: { release: "0.5.0" } }));
});

test.afterEach(async ({ page }) => {
  expect(browserErrors.get(page)).toEqual([]);
});

async function mockDashboard(page, attempts, orchestrator = { enabled: true, state: "running", session: "orchestrator" }) {
  let dashboardState = { version: 1, hidden: [] };
  const snapshot = () => ({ updated_at: new Date().toISOString(), statuses: attempts });
  await page.route("**/orchestrator.json", (route) => route.fulfill({ json: orchestrator }));
  await page.route("**/orchestrator/proposal.json", (route) => route.fulfill({ status: 204 }));
  await page.route("**/dashboard-state.json", (route) => route.fulfill({ json: dashboardState }));
  await page.route("**/status.json", (route) => route.fulfill({ json: snapshot() }));
  await page.route("**/projects.json", (route) => route.fulfill({ json: { version: 1, projects: [{ version: 1, repository: attempts[0]?.repository || "SysSU/agent-symphony", local: true, snapshot: snapshot(), state: dashboardState }] } }));
  return {
    hide(status, reason) {
      dashboardState = {
        version: 1,
        hidden: [{ repository: status.repository, issue: status.issue, attempt: status.attempt, reason }],
      };
    },
  };
}

test("shows loading then unavailable when the initial status request fails", async ({ page }) => {
  let releaseStatus;
  const statusPending = new Promise((resolve) => { releaseStatus = resolve; });
  await page.route("**/orchestrator.json", (route) => route.fulfill({ json: { enabled: true, state: "running", session: "orchestrator" } }));
  await page.route("**/orchestrator/proposal.json", (route) => route.fulfill({ status: 204 }));
  await page.route("**/dashboard-state.json", (route) => route.fulfill({ json: { version: 1, hidden: [] } }));
  await page.route("**/projects.json", (route) => route.fulfill({ json: { version: 1, projects: [{ version: 1, repository: "SysSU/agent-symphony", local: true }] } }));
  await page.route("**/status.json", async (route) => {
    await statusPending;
    await route.fulfill({ status: 500, body: "status unavailable" });
  });
  await page.goto("/");

  const board = page.getByRole("region", { name: "Issue status board" });
  await expect(board).toContainText("Loading issue status board…");
  await expect(board.getByText("No attempts")).toHaveCount(0);

  releaseStatus();
  await expect(board).toContainText("Issue status board unavailable.");
  await expect(board.locator(".lane")).toHaveCount(0);
  const errors = browserErrors.get(page);
  expect(errors.length).toBeGreaterThan(0);
  expect(new Set(errors)).toEqual(new Set(["console: Failed to load resource: the server responded with a status of 500 (Internal Server Error)"]));
  browserErrors.set(page, []);
});

test("renders every board lane and keeps overflowing lanes keyboard reachable", async ({ page }) => {
  await page.setViewportSize({ width: 1000, height: 900 });
  await mockDashboard(page, statuses);
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
  await page.keyboard.press("ArrowLeft");
  await expect.poll(() => board.evaluate((element) => element.scrollLeft)).toBe(0);
});

test("provides the accessible web dashboard on desktop and mobile", async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 900 });
  await mockDashboard(page, statuses);
  await page.goto("/");

  await expect(page.getByRole("main")).toBeVisible();
  await expect(page.getByRole("heading", { level: 1, name: statuses[0].repository })).toBeVisible();
  await expect(page.getByRole("region", { name: "Issue status board" })).toBeVisible();
  const release = page.locator(".release");
  await expect(release).toBeVisible();
  await expect(release).toHaveText("Release 0.5.0");
  await expect(release).toHaveAttribute("aria-live", "polite");
  await page.screenshot({ path: "test-results/web-dashboard-desktop.png", fullPage: true });

  await page.setViewportSize({ width: 390, height: 844 });
  await expect(release).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
  await page.screenshot({ path: "test-results/web-dashboard-mobile.png", fullPage: true });
});

test("switches between two isolated project deployments without exposing peer controls", async ({ page }) => {
  const local = statuses[0];
  const peer = { ...statuses[0], repository: "SysSU/second-project", issue: 7, title: "Peer work", session: "as-second-project-7-1", worktree: "/second/worktrees/7-1" };
  const snapshot = (status) => ({ updated_at: new Date().toISOString(), statuses: [status] });
  await mockDashboard(page, [local]);
  await page.route("**/projects.json", (route) => route.fulfill({ json: {
    version: 1,
    projects: [
      { version: 1, repository: local.repository, local: true, snapshot: snapshot(local), state: { version: 1, hidden: [] } },
      { version: 1, repository: peer.repository, url: "http://127.0.0.1:8082", snapshot: snapshot(peer), state: { version: 1, hidden: [] } },
    ],
  } }));
  await page.goto("/");

  const projects = page.getByRole("navigation", { name: "Project deployments" });
  await expect(projects.getByRole("button", { name: local.repository })).toHaveAttribute("aria-pressed", "true");
  await projects.getByRole("button", { name: peer.repository }).click();
  await expect(page.getByRole("heading", { level: 1, name: peer.repository })).toBeVisible();
  await expect(page.getByRole("link", { name: "#7 Peer work" })).toBeVisible();
  await expect(page.getByText(peer.session, { exact: true })).toBeVisible();
  await expect(page.getByText("agent-symphony-161-1", { exact: true })).toHaveCount(0);
  await expect(page.getByRole("link", { name: "Open project dashboard" })).toHaveAttribute("href", "http://127.0.0.1:8082");
  await expect(page.getByRole("button", { name: /archive|recover|abandon|open terminal|queue follow-up/i })).toHaveCount(0);
  await expect(page.getByText("Peer status is read-only here.")).toBeVisible();

  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
});

test("shows implementation needs-attention and the later review clear", async ({ page }) => {
  const status = {
    repository: "SysSU/agent-symphony",
    issue: 218,
    attempt: 2,
    title: "Direct agent status",
    state: "active",
    needs_attention: true,
    blockers: ["needs attention: implementation needs an operator decision"],
  };
  const historical = {
    ...status,
    attempt: 1,
    state: "failed",
    needs_attention: false,
    blockers: ["attempt 1 failed before retry"],
  };
  await page.setViewportSize({ width: 1280, height: 900 });
  await mockDashboard(page, [historical, status]);
  await page.goto("/");

  const attention = page.locator(".lane").filter({ has: page.locator("#lane-needs-attention") });
  await expect(attention.getByRole("link", { name: "#218 Direct agent status" })).toBeVisible();
  await expect(attention.getByText("needs attention", { exact: true })).toBeVisible();
  await expect(attention.getByText("needs attention: implementation needs an operator decision", { exact: true })).toBeVisible();
  const history = page.locator("details.attemptHistory");
  await history.locator("summary").click();
  await expect(history).toContainText("Attempt 1");
  await expect(history).toContainText("attempt 1 failed before retry");
  await expect(history.getByText("needs attention", { exact: true })).toHaveCount(0);
  await expect(history.getByText("needs attention: implementation needs an operator decision", { exact: true })).toHaveCount(0);
  await page.screenshot({ path: "test-results/direct-status-attention-desktop.png", fullPage: true });

  Object.assign(status, { state: "review-ready", needs_attention: false, blockers: [] });
  await page.reload();
  await expect(page.locator("#lane-needs-attention .laneCount")).toHaveText("0");
  await expect(page.locator("#lane-in-review .laneCount")).toHaveText("1");
  await expect(page.getByText("needs attention", { exact: true })).toHaveCount(0);
  await expect(page.locator("details.attemptHistory")).toContainText("attempt 1 failed before retry");

  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
  await page.screenshot({ path: "test-results/direct-status-cleared-mobile.png", fullPage: true });
});

test("refreshes the running release without reloading", async ({ page }) => {
  let release = "0.5.0";
  await page.clock.install();
  await page.route("**/release.json", (route) => route.fulfill({ json: { release } }));
  await mockDashboard(page, statuses);
  await page.goto("/");

  const metadata = page.locator(".release");
  await expect(metadata).toHaveText("Release 0.5.0");
  release = "0.6.0";
  await page.clock.fastForward(5000);
  await expect(metadata).toHaveText("Release 0.6.0");
});

test("moves superseded terminal attempts into read-only history", async ({ page }) => {
  const failed = {
    repository: "SysSU/agent-symphony",
    issue: 187,
    attempt: 1,
    title: "Show the release version",
    state: "failed",
    session: "as-agent-symphony-187-1",
    diagnostic: "checkout base failed",
  };
  const current = { ...failed, attempt: 2, state: "review-ready", session: "", diagnostic: "", pr: 196 };
  await mockDashboard(page, [failed, current]);
  await page.goto("/");

  const board = page.getByRole("region", { name: "Issue status board" });
  await expect(page.getByRole("heading", { name: "Everything is okay" })).toBeVisible();
  await expect(page.locator("#lane-needs-attention .laneCount")).toHaveText("0");
  await expect(page.locator("#lane-in-review .laneCount")).toHaveText("1");
  await expect(page.locator(".count span")).toHaveText("review-ready");
  await expect(board).not.toContainText("checkout base failed");

  const history = page.locator("details.attemptHistory");
  await expect(history.getByText("Previous attempts")).toBeVisible();
  expect(await history.evaluate((element) => element.open)).toBe(false);
  await history.locator("summary").click();
  await expect(history).toContainText("Attempt 1");
  await expect(history).toContainText("checkout base failed");
  await expect(history.getByRole("button")).toBeDisabled();
  await expect(history.getByRole("button", { name: /investigate|recover|abandon/i })).toHaveCount(0);
  await page.screenshot({ path: "test-results/board-attempt-history-desktop.png", fullPage: true });
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
  await page.screenshot({ path: "test-results/board-attempt-history-mobile.png", fullPage: true });
});

test("contains long attempt text inside a mobile viewport", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const longText = "unbrokenattemptdetail".repeat(18);
  const longState = `unknown-${longText}`;
  await mockDashboard(page, [{
    repository: `SysSU/${longText}`,
    issue: 201,
    attempt: 4,
    title: longText,
    state: longState,
    priority: 2,
    session: `as-${longText}`,
    worktree: `/attempts/${longText}`,
    branch: longText,
    blockers: [longText],
    checks: [longText],
    diagnostic: longText,
    next_action: longText,
  }]);
  await page.goto("/");

  const board = page.getByRole("region", { name: "Issue status board" });
  const card = board.locator(".card");
  await expect(card).toBeVisible();
  await expect(page.locator(".count span")).toHaveText(longState);
  await expect(card.locator(".state")).toHaveText(longState);
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
  expect(await board.evaluate((element) => element.scrollWidth)).toBeLessThanOrEqual(await board.evaluate((element) => element.clientWidth));
  const box = await card.boundingBox();
  expect(box).not.toBeNull();
  expect(box.x).toBeGreaterThanOrEqual(0);
  expect(box.x + box.width).toBeLessThanOrEqual(390);
  await page.screenshot({ path: "test-results/board-mobile-long-state.png", fullPage: true });
});

test("opens attempt and orchestrator terminals, closes them, and restores focus", async ({ page }) => {
  const terminalMessages = [];
  await page.routeWebSocket((url) => url.pathname.endsWith("/terminal"), (socket) => {
    socket.onMessage((message) => terminalMessages.push(message));
  });
  await mockDashboard(page, [statuses[0]]);
  await page.goto("/");

  const attemptTerminal = page.getByRole("button", { name: statuses[0].session });
  await attemptTerminal.click();
  let dialog = page.getByRole("dialog", { name: statuses[0].session });
  await expect(dialog).toBeVisible();
  await expect(dialog.getByRole("status")).toHaveText("Connected");
  await expect(dialog.locator(".terminal textarea")).toBeFocused();
  await page.keyboard.press("Escape");
  await expect.poll(() => terminalMessages.some((message) => Buffer.isBuffer(message) && message.equals(Buffer.from([0x1b])))).toBe(true);
  await expect(dialog).toBeVisible();
  await dialog.getByRole("button", { name: "Close" }).focus();
  await page.keyboard.press("Escape");
  await expect(dialog).toBeHidden();
  await expect(attemptTerminal).toBeFocused();

  const orchestratorTerminal = page.getByRole("button", { name: "Open terminal" });
  await orchestratorTerminal.click();
  dialog = page.getByRole("dialog", { name: "orchestrator" });
  await expect(dialog).toBeVisible();
  await dialog.getByRole("button", { name: "Close" }).click();
  await expect(dialog).toBeHidden();
  await expect(orchestratorTerminal).toBeFocused();
});

test("disables unavailable terminal and investigation controls", async ({ page }) => {
  const blocked = { ...statuses[0], issue: 202, state: "blocked" };
  await mockDashboard(page, [blocked], { enabled: true, state: "starting", session: "orchestrator" });
  await page.goto("/");

  const orchestrator = page.getByRole("region", { name: "Orchestrator" });
  await expect(orchestrator.getByRole("button", { name: "orchestrator" })).toBeDisabled();
  await expect(orchestrator.getByRole("button", { name: "Open terminal" })).toBeDisabled();
  await expect(page.getByRole("button", { name: "Ask orchestrator to investigate" })).toHaveCount(0);
});

test("requests an investigation and disables conflicting controls while it is pending", async ({ page }) => {
  const blocked = { ...statuses[0], issue: 203, attempt: 2, state: "blocked" };
  let releaseAction;
  const actionPending = new Promise((resolve) => { releaseAction = resolve; });
  const requests = [];
  await mockDashboard(page, [blocked]);
  await page.route("**/actions/orchestrator/investigate?*", async (route) => {
    requests.push(route.request());
    await actionPending;
    await route.fulfill({ json: {} });
  });
  await page.goto("/");

  const investigate = page.getByRole("button", { name: "Ask orchestrator to investigate" });
  await investigate.click();
  await expect(page.getByRole("button", { name: "Requesting…" })).toBeDisabled();
  await expect(page.getByRole("button", { name: "Recover/restart" })).toBeDisabled();
  await expect.poll(() => requests.length).toBe(1);
  expect(new URL(requests[0].url()).searchParams.get("issue")).toBe(String(blocked.issue));
  expect(new URL(requests[0].url()).searchParams.get("attempt")).toBe(String(blocked.attempt));
  expect(requests[0].method()).toBe("POST");

  releaseAction();
  await expect(page.getByRole("status").filter({ hasText: "Asked the orchestrator to investigate" })).toBeVisible();
  await expect(investigate).toBeEnabled();
});

for (const scenario of [
  {
    action: "archive",
    button: "Archive",
    status: { ...statuses[1], repository: statuses[0].repository },
    consequence: "hides it from the Done lane",
    notice: "Archived issue #160, attempt 2.",
  },
  {
    action: "recover",
    button: "Recover attempt",
    status: { ...statuses[0], issue: 204, attempt: 3, state: "failed", retryable: true },
    consequence: "preserves the worktree and diagnostics",
    notice: "Recovery requested for issue #204, attempt 3.",
  },
  {
    action: "abandon",
    button: "Abandon attempt",
    status: { ...statuses[0], issue: 205, attempt: 4, state: "orphaned", retryable: false },
    consequence: "permanently deletes its local worktree, log, and retained attempt record",
    notice: "Abandoned issue #205, attempt 4.",
  },
]) {
  test(`${scenario.action} supports confirmation cancellation and acceptance`, async ({ page }) => {
    let releaseAction;
    const actionPending = new Promise((resolve) => { releaseAction = resolve; });
    const requests = [];
    const dashboard = await mockDashboard(page, [scenario.status]);
    await page.route(`**/actions/${scenario.action}?*`, async (route) => {
      const url = new URL(route.request().url());
      requests.push({
        method: route.request().method(),
        issue: url.searchParams.get("issue"),
        attempt: url.searchParams.get("attempt"),
      });
      if (scenario.action === "recover" && requests.length === 1) {
        await route.fulfill({ status: 503, body: "reconciliation in progress" });
        return;
      }
      await actionPending;
      if (scenario.action === "archive" || scenario.action === "abandon") {
        dashboard.hide(scenario.status, scenario.action === "archive" ? "archived" : "abandoned");
      }
      await route.fulfill({ json: {} });
    });
    await page.goto("/");

    const card = page.locator(".card");
    const action = card.getByRole("button", { name: scenario.button });
    let confirmation = "";
    page.once("dialog", async (dialog) => {
      confirmation = dialog.message();
      await dialog.dismiss();
    });
    await action.click();
    expect(confirmation).toContain(scenario.consequence);
    expect(requests).toEqual([]);
    await expect(action).toBeEnabled();

    page.once("dialog", (dialog) => dialog.accept());
    await action.click();
    const actionControl = card.locator(scenario.action === "archive" ? ".secondaryAction" : ".dangerAction");
    await expect(actionControl).toBeDisabled();
    await expect(actionControl).toHaveText(scenario.action === "recover" ? "Waiting for reconciliation…" : "Working…");
    const expectedRequests = scenario.action === "recover" ? 2 : 1;
    await expect.poll(() => requests.length).toBe(expectedRequests);
    expect(requests).toEqual(Array.from({ length: expectedRequests }, () => ({
      method: "POST",
      issue: String(scenario.status.issue),
      attempt: String(scenario.status.attempt),
    })));

    releaseAction();
    await expect(page.getByRole("status").filter({ hasText: scenario.notice })).toBeVisible();
    if (scenario.action === "archive" || scenario.action === "abandon") await expect(card).toBeHidden();
    else await expect(action).toBeEnabled();
    if (scenario.action === "recover") {
      expect(browserErrors.get(page)).toEqual(["console: Failed to load resource: the server responded with a status of 503 (Service Unavailable)"]);
      browserErrors.set(page, []);
    }
  });
}
