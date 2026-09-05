import { expect, test } from "@playwright/test";

const attempt = {
  repository: "SysSU/agent-symphony",
  issue: 163,
  attempt: 1,
  title: "Track attempt lifecycle sessions",
  state: "active",
  current_phase: "review",
  session: "as-syssu-agent-symphony-4bb6b1ed4949-163-1",
  sessions: [
    { role: "implementation", name: "as-syssu-agent-symphony-4bb6b1ed4949-163-1", state: "completed" },
    { role: "reviewer", name: "as-r-4bb6b1ed4949cd7c-163-1", state: "running", mode: "plan-review", target: "SysSU/agent-symphony#163 plan sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", current: true },
  ],
  worktree: "/attempts/agent-symphony-163-1",
  branch: "issue-163-attempt-sessions",
};
const implementationReview = {
  ...attempt,
  issue: 164,
  title: "Review the exact implementation",
  session: "as-syssu-agent-symphony-4bb6b1ed4949-164-1",
  sessions: [
    { role: "implementation", name: "as-syssu-agent-symphony-4bb6b1ed4949-164-1", state: "completed" },
    { role: "reviewer", name: "as-r-4bb6b1ed4949cd7c-164-1", state: "running", mode: "implementation-review", target: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa..bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", current: true },
  ],
  worktree: "/attempts/agent-symphony-164-1",
  branch: "issue-164-interactive-review",
};
const planCandidate = {
  ...attempt,
  issue: 165,
  title: "Review this issue plan",
  current_phase: "implementation",
  session: "as-syssu-agent-symphony-4bb6b1ed4949-165-1",
  sessions: [
    { role: "implementation", name: "as-syssu-agent-symphony-4bb6b1ed4949-165-1", state: "running", current: true },
  ],
};

async function mockDashboard(page, closeReason = "", reviewPlanResponse = { status: 200, json: { ok: true } }) {
  const sockets = [];
  const reviewPlanRequests = [];
  await page.route("**/release.json", (route) => route.fulfill({ json: { release: "0.5.0" } }));
  await page.route("**/status.json", (route) => route.fulfill({ json: { updated_at: new Date().toISOString(), statuses: [attempt, implementationReview, planCandidate] } }));
  await page.route("**/dashboard-state.json", (route) => route.fulfill({ json: { hidden: [] } }));
  await page.route("**/projects.json", (route) => route.fulfill({ json: { version: 1, projects: [{ version: 1, repository: attempt.repository, local: true, snapshot: { updated_at: new Date().toISOString(), statuses: [attempt] }, state: { version: 1, hidden: [] } }] } }));
  await page.route("**/orchestrator.json", (route) => route.fulfill({ json: { enabled: false, state: "disabled" } }));
  await page.route("**/orchestrator/proposal.json", (route) => route.fulfill({ status: 204 }));
  await page.route("**/actions/review-plan?*", (route) => {
    reviewPlanRequests.push(route.request().url());
    return route.fulfill(reviewPlanResponse);
  });
  await page.routeWebSocket(/\/(?:reviewer\/)?terminal\?/, (socket) => {
    const record = { url: socket.url(), messages: [], replied: false };
    sockets.push(record);
    socket.onMessage((message) => {
      record.messages.push(message);
      if (typeof message !== "string" && !record.replied) {
        record.replied = true;
        socket.send(Buffer.from("agent: acknowledged\r\n"));
      }
    });
    if (closeReason) setTimeout(() => void socket.close({ code: 1000, reason: closeReason }), 0);
  });
  return { sockets, reviewPlanRequests };
}

test("opens and chats with both exact review modes", async ({ page }) => {
  const errors = [];
  page.on("console", (message) => {
    if (message.type() === "error") errors.push(message.text());
  });
  page.on("pageerror", (error) => errors.push(error.message));
  const { sockets } = await mockDashboard(page);
  await page.goto("/");

  await expect(page.getByText("review", { exact: true })).toHaveCount(2);
  await expect(page.getByText(/reviewer · plan-review · running/)).toBeVisible();
  await expect(page.getByText(/reviewer · implementation-review · running/)).toBeVisible();
  await expect(page.getByText(attempt.sessions[1].target, { exact: true })).toBeVisible();
  await expect(page.getByText(implementationReview.sessions[1].target, { exact: true })).toBeVisible();
  await expect(page.getByText("ui-review", { exact: true })).toHaveCount(0);
  await page.screenshot({ path: "test-results/session-selection-card.png", fullPage: true });
  for (const [index, review] of [attempt, implementationReview].entries()) {
    await page.getByRole("button", { name: "Open reviewer terminal" }).nth(index).click();
    await expect(page.getByRole("dialog", { name: review.sessions[1].name })).toBeVisible();
    await expect(page.getByRole("status").filter({ hasText: "Connected" })).toBeVisible();
    const terminal = page.getByLabel(`Terminal for ${review.sessions[1].name}`);
    await expect.poll(() => terminal.evaluate((element) => element.contains(document.activeElement))).toBe(true);
    await page.keyboard.type(`${review.sessions[1].mode} question`);
    await expect.poll(() => sockets[index]?.messages.some((message) => typeof message !== "string")).toBe(true);
    const target = new URL(sockets[index].url);
    expect(target.pathname).toBe("/reviewer/terminal");
    expect(Object.fromEntries(target.searchParams)).toEqual({ repository: review.repository, issue: String(review.issue), attempt: "1" });
    await page.getByRole("button", { name: "Close" }).click();
  }
  await page.screenshot({ path: "test-results/session-selection-desktop.png", fullPage: true });

  await page.getByRole("button", { name: attempt.sessions[0].name }).click();
  await expect(page.getByRole("dialog", { name: attempt.sessions[0].name })).toBeVisible();
  await page.keyboard.type("implementation input");
  await expect.poll(() => sockets[2]?.messages.some((message) => typeof message !== "string")).toBe(true);
  const target = new URL(sockets[2].url);
  await expect(page.getByText("agent: acknowledged", { exact: false })).toBeVisible();
  expect(target.pathname).toBe("/terminal");
  expect(Object.fromEntries(target.searchParams)).toEqual({ repository: attempt.repository, issue: "163", attempt: "1" });
  expect(Buffer.concat(sockets[2].messages.filter((message) => typeof message !== "string").map((message) => Buffer.from(message))).toString()).toContain("implementation input");
  expect(errors).toEqual([]);
});

test("reports a fresh plan review start from the retained-clean projection", async ({ page }) => {
  const { reviewPlanRequests } = await mockDashboard(page);
  await page.goto("/");

  await page.getByRole("button", { name: "Start plan review" }).click();
  await expect.poll(() => reviewPlanRequests).toEqual([
    `${new URL(page.url()).origin}/actions/review-plan?repository=SysSU%2Fagent-symphony&issue=165&attempt=1`,
  ]);
  await expect(page.getByRole("status").filter({ hasText: "Started plan review for issue #165, attempt 1." })).toBeVisible();
});

test("reports cleanup-pending plan review as not started", async ({ page }) => {
  await mockDashboard(page, "", { status: 409, body: "plan review did not start a reviewer session" });
  await page.goto("/");

  await page.getByRole("button", { name: "Start plan review" }).click();
  await expect(page.getByRole("status").filter({ hasText: "plan review did not start a reviewer session" })).toBeVisible();
  await expect(page.getByRole("status").filter({ hasText: "Started plan review" })).toHaveCount(0);
});

test("keeps session selection and the terminal dialog inside a mobile viewport", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await mockDashboard(page);
  await page.goto("/");

  await expect(page.getByRole("button", { name: "Open reviewer terminal" }).first()).toBeVisible();
  await page.getByRole("button", { name: "Open reviewer terminal" }).first().click();
  const box = await page.getByRole("dialog", { name: attempt.sessions[1].name }).boundingBox();
  expect(box).not.toBeNull();
  expect(box.x).toBeGreaterThanOrEqual(0);
  expect(box.x + box.width).toBeLessThanOrEqual(390);
  await page.screenshot({ path: "test-results/session-selection-mobile.png", fullPage: true });
});

test("shows the server close reason directly", async ({ page }) => {
  await mockDashboard(page, "Session ended.");
  await page.goto("/");

  await page.getByRole("button", { name: "Open reviewer terminal" }).first().click();
  await expect(page.locator("#terminalConnection")).toHaveText("Session ended.");
});
