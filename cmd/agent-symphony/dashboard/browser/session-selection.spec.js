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
    { role: "reviewer", name: "as-r-4bb6b1ed4949cd7c-163-1", state: "running", current: true },
  ],
  worktree: "/attempts/agent-symphony-163-1",
  branch: "issue-163-attempt-sessions",
};

async function mockDashboard(page, closeReason = "") {
  const sockets = [];
  await page.route("**/release.json", (route) => route.fulfill({ json: { release: "0.5.0" } }));
  await page.route("**/status.json", (route) => route.fulfill({ json: { updated_at: new Date().toISOString(), statuses: [attempt] } }));
  await page.route("**/dashboard-state.json", (route) => route.fulfill({ json: { hidden: [] } }));
  await page.route("**/projects.json", (route) => route.fulfill({ json: { version: 1, projects: [{ version: 1, repository: attempt.repository, local: true, snapshot: { updated_at: new Date().toISOString(), statuses: [attempt] }, state: { version: 1, hidden: [] } }] } }));
  await page.route("**/orchestrator.json", (route) => route.fulfill({ json: { enabled: false, state: "disabled" } }));
  await page.route("**/orchestrator/proposal.json", (route) => route.fulfill({ status: 204 }));
  await page.routeWebSocket(/\/(?:reviewer\/)?terminal\?/, (socket) => {
    const record = { url: socket.url(), messages: [] };
    sockets.push(record);
    socket.onMessage((message) => record.messages.push(message));
    if (closeReason) setTimeout(() => void socket.close({ code: 1000, reason: closeReason }), 0);
  });
  return sockets;
}

test("selects each bounded session and keeps reviewer attachment read-only", async ({ page }) => {
  const errors = [];
  page.on("console", (message) => {
    if (message.type() === "error") errors.push(message.text());
  });
  page.on("pageerror", (error) => errors.push(error.message));
  const sockets = await mockDashboard(page);
  await page.goto("/");

  await expect(page.getByText("review", { exact: true })).toBeVisible();
  await page.screenshot({ path: "test-results/session-selection-card.png", fullPage: true });
  await page.getByRole("button", { name: "Open reviewer terminal" }).click();
  await expect(page.getByRole("dialog", { name: attempt.sessions[1].name })).toBeVisible();
  await expect(page.getByRole("button", { name: "Close" })).toBeFocused();
  await expect(page.getByRole("status").filter({ hasText: "Connected · read-only" })).toBeVisible();
  await page.screenshot({ path: "test-results/session-selection-desktop.png", fullPage: true });
  await page.getByLabel(`Read-only terminal for ${attempt.sessions[1].name}`).click();
  await page.keyboard.type("review input");
  await expect.poll(() => sockets.length).toBe(1);
  let target = new URL(sockets[0].url);
  expect(target.pathname).toBe("/reviewer/terminal");
  expect(Object.fromEntries(target.searchParams)).toEqual({ repository: attempt.repository, issue: "163", attempt: "1" });
  expect(sockets[0].messages.every((message) => typeof message === "string")).toBe(true);
  await page.getByRole("button", { name: "Close" }).click();

  await page.getByRole("button", { name: attempt.sessions[0].name }).click();
  await expect(page.getByRole("dialog", { name: attempt.sessions[0].name })).toBeVisible();
  await page.keyboard.type("implementation input");
  await expect.poll(() => sockets[1]?.messages.some((message) => typeof message !== "string")).toBe(true);
  target = new URL(sockets[1].url);
  expect(target.pathname).toBe("/terminal");
  expect(Object.fromEntries(target.searchParams)).toEqual({ repository: attempt.repository, issue: "163", attempt: "1" });
  expect(errors).toEqual([]);
});

test("keeps session selection and the terminal dialog inside a mobile viewport", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await mockDashboard(page);
  await page.goto("/");

  await expect(page.getByRole("button", { name: "Open reviewer terminal" })).toBeVisible();
  await page.getByRole("button", { name: "Open reviewer terminal" }).click();
  const box = await page.getByRole("dialog", { name: attempt.sessions[1].name }).boundingBox();
  expect(box).not.toBeNull();
  expect(box.x).toBeGreaterThanOrEqual(0);
  expect(box.x + box.width).toBeLessThanOrEqual(390);
  await page.screenshot({ path: "test-results/session-selection-mobile.png", fullPage: true });
});

test("shows the server close reason directly", async ({ page }) => {
  await mockDashboard(page, "Session ended.");
  await page.goto("/");

  await page.getByRole("button", { name: "Open reviewer terminal" }).click();
  await expect(page.locator("#terminalConnection")).toHaveText("Session ended.");
});
