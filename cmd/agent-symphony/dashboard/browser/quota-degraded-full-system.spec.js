import { expect, test } from "@playwright/test";

const baseURL = process.env.AGENT_SYMPHONY_QUOTA_E2E_URL;
test.skip(!baseURL, "run through the compiled quota-startup harness");
test.use({ httpCredentials: { username: "agent-symphony", password: "quota-fixture-secret" } });
test.setTimeout(60_000);

test("quota startup exposes only the controls backed by a live owner", async ({ page }) => {
  const posts = [];
  page.on("request", (request) => { if (request.method() === "POST") posts.push(request.url()); });
  await page.goto(baseURL);
  await expect(page.getByRole("heading", { name: "o/r" })).toBeVisible();
  if (process.env.AGENT_SYMPHONY_QUOTA_E2E_PHASE === "live") {
    await expect(page.getByRole("button", { name: "Check now" })).toBeVisible({ timeout: 30_000 });
    await expect(page.getByRole("heading", { name: "Status refresh failed" })).toHaveCount(0);
    expect(posts).toEqual([]);
    return;
  }
  await expect(page.getByRole("heading", { name: "Status refresh failed" })).toBeVisible();
  await expect(page.getByText("GitHub authentication is unavailable. Controls and terminals are disabled until recovery.")).toBeVisible();
  await expect(page.getByRole("button", { name: "Check now" })).toHaveCount(0);
  await expect(page.getByRole("button", { name: /Archive|Abandon|Dismiss|Permanently remove|Cancel attempt/ })).toHaveCount(0);
  expect(posts).toEqual([]);
});

test("a late degraded response cannot replace a newer live owner", async ({ page }) => {
  test.skip(process.env.AGENT_SYMPHONY_QUOTA_E2E_PHASE !== "live");
  await page.goto(baseURL);
  await expect(page.getByRole("button", { name: "Check now" })).toBeVisible();
  let held;
  let notifyHeld;
  const heldRequest = new Promise((resolve) => { notifyHeld = resolve; });
  await page.route("**/status.json", async (route) => {
    if (!held) {
      held = route;
      notifyHeld();
      return;
    }
    await route.continue();
  });
  await heldRequest;
  await page.getByRole("button", { name: "Check now" }).click();
  await expect(page.getByText("Reconciliation completed.")).toBeVisible();
  const staleResponse = page.waitForResponse((response) => response.headers()["x-test-stale"] === "1");
  const stateRefresh = page.waitForResponse((response) => response.url().endsWith("/dashboard-state.json"));
  await held.fulfill({ headers: { "x-test-stale": "1" }, json: { owner_epoch: 0, owner_revision: 0, read_only: true, statuses: [], reconciliation_error: "GitHub authentication is unavailable", updated_at: new Date().toISOString() } });
  await staleResponse;
  await stateRefresh;
  await page.evaluate(() => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  await expect(page.getByRole("button", { name: "Check now" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Status refresh failed" })).toHaveCount(0);
});

test("an equal-version old live response cannot clear degraded mode", async ({ page }) => {
  test.skip(process.env.AGENT_SYMPHONY_QUOTA_E2E_PHASE !== "live");
  await page.goto(baseURL);
  await expect(page.getByRole("button", { name: "Check now" })).toBeVisible();
  const live = await page.evaluate(async () => (await fetch("/status.json")).json());
  let degraded = false;
  await page.route("**/status.json", async (route) => {
    if (!degraded) {
      degraded = true;
      await route.fulfill({ headers: { "x-test-phase": "degraded" }, json: { ...live, read_only: true, reconciliation_error: "GitHub authentication is unavailable" } });
      return;
    }
    await route.fulfill({ headers: { "x-test-phase": "old-live" }, json: live });
  });
  await page.waitForResponse((response) => response.headers()["x-test-phase"] === "degraded");
  await expect(page.getByRole("button", { name: "Check now" })).toHaveCount(0);
  const oldLive = page.waitForResponse((response) => response.headers()["x-test-phase"] === "old-live");
  const stateRefresh = page.waitForResponse((response) => response.url().endsWith("/dashboard-state.json"));
  await oldLive;
  await stateRefresh;
  await page.evaluate(() => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  await expect(page.getByRole("button", { name: "Check now" })).toHaveCount(0);
  await expect(page.getByRole("heading", { name: "Status refresh failed" })).toBeVisible();
});
