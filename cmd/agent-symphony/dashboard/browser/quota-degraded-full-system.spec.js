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
