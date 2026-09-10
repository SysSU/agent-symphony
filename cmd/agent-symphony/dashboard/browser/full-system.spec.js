import { expect, test } from "@playwright/test";

const baseURL = process.env.AGENT_SYMPHONY_FULL_SYSTEM_URL;

test.skip(!baseURL, "run through the compiled full-system harness");

test("operator reaches the real implementation session and sees prompt status clear", async ({ page }) => {
  const errors = [];
  page.on("console", (message) => {
    if (message.type() === "error") errors.push(message.text());
  });
  page.on("pageerror", (error) => errors.push(error.message));

  await page.goto(baseURL);
  const board = page.getByRole("region", { name: "Issue status board" });
  const inProgress = page.locator(".lane").filter({ has: page.locator("#lane-in-progress") });
  const issue = inProgress.getByRole("link", { name: /#73 Deterministic full-system journey/ });
  await expect(issue).toBeVisible({ timeout: 15_000 });
  await expect(inProgress).toContainText("Deterministic full-system journey");

  const session = inProgress.getByRole("button", { name: /^as-/ });
  await session.click();
  const dialog = page.getByRole("dialog", { name: /^as-/ });
  await expect(dialog.getByRole("status")).toHaveText("Connected");
  const attentionStarted = Date.now();
  await dialog.locator(".terminal textarea").pressSequentially("pause for operator decision");
  await page.keyboard.press("Enter");
  await expect(dialog.locator(".xterm-rows")).toContainText("attention-status-set", { timeout: 10_000 });
  await expect(page.locator(".lane").filter({ has: page.locator("#lane-needs-attention") })).toContainText("Deterministic full-system journey", { timeout: 10_000 });
  expect(Date.now() - attentionStarted).toBeLessThan(10_000);
  const recoveryStarted = Date.now();
  await dialog.locator(".terminal textarea").pressSequentially("continue the verified test journey");
  await page.keyboard.press("Enter");
  await expect(dialog.locator(".xterm-rows")).toContainText("operator-message-received", { timeout: 10_000 });
  await dialog.getByRole("button", { name: "Close" }).click();

  await expect(inProgress).toContainText("review", { timeout: 25_000 });
  await expect(page.getByText("needs attention", { exact: true })).toHaveCount(0, { timeout: 10_000 });
  expect(Date.now() - recoveryStarted).toBeLessThan(10_000);
  expect(errors).toEqual([]);
});
