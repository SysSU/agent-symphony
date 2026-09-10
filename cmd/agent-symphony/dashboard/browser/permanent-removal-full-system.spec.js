import { expect, test } from "@playwright/test";

const baseURL = process.env.AGENT_SYMPHONY_REMOVAL_E2E_URL;

test.skip(!baseURL, "run through the compiled permanent-removal harness");

test("permanently removes one real historical attempt", async ({ page }) => {
  await page.goto(baseURL);
  const history = page.locator("details.attemptHistory");
  await history.locator("summary").click();

  const remove = history.getByRole("button", { name: "Permanently remove issue #73, attempt 1" });
  await expect(remove).toBeVisible();
  await expect(history.getByRole("button", { name: "Dismiss issue #73, attempt 1; keep diagnostics" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Permanently remove issue #73, attempt 2" })).toHaveCount(0);

  let confirmation = "";
  page.once("dialog", async (dialog) => {
    confirmation = dialog.message();
    await dialog.accept();
  });
  await remove.click();
  await expect(page.getByRole("status").filter({ hasText: "Permanently removed issue #73, attempt 1." })).toBeVisible();
  expect(confirmation).toContain("cannot be restored");

  await page.reload();
  await expect(page.getByRole("button", { name: "Permanently remove issue #73, attempt 1" })).toHaveCount(0);
  await expect(page.getByRole("link", { name: /#73 Deterministic full-system journey/ })).toBeVisible();
});
