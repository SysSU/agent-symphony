import { expect, test } from "@playwright/test";

const baseURL = process.env.AGENT_SYMPHONY_HISTORICAL_E2E_URL;
test.skip(!baseURL, "run through the compiled historical-actions harness");
test.setTimeout(process.env.AGENT_SYMPHONY_FULL_SYSTEM_RACE === "true" ? 180_000 : 90_000);

test("archive, dismiss, and abandon exact historical attempts through the deployed dashboard", async ({ page }) => {
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("console", (message) => { if (message.type() === "error") errors.push(message.text()); });
  await page.goto(baseURL);
  await expect(page.getByRole("region", { name: "Issue status board" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Archive" })).toHaveCount(3);
  await expect(page.getByRole("button", { name: "Dismiss issue #161, attempt 2; keep diagnostics" })).toHaveCount(0);
  await expect(page.getByRole("link", { name: /#161\b/ })).toHaveCount(0);

  const card = (issue) => page.getByRole("listitem").filter({ has: page.getByRole("link", { name: new RegExp(`#${issue}\\b`) }) });
  await expect(card(191).getByRole("button", { name: "Abandon attempt" })).toBeVisible();
  await expect(card(192).getByRole("button", { name: "Dismiss issue #192, attempt 9; keep diagnostics" })).toBeVisible();

  async function clickAction(path, button) {
    const responsePromise = page.waitForResponse((response) => response.url().includes(`/actions/${path}?`) && response.request().method() === "POST");
    page.once("dialog", (dialog) => dialog.accept());
    await button.click();
    const response = await responsePromise;
    expect([200, 202]).toContain(response.status());
    expect(response.headers()["content-type"]).toContain("application/json");
  }

  await clickAction("archive", card(160).getByRole("button", { name: "Archive" }));
  await clickAction("dismiss", card(162).getByRole("button", { name: "Dismiss issue #162, attempt 1; keep diagnostics" }));
  await clickAction("archive", card(163).getByRole("button", { name: "Archive" }));
  await clickAction("abandon", card(191).getByRole("button", { name: "Abandon attempt" }));
  await clickAction("dismiss", card(192).getByRole("button", { name: "Dismiss issue #192, attempt 9; keep diagnostics" }));

  await expect.poll(async () => {
    const response = await fetch(`${baseURL}/dashboard-state.json`, { cache: "no-store" });
    const state = await response.json();
    return [160, 162, 163, 191, 192].every((issue) => state.hidden?.some((entry) => entry.issue === issue));
  }).toBe(true);
  await page.reload();
  for (const issue of [160, 161, 162, 163, 191, 192]) {
    await expect(page.getByRole("link", { name: new RegExp(`#${issue}\\b`) })).toHaveCount(0);
  }
  expect(errors).toEqual([]);
});
