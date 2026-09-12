import { expect, test } from "@playwright/test";

const baseURL = process.env.AGENT_SYMPHONY_HISTORICAL_E2E_URL;
test.skip(!baseURL, "run through the compiled historical-actions harness");
test.setTimeout(process.env.AGENT_SYMPHONY_FULL_SYSTEM_RACE === "true" ? 180_000 : 90_000);

test("archive, dismiss, and abandon exact historical attempts through the deployed dashboard", async ({ page }) => {
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("console", (message) => { if (message.type() === "error") errors.push(message.text()); });
  await page.goto(baseURL);
  const board = page.getByRole("region", { name: "Issue status board" });
  await expect(board).toBeVisible();
  const card = (issue) => board.getByRole("listitem").filter({ has: page.getByRole("link", { name: new RegExp(`#${issue}\\b`) }) });
  async function clickAction(path, button) {
    const responsePromise = page.waitForResponse((response) => response.url().includes(`/actions/${path}?`) && response.request().method() === "POST");
    page.once("dialog", (dialog) => dialog.accept());
    await button.click();
    const response = await responsePromise;
    expect([200, 202]).toContain(response.status());
    expect(response.headers()["content-type"]).toContain("application/json");
  }

  async function expectOldAttemptsOnlyInHistory() {
    for (const issue of [164, 191, 193]) {
      await expect(board.getByRole("link", { name: new RegExp(`#${issue}\\b`) })).toHaveCount(0);
    }
    await page.getByText("Previous attempts").click();
    for (const [issue, attempt] of [[164, 1], [191, 8], [193, 8]]) {
      await expect(page.getByRole("listitem").filter({ has: page.getByRole("link", { name: new RegExp(`#${issue}\\b`) }) })).toContainText(`Attempt ${attempt}`);
    }
  }

  if (process.env.AGENT_SYMPHONY_HISTORICAL_E2E_PHASE === "verification") {
    await expectOldAttemptsOnlyInHistory();
    expect(errors).toEqual([]);
    return;
  }

  if (process.env.AGENT_SYMPHONY_HISTORICAL_E2E_PHASE === "post-restart") {
    await expect(card(191).getByRole("button", { name: "Abandon attempt" })).toBeVisible();
    await expect(page.getByText("Previous attempts")).toBeVisible();
    await expect(card(193).getByRole("button", { name: "Dismiss issue #193, attempt 9; keep diagnostics" })).toBeVisible();
    await clickAction("abandon", card(191).getByRole("button", { name: "Abandon attempt" }));
    await clickAction("dismiss", card(193).getByRole("button", { name: "Dismiss issue #193, attempt 9; keep diagnostics" }));
    await expect.poll(async () => {
      const response = await fetch(`${baseURL}/dashboard-state.json`, { cache: "no-store" });
      const state = await response.json();
      return [191, 193].every((issue) => state.hidden?.some((entry) => entry.issue === issue));
    }).toBe(true);
    await page.reload();
    await expectOldAttemptsOnlyInHistory();
    expect(errors).toEqual([]);
    return;
  }

  await expect(board.getByRole("button", { name: "Archive" })).toHaveCount(4);
  await expect(page.getByRole("button", { name: "Dismiss issue #161, attempt 2; keep diagnostics" })).toHaveCount(0);
  await expect(page.getByRole("link", { name: /#161\b/ })).toHaveCount(0);
  await expect(card(191).getByRole("button", { name: "Abandon attempt" })).toBeVisible();
  await expect(card(192).getByRole("button", { name: "Dismiss issue #192, attempt 9; keep diagnostics" })).toBeVisible();
  await expect(card(193).getByRole("button", { name: "Dismiss issue #193, attempt 9; keep diagnostics" })).toBeVisible();

  await clickAction("archive", card(160).getByRole("button", { name: "Archive" }));
  await clickAction("dismiss", card(162).getByRole("button", { name: "Dismiss issue #162, attempt 1; keep diagnostics" }));
  await clickAction("archive", card(163).getByRole("button", { name: "Archive" }));
  await clickAction("archive", card(164).getByRole("button", { name: "Archive" }));
  await clickAction("dismiss", card(192).getByRole("button", { name: "Dismiss issue #192, attempt 9; keep diagnostics" }));

  await expect.poll(async () => {
    const response = await fetch(`${baseURL}/dashboard-state.json`, { cache: "no-store" });
    const state = await response.json();
    return [160, 162, 163, 164, 192].every((issue) => state.hidden?.some((entry) => entry.issue === issue));
  }).toBe(true);
  await page.reload();
  for (const issue of [160, 161, 162, 163, 192]) {
    await expect(page.getByRole("link", { name: new RegExp(`#${issue}\\b`) })).toHaveCount(0);
  }
  await expect(board.getByRole("link", { name: /#164\b/ })).toHaveCount(0);
  expect(errors).toEqual([]);
});
