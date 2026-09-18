import { expect, test } from "@playwright/test";

const baseURL = process.env.AGENT_SYMPHONY_REMOVAL_E2E_URL;
const fakeGitHubURL = process.env.AGENT_SYMPHONY_REMOVAL_E2E_FAKE_GITHUB_URL;
const phase = process.env.AGENT_SYMPHONY_REMOVAL_E2E_PHASE;

test.skip(!baseURL || !fakeGitHubURL, "run through the compiled permanent-removal harness");

test("removes a historical attempt and archives a generated attempt", async ({ page }) => {
  const outbound = [];
  await page.context().route("https://github.com/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path !== "/o/r/issues/73" && path !== "/o/r/pull/91") {
      outbound.push({ path, status: "unexpected" });
      await route.abort();
      return;
    }
    const apiPath = path.endsWith("/pull/91") ? "/repos/o/r/pulls/91" : "/repos/o/r/issues/73";
    const response = await route.fetch({ url: `${fakeGitHubURL}${apiPath}` });
    outbound.push({ path, status: response.status() });
    await route.fulfill({ response });
  });
  await page.goto(baseURL);
  const issueCards = page.getByRole("listitem").filter({ has: page.getByRole("link", { name: /#73 Deterministic full-system journey/ }) });
  if (phase === "archive-generated-click") {
    const generated = issueCards.filter({ hasText: /Attempt 2\b/ }).first();
    const archive = generated.getByRole("button", { name: "Archive" });
    await expect(archive).toBeVisible();
    const responsePromise = page.waitForResponse((response) => response.url().includes("/actions/archive?") && response.request().method() === "POST");
    page.once("dialog", (dialog) => dialog.accept());
    await archive.click();
    const response = await responsePromise;
    expect([200, 202]).toContain(response.status());
    await expect(page.getByRole("status").filter({ hasText: /Archived issue #73, attempt 2|Archive accepted for issue #73, attempt 2/ })).toBeVisible();
    await expect.poll(async () => {
      const state = await fetch(`${baseURL}/dashboard-state.json`, { cache: "no-store" }).then((result) => result.json());
      return state.hidden?.some((attempt) => attempt.repository === "o/r" && attempt.issue === 73 && attempt.attempt === 2 && attempt.reason === "archived");
    }).toBe(true);
    await page.reload();
    await expect(issueCards.filter({ hasText: /Attempt 2\b/ })).toHaveCount(0);
    return;
  }
  if (phase === "post-archive-restart") {
    await expect(issueCards.filter({ hasText: /Attempt [12]\b/ })).toHaveCount(0);
    const state = await fetch(`${baseURL}/dashboard-state.json`, { cache: "no-store" }).then((response) => response.json());
    expect(state.hidden).toEqual(expect.arrayContaining([
      { repository: "o/r", issue: 73, attempt: 1, reason: "removed" },
      { repository: "o/r", issue: 73, attempt: 2, reason: "archived" },
    ]));
    return;
  }
  if (phase === "post-restart") {
    await expect(issueCards.filter({ hasText: /Attempt 1\b/ })).toHaveCount(0);
    await expect(issueCards.filter({ hasText: /Attempt 2\b/ }).first()).toBeVisible();
    await expect(page.getByRole("button", { name: "Permanently remove issue #73, attempt 1" })).toHaveCount(0);
    await expect.poll(async () => {
      const state = await fetch(`${baseURL}/dashboard-state.json`, { cache: "no-store" }).then((response) => response.json());
      return state.hidden?.some((attempt) => attempt.repository === "o/r" && attempt.issue === 73 && attempt.attempt === 1 && attempt.reason === "removed");
    }).toBe(true);
    return;
  }
  for (const [name, path, number] of [
    [/#73 Deterministic full-system journey/, "/o/r/issues/73", 73],
    ["PR #91", "/o/r/pull/91", 91],
  ]) {
    const popupPromise = page.waitForEvent("popup");
    await page.getByRole("link", { name }).first().click();
    const popup = await popupPromise;
    await expect(popup).toHaveURL(`https://github.com${path}`);
    await expect(popup.locator("body")).toContainText(`"number":${number}`);
    const payload = JSON.parse(await popup.locator("body").innerText());
    expect(payload.number).toBe(number);
    await popup.close();
  }
  expect(outbound).toEqual([
    { path: "/o/r/issues/73", status: 200 },
    { path: "/o/r/pull/91", status: 200 },
  ]);
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
  const responsePromise = page.waitForResponse((response) => response.url().includes("/actions/remove?") && response.request().method() === "POST");
  await remove.click();
  const response = await responsePromise;
  expect([200, 202], await response.text()).toContain(response.status());
  await expect(page.getByRole("status").filter({ hasText: "Permanently remove accepted for issue #73, attempt 1." })).toBeVisible();
  expect(confirmation).toContain("cannot be restored");

  await expect.poll(async () => {
    const state = await fetch(`${baseURL}/dashboard-state.json`, { cache: "no-store" }).then((response) => response.json());
    return state.hidden?.some((attempt) => attempt.repository === "o/r" && attempt.issue === 73 && attempt.attempt === 1 && attempt.reason === "removed");
  }, { timeout: 15_000 }).toBe(true);

  await page.reload();
  await expect(page.getByRole("button", { name: "Permanently remove issue #73, attempt 1" })).toHaveCount(0);
  await expect(page.getByRole("link", { name: /#73 Deterministic full-system journey/ })).toBeVisible();
});
