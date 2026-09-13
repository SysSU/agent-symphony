import { expect, test } from "@playwright/test";
import { constants } from "node:fs";
import { open, readFile } from "node:fs/promises";

const baseURL = process.env.AGENT_SYMPHONY_LIFECYCLE_E2E_URL;
const action = process.env.AGENT_SYMPHONY_LIFECYCLE_E2E_ACTION;
const reviewGate = process.env.AGENT_SYMPHONY_LIFECYCLE_E2E_REVIEW_GATE;
const reviewerPID = process.env.AGENT_SYMPHONY_LIFECYCLE_E2E_REVIEWER_PID;
const fakeGitHubURL = process.env.AGENT_SYMPHONY_LIFECYCLE_E2E_FAKE_GITHUB_URL;
test.skip(!baseURL || !action, "run through the compiled dashboard lifecycle harness");
test.setTimeout(process.env.AGENT_SYMPHONY_FULL_SYSTEM_RACE === "true" ? 180_000 : 90_000);

test("lifecycle action commits through the real dashboard", async ({ page }) => {
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("console", (message) => { if (message.type() === "error") errors.push(message.text()); });
  await page.goto(baseURL);
  const card = page.getByRole("listitem").filter({ has: page.getByRole("link", { name: /#73\b/ }) }).first();
  const exactAttempt = page.getByRole("listitem").filter({ has: page.getByRole("link", { name: /#73\b/ }) }).filter({ hasText: /Attempt 1\b/ });
  if (action.endsWith("-overlap-verify")) {
    await expect.poll(async () => {
      const status = await fetch(`${baseURL}/status.json`, { cache: "no-store" }).then((result) => result.json());
      return status.statuses?.some((entry) => entry.issue === 73 && entry.attempt === 1);
    }).toBe(false);
    await expect(exactAttempt).toHaveCount(0);
    expect(errors).toEqual([]);
    return;
  }
  if (action === "review-plan-archive-click") {
    await expect(card).toContainText("Attempt 1");
    const archive = card.getByRole("button", { name: "Archive" });
    await expect(archive).toBeVisible();
    const responsePromise = page.waitForResponse((response) => response.url().includes("/actions/archive?") && response.request().method() === "POST");
    page.once("dialog", (dialog) => dialog.accept());
    await archive.click();
    const response = await responsePromise;
    expect([200, 202]).toContain(response.status());
    await expect(page.getByRole("status").filter({ hasText: /Archived issue #73, attempt 1|Archive accepted for issue #73, attempt 1/ })).toBeVisible();
    await expect.poll(async () => {
      const state = await fetch(`${baseURL}/dashboard-state.json`, { cache: "no-store" }).then((result) => result.json());
      return state.hidden?.some((entry) => entry.issue === 73 && entry.attempt === 1 && entry.reason === "archived");
    }).toBe(true);
    await page.reload();
    await expect(exactAttempt).toHaveCount(0);
    expect(errors).toEqual([]);
    return;
  }
  if (action === "review-plan-archive-verify") {
    await expect.poll(async () => {
      const status = await fetch(`${baseURL}/status.json`, { cache: "no-store" }).then((result) => result.json());
      return status.statuses?.some((entry) => entry.issue === 73 && entry.attempt === 1);
    }).toBe(false);
    await expect(exactAttempt).toHaveCount(0);
    expect(errors).toEqual([]);
    return;
  }
  await expect(card).toContainText("Attempt 1");
  const button = {
    cancel: card.getByRole("button", { name: "Cancel issue #73, attempt 1; retain diagnostics" }),
    recover: card.getByRole("button", { name: "Recover attempt" }),
    "review-plan": card.getByRole("button", { name: "Start plan review" }),
    "review-plan-archive": card.getByRole("button", { name: "Start plan review" }),
    "review-plan-cancel": card.getByRole("button", { name: "Start plan review" }),
    "dismiss-overlap": card.getByRole("button", { name: "Dismiss issue #73, attempt 1; keep diagnostics" }),
    "abandon-overlap": card.getByRole("button", { name: "Abandon attempt" }),
  }[action];
  await expect(button).toBeVisible();
  const requestAction = action.startsWith("review-plan") ? "review-plan" : action.replace("-overlap", "");
  const responsePromise = page.waitForResponse((response) => response.url().includes(`/actions/${requestAction}?`) && response.request().method() === "POST");
  if (requestAction !== "review-plan") page.once("dialog", (dialog) => dialog.accept());
  await button.click();
  const response = await responsePromise;
  if (![200, 202].includes(response.status())) {
    await page.getByRole("status").filter({ hasText: /conflict|changed|failed|invalid/i }).first().waitFor({ state: "visible", timeout: 5_000 }).catch(() => {});
    throw new Error(`${action} returned HTTP ${response.status()}; dashboard notices: ${JSON.stringify(await page.getByRole("status").allTextContents())}`);
  }
  expect([200, 202]).toContain(response.status());
  expect(response.headers()["content-type"]).toContain("application/json");
  await expect(page.getByRole("status").filter({ hasText: requestAction === "review-plan" ? "Started plan review for issue #73, attempt 1." : "issue #73, attempt 1" })).toBeVisible({ timeout: 15_000 });

  if (action.endsWith("-overlap")) {
    const reason = requestAction === "dismiss" ? "dismissed" : "abandoned";
    await expect.poll(async () => {
      const state = await fetch(`${baseURL}/dashboard-state.json`, { cache: "no-store" }).then((result) => result.json());
      return state.hidden?.some((entry) => entry.issue === 73 && entry.attempt === 1 && entry.reason === reason);
    }).toBe(true);
    await page.reload();
    await expect(exactAttempt).toHaveCount(0);
    expect(errors).toEqual([]);
    return;
  }

  if (action === "cancel") {
    await expect.poll(async () => {
      const status = await fetch(`${baseURL}/status.json`, { cache: "no-store" }).then((result) => result.json());
      return status.statuses?.find((entry) => entry.issue === 73 && entry.attempt === 1)?.state;
    }, { timeout: 20_000 }).toMatch(/^(cancelled|failed)$/);
    await page.reload();
    await expect(page.getByRole("listitem").filter({ has: page.getByRole("link", { name: /#73\b/ }) }).first()).toContainText(/cancelled|failed/);
  }
  if (requestAction === "review-plan") {
    let reviewer;
    await expect.poll(async () => {
      const status = await fetch(`${baseURL}/status.json`, { cache: "no-store" }).then((result) => result.json());
      reviewer = status.statuses?.find((entry) => entry.issue === 73 && entry.attempt === 1)?.sessions?.find((session) => session.role === "reviewer" && session.mode === "plan-review");
      return reviewer?.state;
    }, { timeout: 20_000 }).toBe("running");
    expect(reviewer.name).toMatch(/^as-/);
    const reviewerButton = card.getByRole("button", { name: "Open reviewer terminal" });
    await expect(reviewerButton).toBeVisible();
    await reviewerButton.click();
    const terminal = page.getByRole("dialog", { name: reviewer.name });
    await expect(terminal).toBeVisible();
    await expect(terminal.getByRole("status")).toHaveText("Connected");
    await terminal.getByRole("button", { name: "Close" }).click();
    if (action === "review-plan-cancel") {
      await expect.poll(async () => {
        try { return Number((await readFile(reviewerPID, "utf8")).trim()) > 1; }
        catch { return false; }
      }, { timeout: 20_000 }).toBe(true);
      const cancel = card.getByRole("button", { name: "Cancel issue #73, attempt 1; retain diagnostics" });
      await expect(cancel).toBeVisible();
      const cancelResponse = page.waitForResponse((result) => result.url().includes("/actions/cancel?") && result.request().method() === "POST");
      page.once("dialog", (dialog) => dialog.accept());
      await cancel.click();
      const canceled = await cancelResponse;
      expect([200, 202], await canceled.text()).toContain(canceled.status());
      await expect(page.getByRole("status").filter({ hasText: "issue #73, attempt 1" })).toBeVisible();
      await expect.poll(async () => {
        const status = await fetch(`${baseURL}/status.json`, { cache: "no-store" }).then((result) => result.json());
        return status.statuses?.find((entry) => entry.issue === 73 && entry.attempt === 1)?.state;
      }, { timeout: 20_000 }).toBe("failed");
      await page.reload();
      const canceledCard = page.getByRole("listitem").filter({ has: page.getByRole("link", { name: /#73\b/ }) }).first();
      await expect(canceledCard).toContainText("failed");
      try {
        await expect(canceledCard.getByRole("button", { name: "Recover attempt" })).toBeVisible();
      } catch (error) {
        const [status, comments] = await Promise.all([
          page.request.get(`${baseURL}/status.json`).then((response) => response.json()),
          page.request.get(`${fakeGitHubURL}/repos/o/r/issues/73/comments`).then((response) => response.json()),
        ]);
        throw new Error(`Recover is unavailable after Cancel: status=${JSON.stringify(status)} comments=${JSON.stringify(comments)}`, { cause: error });
      }
      await expect(canceledCard.getByRole("button", { name: "Open reviewer terminal" })).toHaveCount(0);
      expect(errors).toEqual([]);
      return;
    }
    const gate = await open(reviewGate, constants.O_RDWR | constants.O_NONBLOCK);
    try {
      await gate.writeFile("release\n");
      await expect.poll(async () => {
        const status = await fetch(`${baseURL}/status.json`, { cache: "no-store" }).then((result) => result.json());
        return status.statuses?.find((entry) => entry.issue === 73 && entry.attempt === 1)?.sessions?.find((session) => session.role === "reviewer" && session.mode === "plan-review")?.state;
      }, { timeout: 20_000 }).toBe("clean");
    } finally {
      await gate.close();
    }
    await page.reload();
    await expect(page.getByText(/reviewer · plan-review · clean/)).toBeVisible();
  }
  if (action === "recover") {
    const released = await fetch(`${fakeGitHubURL}/fixture/release-retry`, { method: "POST" });
    expect(released.status).toBe(204);
    let recoveredSession;
    await expect.poll(async () => {
      const status = await fetch(`${baseURL}/status.json`, { cache: "no-store" }).then((result) => result.json());
      const recovered = status.statuses?.find((entry) => entry.issue === 73 && entry.attempt === 2);
      recoveredSession = recovered?.session;
      return { state: recovered?.state, session: recovered?.sessions?.find((session) => session.role === "implementation")?.state };
    }, { timeout: process.env.AGENT_SYMPHONY_FULL_SYSTEM_RACE === "true" ? 90_000 : 20_000 }).toEqual({ state: "active", session: "running" });
    await page.reload();
    const recoveredCard = page.getByRole("listitem").filter({ hasText: "Attempt 2" }).first();
    await expect(recoveredCard).toContainText("active");
    await expect(recoveredCard.getByRole("button", { name: recoveredSession })).toBeVisible();
  }
  expect(errors).toEqual([]);
});
