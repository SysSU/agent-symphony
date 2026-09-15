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
  const exactAttempt = page.getByRole("listitem").filter({ has: page.getByRole("link", { name: /#73\b/ }) }).filter({ hasText: /Attempt 1(?!\d)/ });
  const historicalAttempt = async () => {
    const history = page.locator("details.attemptHistory");
    await expect(history).toBeVisible();
    if (await history.getAttribute("open") === null) await history.locator("summary").click();
    await expect(history).toHaveAttribute("open", "");
    return history.getByRole("listitem").filter({ hasText: /Attempt 1(?!\d)/ });
  };
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
    "archive-overlap": card.getByRole("button", { name: "Archive" }),
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
    const reason = { archive: "archived", dismiss: "dismissed", abandon: "abandoned" }[requestAction];
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
    const current = await page.request.get(`${baseURL}/status.json`).then((result) => result.json());
    const old = current.statuses?.find((entry) => entry.issue === 73 && entry.attempt === 1);
    const next = current.statuses?.find((entry) => entry.issue === 73 && entry.attempt === 2);
    if (next && !/Attempt 2(?!\d)/.test(await card.innerText())) await page.reload();
    const canceledCard = /Attempt 2(?!\d)/.test(await card.innerText()) ? await historicalAttempt() : exactAttempt;
    await expect(canceledCard).toContainText(/cancelled|failed/);
    if (next) {
      expect(next.session).toBeTruthy();
      expect(next.session).not.toBe(old?.session);
      await expect(card).toContainText(/Attempt 2(?!\d)/);
      await expect(canceledCard.getByRole("button", { name: "Recover attempt" })).toHaveCount(0);
    }
  }
  if (requestAction === "review-plan") {
    let reviewer;
    await expect.poll(async () => {
      const status = await fetch(`${baseURL}/status.json`, { cache: "no-store" }).then((result) => result.json());
      reviewer = status.statuses?.find((entry) => entry.issue === 73 && entry.attempt === 1)?.sessions?.find((session) => session.role === "reviewer" && session.mode === "plan-review");
      return reviewer?.state;
    }, { timeout: 20_000 }).toBe("running");
    expect(reviewer.name).toMatch(/^as-/);
    const reviewerButton = card.getByRole("button", { name: "Reviewer terminal unavailable; show why" });
    await expect(reviewerButton).toBeVisible();
    await reviewerButton.click();
    await expect(page.getByRole("status").filter({ hasText: "Reviewer terminal is unavailable until session identity can be verified safely." })).toBeVisible();
    await expect(page.getByRole("dialog")).toHaveCount(0);
    const terminal = await page.request.get(`${baseURL}/reviewer/terminal?repository=o%2Fr&issue=73&attempt=1`, { headers: { Origin: baseURL } });
    expect(terminal.status()).toBe(409);
    expect(await terminal.text()).toContain("session identity can be verified safely");
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
      await expect(card).toBeVisible();
      let canceledHistorical = /Attempt 2(?!\d)/.test(await card.innerText());
      let canceledCard = canceledHistorical ? await historicalAttempt() : exactAttempt;
      try {
        await expect(canceledCard).toContainText("failed");
      } catch {
        canceledHistorical = true;
        canceledCard = await historicalAttempt();
        await expect(canceledCard).toContainText("failed");
      }
      const status = await page.request.get(`${baseURL}/status.json`).then((response) => response.json());
      const old = status.statuses?.find((entry) => entry.issue === 73 && entry.attempt === 1);
      const next = status.statuses?.find((entry) => entry.issue === 73 && entry.attempt === 2);
      if (next) {
        expect(next.session).toBeTruthy();
        expect(next.session).not.toBe(old?.session);
        await page.reload();
        await expect(card).toContainText(/Attempt 2(?!\d)/);
        canceledCard = await historicalAttempt();
        await expect(canceledCard).toContainText("failed");
        await expect(canceledCard.getByRole("button", { name: "Recover attempt" })).toHaveCount(0);
      } else if (old?.retryable && !old.operator_blocked) {
        try {
          await expect(canceledCard.getByRole("button", { name: "Recover attempt" })).toBeVisible();
        } catch (error) {
          let detail = `historical=${canceledHistorical} original_old=${JSON.stringify({ state: old.state, retryable: old.retryable, operator_blocked: old.operator_blocked })} original_next=${next?.state ?? "none"}`;
          try {
            const fresh = await page.request.get(`${baseURL}/status.json`, { timeout: 1_000 }).then((response) => response.json());
            const freshOld = fresh.statuses?.find((entry) => entry.issue === 73 && entry.attempt === 1);
            const freshNext = fresh.statuses?.find((entry) => entry.issue === 73 && entry.attempt === 2);
            const currentAttempt2 = /Attempt 2(?!\d)/.test(await card.innerText({ timeout: 1_000 }));
            detail += ` current_attempt_2=${currentAttempt2} fresh_old=${JSON.stringify({ state: freshOld?.state, retryable: freshOld?.retryable, operator_blocked: freshOld?.operator_blocked })} fresh_next=${freshNext?.state ?? "none"}`;
          } catch (diagnosticError) {
            detail += ` diagnostic_error=${String(diagnosticError)}`;
          }
          error.message += `\nRecover button diagnostic: ${detail}`;
          throw error;
        }
      }
      await expect(canceledCard.getByRole("button", { name: "Reviewer terminal unavailable; show why" })).toHaveCount(0);
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
