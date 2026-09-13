import { expect, test } from "@playwright/test";

const baseURL = process.env.AGENT_SYMPHONY_ORCHESTRATOR_E2E_URL;
test.skip(!baseURL, "run through the compiled orchestrator harness");
test.setTimeout(process.env.AGENT_SYMPHONY_FULL_SYSTEM_RACE === "true" ? 180_000 : 90_000);

test("operator controls the real supervised orchestrator and manual reconciliation", async ({ page }) => {
  const errors = [];
  const peerPosts = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("console", (message) => { if (message.type() === "error") errors.push(message.text()); });
  page.on("request", (request) => {
    if (request.method() === "POST" && request.url().startsWith(`${process.env.AGENT_SYMPHONY_ORCHESTRATOR_E2E_PEER}/actions/`)) peerPosts.push(request.url());
  });
  await page.goto(baseURL);
  const projects = page.getByRole("navigation", { name: "Project deployments" });
  await projects.getByRole("button", { name: "peer/project" }).click();
  await expect(page.getByRole("heading", { name: "peer/project" })).toBeVisible();
  const projectedPeer = page.getByRole("listitem").filter({ has: page.getByRole("link", { name: /#27\b/ }) });
  await expect(projectedPeer).toContainText("completed");
  await expect(page.getByRole("link", { name: "Open project dashboard" })).toHaveAttribute("href", process.env.AGENT_SYMPHONY_ORCHESTRATOR_E2E_PEER);
  await expect(projectedPeer.getByRole("button", { name: /Dismiss issue #27/ })).toHaveCount(0);
  await expect(projectedPeer.getByRole("button", { name: "Archive" })).toHaveCount(0);
  await page.getByRole("link", { name: "Open project dashboard" }).click();
  await expect(page).toHaveURL(process.env.AGENT_SYMPHONY_ORCHESTRATOR_E2E_PEER + "/");
  await expect(page.getByRole("heading", { name: "peer/project" })).toBeVisible();
  const linkedPeer = page.getByRole("listitem").filter({ has: page.getByRole("link", { name: /#27\b/ }) });
  await expect(linkedPeer.getByRole("button", { name: /Dismiss issue #27/ })).toBeVisible();
  await expect(linkedPeer.getByRole("button", { name: "Archive" })).toBeVisible();
  expect(peerPosts).toEqual([]);
  await page.goBack();
  await projects.getByRole("button", { name: "o/r" }).click();
  await expect(page.getByRole("heading", { name: "o/r" })).toBeVisible();
  const orchestrator = page.getByRole("region", { name: "Orchestrator" });
  await expect(orchestrator.getByRole("status", { name: "" })).toHaveText("Running");
  const initialGeneration = Number(process.env.AGENT_SYMPHONY_ORCHESTRATOR_E2E_GENERATION);
  const session = process.env.AGENT_SYMPHONY_ORCHESTRATOR_E2E_SESSION;
  const phase = process.env.AGENT_SYMPHONY_ORCHESTRATOR_E2E_PHASE;

  if (phase === "recover-held") {
    const responsePromise = page.waitForResponse((response) => response.url().endsWith("/actions/orchestrator/recover") && response.request().method() === "POST");
    page.once("dialog", (dialog) => dialog.accept());
    await orchestrator.getByRole("button", { name: "Recover/restart" }).click();
    const response = await responsePromise;
    const result = await response.json();
    expect(response.status(), JSON.stringify(result)).toBe(200);
    expect(result.status.generation).toBe(initialGeneration + 2);
    expect(result.status.context_mode).toBe("rebuild");
    await expect(orchestrator).toContainText(`Generation ${initialGeneration + 2} · rebuild`);
    expect(errors).toEqual([]);
    return;
  }

  if (["post-restart", "start-held", "fail-manual", "retry-manual"].includes(phase)) {
    await expect(orchestrator).toContainText(`Generation ${initialGeneration + 2} · rebuild`);
    const issue = page.getByRole("listitem").filter({ has: page.getByRole("link", { name: /#191\b/ }) });
    const investigate = page.waitForResponse((response) => response.url().includes("/actions/orchestrator/investigate?") && response.request().method() === "POST");
    await issue.getByRole("button", { name: "Ask orchestrator to investigate" }).click();
    const response = await investigate;
    expect(response.status(), await response.text()).toBe(200);
    await expect(page.getByRole("status").filter({ hasText: "Asked the orchestrator to investigate issue #191, attempt 9." })).toBeVisible();
    expect(errors).toEqual([]);
    return;
  }

  await orchestrator.getByRole("button", { name: "Open terminal" }).click();
  const terminal = page.getByRole("dialog", { name: session });
  await expect(terminal.getByRole("status")).toHaveText("Connected");
  await expect(terminal.locator(".xterm-rows")).toContainText("orchestrator-ready");
  await terminal.getByRole("button", { name: "Close" }).click();
  await expect(terminal).toBeHidden();
  await orchestrator.getByRole("button", { name: session }).click();
  await expect(terminal.getByRole("status")).toHaveText("Connected");
  await expect(terminal.locator(".xterm-rows")).toContainText("orchestrator-ready");
  await terminal.getByRole("button", { name: "Close" }).click();

  async function action(name, path, expectedGeneration, expectedMode) {
    const button = orchestrator.getByRole("button", { name });
    let posted = false;
    const onRequest = (request) => { if (request.url().endsWith(path) && request.method() === "POST") posted = true; };
    page.on("request", onRequest);
    page.once("dialog", (dialog) => dialog.dismiss());
    await button.click();
    expect(posted).toBe(false);
    const responsePromise = page.waitForResponse((response) => response.url().endsWith(path) && response.request().method() === "POST");
    page.once("dialog", (dialog) => dialog.accept());
    await button.click();
    const response = await responsePromise;
    const result = await response.json();
    expect(response.status(), JSON.stringify(result)).toBe(200);
    expect(result.ok).toBe(true);
    expect(result.status.generation).toBe(expectedGeneration);
    expect(result.status.context_mode).toBe(expectedMode);
    expect(result.status.session).toBe(session);
    await expect(orchestrator).toContainText(`Generation ${expectedGeneration} · ${expectedMode}`);
    page.off("request", onRequest);
    return result.status;
  }

  const mode = process.env.AGENT_SYMPHONY_ORCHESTRATOR_E2E_MODE;
  const recovered = await action("Recover/restart", "/actions/orchestrator/recover", initialGeneration, mode);
  expect(recovered.last_healthy_at).not.toBe(process.env.AGENT_SYMPHONY_ORCHESTRATOR_E2E_LAST_HEALTHY);
  await orchestrator.getByRole("button", { name: "Open terminal" }).click();
  await expect(terminal.getByRole("status")).toHaveText("Connected");
  await terminal.locator(".terminal textarea").pressSequentially("conversation-before-clear");
  await page.keyboard.press("Enter");
  await terminal.getByRole("button", { name: "Close" }).click();
  await action("Clear context", "/actions/orchestrator/clear", initialGeneration + 1, "clear");
  await orchestrator.getByRole("button", { name: "Open terminal" }).click();
  await expect(terminal.locator(".xterm-rows")).toContainText("orchestrator-projection-absent");
  await terminal.getByRole("button", { name: "Close" }).click();
  await action("Rebuild context", "/actions/orchestrator/rebuild", initialGeneration + 2, "rebuild");
  await orchestrator.getByRole("button", { name: "Open terminal" }).click();
  await expect(terminal.locator(".xterm-rows")).toContainText("orchestrator-projection-present");
  await terminal.getByRole("button", { name: "Close" }).click();

  const reconcile = page.waitForResponse((response) => response.url().endsWith("/actions/reconcile") && response.request().method() === "POST");
  await page.getByRole("button", { name: "Check now" }).click();
  expect((await reconcile).status()).toBe(204);
  await expect(page.getByRole("status").filter({ hasText: "Reconciliation completed." })).toBeVisible();
  expect(errors).toEqual([]);
});
