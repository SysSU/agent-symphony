import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./browser",
  use: {
    baseURL: "http://127.0.0.1:33131",
  },
  webServer: {
    command: "npm run dev -- --hostname 127.0.0.1 --port 33131",
    url: "http://127.0.0.1:33131",
    reuseExistingServer: false,
  },
});
