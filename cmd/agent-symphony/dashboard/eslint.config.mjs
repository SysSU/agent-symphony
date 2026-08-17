import { defineConfig, globalIgnores } from "eslint/config";
import nextVitals from "eslint-config-next/core-web-vitals";
import aiGuard from "eslint-plugin-ai-guard";

export default defineConfig([
  ...nextVitals,
  {
    files: ["**/*.{js,mjs,jsx}"],
    plugins: { "ai-guard": aiGuard },
    rules: {
      "ai-guard/no-async-array-callback": "error",
      "ai-guard/no-dead-branch": "error",
      "ai-guard/no-floating-promise": "error",
    },
  },
  {
    files: ["app/**/*.{js,mjs,jsx}"],
    rules: {
      complexity: ["error", 15],
      "max-lines": ["error", { max: 250, skipBlankLines: true, skipComments: true }],
    },
  },
  globalIgnores([".next/**", "out/**"]),
]);
