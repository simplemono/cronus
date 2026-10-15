import { defineConfig } from "vitest/config";
import { cloudflareTest } from "@cloudflare/vitest-plugin";

// The Worker tests run inside workerd with a fresh local D1 per test; the
// targets are a fetch stub installed by the test. See test/worker/.
export default defineConfig({
  plugins: [
    cloudflareTest({
      wrangler: { configPath: "./wrangler.toml" },
      miniflare: { bindings: { CRONUS_SECRET: "test-secret" } },
    }),
  ],
  test: {
    include: ["dist/worker/*_test.mjs"],
  },
});
