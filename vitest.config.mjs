import { defineConfig } from "vitest/config";
import { cloudflareTest, readD1Migrations } from "@cloudflare/vitest-plugin";

// The Worker tests run inside workerd with a local D1 to which the test
// applies the migrations; the targets are a fetch stub installed by the
// test. See test/worker/.
export default defineConfig(async () => {
  const migrations = await readD1Migrations("./migrations");
  return {
    plugins: [
      cloudflareTest({
        wrangler: { configPath: "./wrangler.toml" },
        miniflare: { bindings: { CRONUS_SECRET: "test-secret", TEST_MIGRATIONS: migrations } },
      }),
    ],
    test: {
      include: ["dist/worker/*_test.mjs"],
    },
  };
});
