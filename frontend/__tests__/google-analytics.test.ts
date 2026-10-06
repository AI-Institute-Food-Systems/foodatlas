import { afterEach, describe, expect, it, vi } from "vitest";

// GA rendered on every build, even with no id (K4). It now follows umami:
// production deploys only, and only with a measurement id.
const load = async (env: Record<string, string | undefined>) => {
  vi.resetModules();
  for (const [k, v] of Object.entries(env)) vi.stubEnv(k, v);
  return import("@/utils/googleAnalytics");
};

afterEach(() => vi.unstubAllEnvs());

describe("Google Analytics gate", () => {
  it("is on in production with an id", async () => {
    const ga = await load({
      VERCEL_ENV: "production",
      NEXT_PUBLIC_GA_MEASUREMENT_ID: "G-TEST",
    });
    expect(ga.GA_ENABLED).toBe(true);
    expect(ga.GA_MEASUREMENT_ID).toBe("G-TEST");
  });

  it("is off on previews", async () => {
    const ga = await load({
      VERCEL_ENV: "preview",
      NEXT_PUBLIC_GA_MEASUREMENT_ID: "G-TEST",
    });
    expect(ga.GA_ENABLED).toBe(false);
  });

  it("is off without an id", async () => {
    const ga = await load({
      VERCEL_ENV: "production",
      NEXT_PUBLIC_GA_MEASUREMENT_ID: "",
    });
    expect(ga.GA_ENABLED).toBe(false);
  });
});
