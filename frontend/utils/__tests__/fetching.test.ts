import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { getMetaData } from "@/utils/fetching";

const ok = (body: unknown) =>
  ({
    ok: true,
    json: async () => body,
  }) as unknown as Response;

describe("getMetaData", () => {
  beforeEach(() => {
    vi.stubEnv("NEXT_PUBLIC_API_URL", "http://api.test");
    vi.stubEnv("NEXT_PUBLIC_API_KEY", "k");
  });

  afterEach(() => {
    vi.unstubAllEnvs();
    vi.restoreAllMocks();
  });

  it("returns null when the API returns an empty data array", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      ok({ data: [], metadata: { row_count: 0 } })
    );

    const result = await getMetaData("nonexistent", "food");

    expect(result).toBeNull();
  });

  it("returns the first entity when the API returns matches", async () => {
    const entity = {
      id: "FA-1",
      entity_type: "food",
      common_name: "tomato",
      scientific_name: "Solanum lycopersicum",
      synonyms: [],
      external_ids: {},
    };
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      ok({ data: [entity], metadata: { row_count: 1 } })
    );

    const result = await getMetaData("tomato", "food");

    expect(result).toEqual(entity);
  });

  it("returns null when the API responds with a non-ok status", async () => {
    // Staging is flaky; getMetaData swallows fetch failures and returns
    // null so generateMetadata() can fall back to notFound() instead of
    // throwing a 500 on the user's browser. See feedback-graceful-api-failures.
    vi.spyOn(globalThis, "fetch").mockResolvedValue({
      ok: false,
      json: async () => ({}),
    } as unknown as Response);

    const result = await getMetaData("tomato", "food");
    expect(result).toBeNull();
  });
});
