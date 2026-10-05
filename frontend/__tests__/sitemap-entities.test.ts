import { afterEach, describe, expect, it, vi } from "vitest";

const getAllEntities = vi.fn();
vi.mock("@/utils/fetching", () => ({
  getAllEntities: (...a: unknown[]) => getAllEntities(...a),
  getLatestBundle: async () => ({ release_date: "2026-09-01" }),
}));

import sitemap from "@/app/sitemap";
import { SITE_URL } from "@/utils/site";

const row = (common_name: string, entity_type = "chemical") => ({
  foodatlas_id: "e1",
  entity_type,
  common_name,
});

afterEach(() => vi.clearAllMocks());

// The entity sitemaps list what the API's entity index returns, which uses
// the same existence rule as requireEntity: a bioassay-only chemical has a
// page, so it must be in the sitemap too.
describe("entity sitemaps", () => {
  it("asks the index for its own type only", async () => {
    getAllEntities.mockResolvedValue([row("olaparib")]);
    const urls = await sitemap({ id: 2 });
    expect(getAllEntities).toHaveBeenCalledWith("chemical");
    expect(urls.map((u) => u.url)).toEqual([`${SITE_URL}/chemical/olaparib`]);
  });

  it("drops rows of other types from an API that ignores the filter", async () => {
    getAllEntities.mockResolvedValue([row("tomato", "food"), row("quercetin")]);
    const urls = await sitemap({ id: 2 });
    expect(urls.map((u) => u.url)).toEqual([`${SITE_URL}/chemical/quercetin`]);
  });

  it("lists each URL once", async () => {
    getAllEntities.mockResolvedValue([row("vitamin c"), row("vitamin c")]);
    const urls = await sitemap({ id: 2 });
    expect(urls).toHaveLength(1);
    expect(urls[0].url).toBe(`${SITE_URL}/chemical/vitamin--c`);
  });

  it("stays within the 50,000-URL protocol limit", async () => {
    const error = vi.spyOn(console, "error").mockImplementation(() => {});
    getAllEntities.mockResolvedValue(
      Array.from({ length: 50_002 }, (_, i) => row(`c${i}`))
    );
    const urls = await sitemap({ id: 2 });
    expect(urls).toHaveLength(50_000);
    expect(urls[0].url).toBe(`${SITE_URL}/chemical/c0`);
    expect(error).toHaveBeenCalledOnce();
    error.mockRestore();
  });
});
