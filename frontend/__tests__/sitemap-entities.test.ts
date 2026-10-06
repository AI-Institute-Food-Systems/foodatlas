import { afterEach, describe, expect, it, vi } from "vitest";

const getAllEntities = vi.fn();
vi.mock("@/utils/fetching", () => ({
  getAllEntities: (...a: unknown[]) => getAllEntities(...a),
  getLatestBundle: async () => ({ release_date: "2026-09-01" }),
}));

import sitemap from "@/app/sitemap";
import { SITE_URL } from "@/utils/site";

const row = (
  common_name: string,
  entity_type = "chemical",
  has_metadata?: boolean
) => ({
  foodatlas_id: "e1",
  entity_type,
  common_name,
  has_metadata,
});

afterEach(() => vi.clearAllMocks());

// The entity sitemaps list what the API's entity index returns (the same
// existence rule as requireEntity), except that only chemicals with foods are
// promoted; the other chemicals keep their pages.
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

  it("leaves out bioassay-only chemicals", async () => {
    getAllEntities.mockResolvedValue([
      row("quercetin", "chemical", true),
      row("zygosporamide", "chemical", false),
      row("olaparib"),
    ]);
    const urls = await sitemap({ id: 2 });
    expect(urls.map((u) => u.url)).toEqual([
      `${SITE_URL}/chemical/quercetin`,
      `${SITE_URL}/chemical/olaparib`,
    ]);
  });

  it("lists only chemicals with foods when the API flags them", async () => {
    getAllEntities.mockResolvedValue([
      { ...row("quercetin", "chemical", true), has_foods: true },
      { ...row("methotrexate", "chemical", true), has_foods: false },
      { ...row("zygosporamide", "chemical", false), has_foods: false },
    ]);
    const urls = await sitemap({ id: 2 });
    expect(urls.map((u) => u.url)).toEqual([`${SITE_URL}/chemical/quercetin`]);
  });

  it("keeps assay-only diseases", async () => {
    getAllEntities.mockResolvedValue([row("viremia", "disease", false)]);
    const urls = await sitemap({ id: 3 });
    expect(urls.map((u) => u.url)).toEqual([`${SITE_URL}/disease/viremia`]);
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
