import { render } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import JsonLd from "@/components/misc/JsonLd";
import { apiEntityUrl, entityPath } from "@/utils/site";
import {
  datasetJsonLd,
  webApiJsonLd,
  webSiteJsonLd,
} from "@/utils/structuredData";

const getAllEntities = vi.fn();
vi.mock("@/utils/fetching", () => ({
  getAllEntities: () => getAllEntities(),
}));

import sitemap, { generateSitemaps } from "@/app/sitemap";

const entry = {
  version: "v4.12",
  release_date: "2026-09-18",
  file_size: "1.2 GB",
  kgc_run: "20260918T100923Z",
  download_link: "https://downloads.example/bundles/v4.12.zip",
  summary_link: "https://downloads.example/bundles/v4.12.md",
};

afterEach(() => vi.clearAllMocks());

describe("site urls", () => {
  it("builds entity paths with the app's slug rule", () => {
    expect(entityPath("food", "cow milk (raw)")).toBe(
      "/food/cow--milk--(raw)"
    );
  });

  it("maps entity types to their /v1 collections", () => {
    expect(apiEntityUrl("food", "e12")).toBe(
      "https://api.foodatlas.ai/v1/foods/e12"
    );
    expect(apiEntityUrl("bioactivity", "e7")).toBe(
      "https://api.foodatlas.ai/v1/bioactivities/e7"
    );
  });
});

describe("structured data", () => {
  it("describes the bundles as a schema.org Dataset with one DataDownload per version", () => {
    const d = datasetJsonLd([entry]);
    expect(d["@type"]).toBe("Dataset");
    expect(d.license).toBe("https://www.apache.org/licenses/LICENSE-2.0");
    expect(d.isAccessibleForFree).toBe(true);
    expect(d.version).toBe("v4.12");
    expect(d.conditionsOfAccess).toMatch(/API key required/);
    expect(d.distribution).toEqual([
      expect.objectContaining({
        "@type": "DataDownload",
        // Gated hop, never the raw object URL.
        contentUrl: "https://api.foodatlas.ai/v1/bundles/v4.12/download",
        encodingFormat: "application/zip",
        datePublished: "2026-09-18",
      }),
    ]);
    expect(JSON.stringify(d)).not.toContain("downloads.example");
    expect(d.citation).toMatch(/^https:\/\/doi\.org\//);
  });

  it("points the WebAPI at the OpenAPI docs", () => {
    const a = webApiJsonLd();
    expect(a["@type"]).toBe("WebAPI");
    expect(a.url).toBe("https://api.foodatlas.ai/v1");
    expect(a.documentation).toBe("https://api.foodatlas.ai/docs");
  });

  it("gives the WebSite a SearchAction on /results", () => {
    const w = webSiteJsonLd();
    expect(w.potentialAction.target.urlTemplate).toContain(
      "/results?term={search_term_string}"
    );
  });

  it("JsonLd renders a script tag and escapes '<'", () => {
    const { container } = render(<JsonLd data={{ name: "<x>" }} />);
    const script = container.querySelector('script[type="application/ld+json"]');
    expect(script).not.toBeNull();
    expect(script!.innerHTML).toContain("\\u003cx>");
    expect(script!.innerHTML).not.toContain("<x>");
  });
});

describe("sitemap", () => {
  it("emits one static sitemap plus one per entity type", async () => {
    expect(await generateSitemaps()).toEqual([
      { id: 0 },
      { id: 1 },
      { id: 2 },
      { id: 3 },
      { id: 4 },
    ]);
  });

  it("static sitemap lists the site pages with the home page first", async () => {
    const rows = await sitemap({ id: 0 });
    expect(rows[0]).toMatchObject({ url: "https://www.foodatlas.ai/", priority: 1 });
    expect(rows.map((r) => r.url)).toContain(
      "https://www.foodatlas.ai/food-composition-downloads"
    );
    expect(getAllEntities).not.toHaveBeenCalled();
  });

  it("entity sitemaps filter the index by type and use slug paths", async () => {
    getAllEntities.mockResolvedValue([
      { foodatlas_id: "e1", entity_type: "food", common_name: "cow milk" },
      { foodatlas_id: "e2", entity_type: "chemical", common_name: "quercetin" },
    ]);
    const foods = await sitemap({ id: 1 });
    expect(foods.map((r) => r.url)).toEqual([
      "https://www.foodatlas.ai/food/cow--milk",
    ]);
    const chemicals = await sitemap({ id: 2 });
    expect(chemicals.map((r) => r.url)).toEqual([
      "https://www.foodatlas.ai/chemical/quercetin",
    ]);
  });

  it("is empty, not broken, when the index is unavailable", async () => {
    getAllEntities.mockResolvedValue([]);
    expect(await sitemap({ id: 3 })).toEqual([]);
    expect(await sitemap({ id: 99 })).toEqual([]);
  });
});
