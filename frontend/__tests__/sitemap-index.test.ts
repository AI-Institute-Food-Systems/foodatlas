import { readFileSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { GET } from "@/app/sitemap.xml/route";
import { generateSitemaps } from "@/app/sitemap";
import { ENTITY_TYPES, SITEMAP_IDS, SITE_URL, sitemapPath } from "@/utils/site";

// The sitemap id list lives in three places that a crawler has to agree on:
// generateSitemaps() (what Next actually serves), the index route (what
// crawlers read), and public/robots.txt (how they find it at all). Before
// this, robots.txt hardcoded five URLs with a "keep the two in sync" comment
// — exactly the arrangement that goes stale silently. These pin the contract
// rather than the current count, so adding an entity type either keeps
// everything consistent or fails here.

const robots = readFileSync(
  join(process.cwd(), "public", "robots.txt"),
  "utf8"
);

const locs = (xml: string): string[] => {
  const out: string[] = [];
  const re = /<loc>([^<]+)<\/loc>/g;
  let m = re.exec(xml);
  while (m !== null) {
    out.push(m[1]);
    m = re.exec(xml);
  }
  return out;
};

describe("sitemap index", () => {
  it("lists exactly the sitemaps Next generates", async () => {
    const generated = (await generateSitemaps()).map((s) => s.id);
    expect(generated).toEqual(SITEMAP_IDS);

    const xml = await GET().text();
    expect(locs(xml)).toEqual(
      SITEMAP_IDS.map((id) => `${SITE_URL}${sitemapPath(id)}`)
    );
  });

  it("covers the static page list plus every entity type", () => {
    expect(SITEMAP_IDS).toHaveLength(ENTITY_TYPES.length + 1);
    expect(SITEMAP_IDS[0]).toBe(0);
  });

  it("is well-formed sitemapindex XML, not a urlset", async () => {
    const xml = await GET().text();
    expect(xml.startsWith('<?xml version="1.0" encoding="UTF-8"?>')).toBe(true);
    expect(xml).toContain(
      '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
    );
    expect(xml).toContain("</sitemapindex>");
    expect(xml).not.toContain("<urlset");
    // <loc> belongs to <sitemap> entries here; a stray <url> would mean the
    // index was built from the page-sitemap shape by mistake.
    expect(xml).not.toContain("<url>");
  });

  it("serves XML", async () => {
    expect(GET().headers.get("content-type")).toBe("application/xml");
  });

  it("is advertised in robots.txt", () => {
    expect(robots).toContain(`Sitemap: ${SITE_URL}/sitemap.xml`);
  });

  it("does not leave per-id Sitemap lines in robots.txt to go stale", () => {
    const sitemapLines = robots
      .split("\n")
      .filter((l) => l.trim().toLowerCase().startsWith("sitemap:"));
    expect(sitemapLines).toHaveLength(1);
  });

  it("keeps /_proxy-api disallowed now that it is unauthenticated", () => {
    expect(robots).toContain("Disallow: /_proxy-api/");
  });
});
