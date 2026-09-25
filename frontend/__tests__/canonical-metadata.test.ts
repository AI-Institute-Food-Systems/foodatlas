import { readFileSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import {
  ENTITY_TYPES,
  SITE_URL,
  canonicalUrl,
  entityPath,
} from "@/utils/site";

// Source-scanning guard, in the style of the other convention tests.
//
// Every indexable page needs a canonical, because query strings produce
// distinct URLs serving identical content: /food/cantaloupe and
// /food/cantaloupe?tab=composition&sources=fdc both returned 200 with the
// same <title> and no canonical, which is the duplicate-title report Search
// Console raises first.
//
// The /validation case is why this scans rather than spot-checks: it was
// listed in the sitemap, had no metadata at all, and so served no <title> on
// a page that sits behind a next-auth session. A test over "the pages I
// remembered to check" would not have caught it; one over "every path the
// sitemap advertises" does.

const ROOT = process.cwd();
const read = (p: string) => readFileSync(join(ROOT, p), "utf8");

const sitemapSrc = read("app/sitemap.ts");

// The static routes the sitemap advertises, read from the source of truth.
const staticPaths = (() => {
  const block = sitemapSrc.match(/const STATIC_PATHS = \[([\s\S]*?)\];/);
  if (!block) throw new Error("STATIC_PATHS not found in app/sitemap.ts");
  const out: string[] = [];
  const re = /"([^"]+)"/g;
  let m = re.exec(block[1]);
  while (m !== null) {
    out.push(m[1]);
    m = re.exec(block[1]);
  }
  return out;
})();

// Where each advertised route's metadata lives. A route in the sitemap with
// no entry here is a route nobody has claimed — the test below fails on it
// rather than skipping it.
const PAGE_FOR_PATH: Record<string, string> = {
  "/": "app/(home)/page.tsx",
  "/about": "app/(everything-else)/about/page.tsx",
  "/contact": "app/(everything-else)/contact/page.tsx",
  "/developers": "app/(everything-else)/developers/page.tsx",
  "/technical-background": "app/(everything-else)/technical-background/page.tsx",
  "/food-composition-api": "app/(everything-else)/food-composition-api/page.tsx",
  "/food-composition-downloads":
    "app/(everything-else)/food-composition-downloads/page.tsx",
};

describe("canonical URLs", () => {
  it("declares one for every static path the sitemap advertises", () => {
    for (const path of staticPaths) {
      const file = PAGE_FOR_PATH[path];
      expect(file, `no page mapped for sitemap path ${path}`).toBeDefined();
      expect(read(file)).toContain(`canonical: "${path}"`);
    }
  });

  it("declares one on every entity page", () => {
    for (const type of ENTITY_TYPES) {
      const src = read(`app/(everything-else)/${type}/[slug]/page.tsx`);
      expect(src, `${type} page`).toContain(`canonicalUrl("${type}"`);
    }
  });

  it("builds entity canonicals identically to sitemap entries", () => {
    // A canonical that disagrees with the sitemap is a contradictory signal,
    // so both must come out of entityPath.
    for (const type of ENTITY_TYPES) {
      const name = "cow milk (raw)";
      expect(canonicalUrl(type, name)).toBe(`${SITE_URL}${entityPath(type, name)}`);
    }
  });

  it("resolves relative canonicals via metadataBase", () => {
    // The static pages use root-relative values; without metadataBase Next
    // emits them against localhost.
    expect(read("app/layout.tsx")).toContain("metadataBase: new URL(SITE_URL)");
  });
});

describe("/validation is kept out of the index", () => {
  it("is not advertised in the sitemap", () => {
    expect(staticPaths).not.toContain("/validation");
  });

  it("is noindex", () => {
    const src = read("app/(everything-else)/validation/layout.tsx");
    expect(src).toContain("robots:");
    expect(src).toContain("index: false");
  });

  it("stays crawlable so the noindex is actually read", () => {
    // Disallowing it in robots.txt would stop Google fetching the page, so it
    // would never see the noindex and could still list a bare URL.
    expect(read("public/robots.txt")).not.toContain("Disallow: /validation");
  });
});
