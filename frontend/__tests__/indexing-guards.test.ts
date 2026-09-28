import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

// Source-scanning guards for the indexing decisions the SEO audit surfaced.
// Each pins a property that was measured broken on prod, so a regression fails
// here rather than in Search Console weeks later.

const ROOT = process.cwd();
const read = (p: string) => readFileSync(join(ROOT, p), "utf8");

const robots = read("public/robots.txt");
const sitemap = read("app/sitemap.ts");
const nextConfig = read("next.config.mjs");

describe("soft-404 surface", () => {
  it("chemical pages noindex an unknown slug", () => {
    // /chemical/<anything> returned 200 with a real title, description and a
    // self-canonical — an unbounded indexable surface. It is the one entity
    // route that deliberately renders without metaData, so noindex is the fix
    // rather than notFound().
    const src = read("app/(everything-else)/chemical/[slug]/page.tsx");
    expect(src).toContain("index: false");
  });
});

describe("sitemap advertises only URLs that resolve", () => {
  it("omits paths that next.config redirects away", () => {
    const redirected: string[] = [];
    const re = /source:\s*"([^"]+)"/g;
    let m = re.exec(nextConfig);
    while (m !== null) {
      if (!redirected.includes(m[1])) redirected.push(m[1]);
      m = re.exec(nextConfig);
    }
    // Every redirect source must be absent from STATIC_PATHS: submitting a URL
    // that 3xxs elsewhere is what GSC reports as a duplicate.
    for (const src of redirected) {
      expect(sitemap.includes(`"${src}"`), `${src} is a redirect source`).toBe(
        false
      );
    }
  });

  it("emits lastmod", () => {
    expect(sitemap).toContain("lastModified");
  });
});

describe("parameterised links stay out of the crawl", () => {
  it("disallows the chemical→food ?highlight= links", () => {
    // Every composition row on a chemical page links to its food with a
    // highlight param — ~200k crawlable URLs, one per food-chemical pair.
    const table = read("components/entities/chemical/ChemicalCompositionTable.tsx");
    expect(table).toContain("?highlight=");
    expect(robots).toContain("Disallow: /*?highlight=");
    expect(robots).toContain("Disallow: /*&highlight=");
  });

  it("no other internal link carries a query param", () => {
    // Any new param in an internal URL needs the decision ?highlight= got.
    // /results?term= must stay crawlable (SearchAction); /contact?api-access
    // is a single URL with a canonical.
    const reviewed = ["api-access", "highlight", "term"];
    const params = new Set<string>();
    for (const dir of ["app", "components"]) {
      for (const f of readdirSync(join(ROOT, dir), { recursive: true })) {
        if (!String(f).endsWith(".tsx")) continue;
        const src = read(join(dir, String(f)));
        // A string literal that starts as a site path or bare query string.
        const re = /["`](?:\/[^"`\s]*?)?\?([a-z_-]+)/g;
        let m = re.exec(src);
        while (m !== null) {
          params.add(m[1]);
          m = re.exec(src);
        }
      }
    }
    expect(Array.from(params).sort()).toEqual(reviewed);
  });
});

describe("SearchAction target is reachable", () => {
  it("does not disallow the URL the SearchAction points at", () => {
    const structured = read("utils/structuredData.ts");
    expect(structured).toContain("/results?term={search_term_string}");
    // Google only honours a crawlable sitelinks-searchbox target.
    expect(robots).not.toContain("Disallow: /results");
  });

  it("keeps the results page out of the index without blocking the crawl", () => {
    const layout = read("app/(everything-else)/results/layout.tsx");
    expect(layout).toContain("index: false");
    expect(layout).toContain("follow: true");
  });
});

describe("llms.txt matches reality", () => {
  it("does not advertise the noindexed validation tool", () => {
    expect(read("public/llms.txt")).not.toContain("/validation");
  });
});

describe("middleware redirect cannot leave the site", () => {
  it("allowlists the upstream entity_type before building the URL", () => {
    const src = read("middleware.ts");
    // entity_type was interpolated unencoded, so "/evil.com" from a hostile
    // upstream would have produced a protocol-relative open redirect.
    const afterJson = src.slice(src.indexOf("await res.json()"));
    expect(afterJson).toContain("ENTITY_ROUTES.has(entity_type)");
  });
});
