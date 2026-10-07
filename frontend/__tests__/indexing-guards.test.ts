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
  it.each(["food", "chemical", "disease", "bioactivity"])(
    "%s slugs are checked above the loading boundary",
    (type) => {
      // notFound() below loading.tsx lands after the 200 has streamed, which
      // GSC reports as a soft 404. The check has to sit in the layout.
      const dir = `app/(everything-else)/${type}/[slug]`;
      expect(read(`${dir}/loading.tsx`)).toBeTruthy();
      expect(read(`${dir}/layout.tsx`)).toContain(
        `await requireEntity("${type}", params.slug)`
      );
    }
  );

  it("has a not-found boundary inside the route group", () => {
    // Without it the layout-level 404 renders bare, with no site nav.
    expect(read("app/(everything-else)/not-found.tsx")).toContain("not-found");
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
  it("keeps the chemical→food highlight out of the query string", () => {
    // Every composition row on a chemical page links to its food. As a
    // ?highlight= param that was ~200k crawlable URLs, one per food-chemical
    // pair, and blocking them in robots.txt left chemical pages with no
    // crawlable food links at all. The fragment keeps the link bare.
    const table = read("components/entities/chemical/ChemicalCompositionTable.tsx");
    expect(table).not.toContain("?highlight=");
    expect(table).toContain("foodHighlightHref(");
    // Links already out there keep the old form; they stay blocked.
    expect(robots).toContain("Disallow: /*?highlight=");
    expect(robots).toContain("Disallow: /*&highlight=");
  });

  it("no other internal link carries a query param", () => {
    // Any new param in an internal URL needs the decision ?highlight= got
    // (moved to the fragment). /results?term= is noindex but crawlable;
    // /contact?api-access is a single URL with a canonical.
    const reviewed = ["api-access", "term"];
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

describe("/results stays noindex and crawlable", () => {
  it("is not disallowed, so Google can read its noindex", () => {
    expect(robots).not.toContain("Disallow: /results");
  });

  it("keeps the results page out of the index without blocking the crawl", () => {
    const layout = read("app/(everything-else)/results/layout.tsx");
    expect(layout).toContain("index: false");
    expect(layout).toContain("follow: true");
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
