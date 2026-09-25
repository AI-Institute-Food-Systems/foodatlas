import { SITEMAP_IDS, SITE_URL, sitemapPath } from "@/utils/site";

// Sitemap index. `app/sitemap.ts` uses generateSitemaps(), which makes Next
// serve the individual files at /sitemap/{id}.xml and leaves /sitemap.xml
// itself a 404 — but that is the path crawlers probe by convention, and the
// one you hand to Google Search Console. This handler fills it with a
// <sitemapindex> pointing at the per-type files.
//
// Ids come from SITEMAP_IDS, the same list generateSitemaps() maps over, so
// the index cannot fall out of step with what actually exists.
//
// Static on purpose: the id list is fixed at build time even though the
// entity sitemaps themselves revalidate daily. Nothing here touches the API.
export const dynamic = "force-static";

export function GET(): Response {
  const entries = SITEMAP_IDS.map(
    (id) => `  <sitemap><loc>${SITE_URL}${sitemapPath(id)}</loc></sitemap>`
  ).join("\n");

  const body = `<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${entries}
</sitemapindex>
`;

  return new Response(body, {
    headers: {
      "content-type": "application/xml",
      "cache-control": "public, max-age=0, s-maxage=86400",
    },
  });
}
