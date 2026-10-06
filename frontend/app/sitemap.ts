import type { MetadataRoute } from "next";

import { getAllEntities, getLatestBundle } from "@/utils/fetching";
import {
  ENTITY_TYPES,
  SITEMAP_IDS,
  SITE_URL,
  entityPath,
} from "@/utils/site";

// One sitemap per entity type plus one for the static pages, so a crawler
// that only wants foods fetches one file. Numeric ids are what Next 14's
// generateSitemaps supports: /sitemap/0.xml is static, 1–4 follow
// ENTITY_TYPES order (food, chemical, disease, bioactivity). The ids come
// from SITEMAP_IDS, shared with the /sitemap.xml index that robots.txt lists.
//
// Read from the API at request time (never at build, where there is no
// API_URL), with the entity index cached for a day by getAllEntities.
export const dynamic = "force-dynamic";

const MAX_SITEMAP_URLS = 50_000;

// Entity pages change only when a new dataset ships, so their lastmod is the
// latest bundle's release date. Stamping the generation time instead claimed
// every page changed daily, and Google learns to ignore a lastmod that always
// moves. Per-entity timestamps are not available from /metadata/entities.
// Omitted when the bundle list is unreachable, rather than guessed.
async function datasetReleaseDate(): Promise<Date | undefined> {
  const release = (await getLatestBundle())?.release_date;
  return release ? new Date(release) : undefined;
}

const STATIC_PATHS = [
  "/",
  "/about",
  "/developers",
  "/food-composition-downloads",
  // Never list a redirect source from next.config.mjs (e.g. the old
  // /food-composition-api): GSC reports a URL that resolves elsewhere as a
  // page with redirect.
  "/technical-background",
  // /validation is the auth-gated internal curation tool — noindex, and it
  // has no business being advertised to crawlers. See its layout.tsx.
  "/contact",
];

export async function generateSitemaps() {
  return SITEMAP_IDS.map((id) => ({ id }));
}

export default async function sitemap({
  id,
}: {
  id: number;
}): Promise<MetadataRoute.Sitemap> {
  if (id === 0) {
    // No lastmod: these change with deploys, not datasets, and no date we
    // have tracks that.
    return STATIC_PATHS.map((path) => ({
      url: `${SITE_URL}${path}`,
      changeFrequency: "monthly",
      priority: path === "/" ? 1 : 0.6,
    }));
  }
  const type = ENTITY_TYPES[id - 1];
  if (!type) return [];
  const [entities, lastModified] = await Promise.all([
    getAllEntities(type),
    datasetReleaseDate(),
  ]);
  // The filter stays for an API that ignores ?entity_type. Two names can
  // map to one slug, and a sitemap must not repeat a URL.
  const urls = new Set(
    entities
      .filter((e) => e.entity_type === type)
      // Bioassay-only chemicals (~40k, no food data) keep their pages but are
      // not promoted: they would compete for crawl budget with the ~5.4k
      // chemicals that have food composition. Decided 2026-10-06.
      .filter((e) => !(type === "chemical" && e.has_metadata === false))
      .map((e) => `${SITE_URL}${entityPath(type, e.common_name)}`)
  );
  // The protocol caps one file at 50,000 URLs. The API lists metadata-backed
  // entities first, so a cut drops the thinnest pages (bioassay-only
  // chemicals). Logged, because hitting it means this file needs splitting.
  if (urls.size > MAX_SITEMAP_URLS) {
    console.error(
      `sitemap ${type}: ${urls.size} URLs, cut to ${MAX_SITEMAP_URLS}`
    );
  }
  return Array.from(urls)
    .slice(0, MAX_SITEMAP_URLS)
    .map((url) => ({
      url,
      lastModified,
      changeFrequency: "monthly",
      priority: 0.5,
    }));
}
