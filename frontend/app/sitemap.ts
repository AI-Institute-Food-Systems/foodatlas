import type { MetadataRoute } from "next";

import { getAllEntities } from "@/utils/fetching";
import {
  ENTITY_TYPES,
  SITEMAP_IDS,
  SITE_URL,
  entityPath,
} from "@/utils/site";

// One sitemap per entity type plus one for the static pages, so a crawler
// that only wants foods fetches one file. Numeric ids are what Next 14's
// generateSitemaps supports: /sitemap/0.xml is static, 1–4 follow
// ENTITY_TYPES order (food, chemical, disease, bioactivity). robots.txt
// lists all five; keep the two in sync.
//
// Read from the API at request time (never at build, where there is no
// API_URL), with the entity index cached for a day by getAllEntities.
export const dynamic = "force-dynamic";

// lastmod was absent entirely, which with 9k URLs leaves Google no signal for
// what changed between injection runs. Per-entity timestamps are not available
// from /metadata/entities, so this is the coarse honest answer: the moment the
// sitemap was generated. The files revalidate daily, so it tracks reality
// within a day rather than claiming a precision we do not have.
const BUILD_TIME = new Date();

const STATIC_PATHS = [
  "/",
  "/about",
  "/developers",
  "/food-composition-downloads",
  // /food-composition-api 307-redirects to / (next.config.mjs), so listing it
  // submits a URL that resolves elsewhere — GSC reports that as a duplicate.
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
    return STATIC_PATHS.map((path) => ({
      url: `${SITE_URL}${path}`,
      lastModified: BUILD_TIME,
      changeFrequency: "monthly",
      priority: path === "/" ? 1 : 0.6,
    }));
  }
  const type = ENTITY_TYPES[id - 1];
  if (!type) return [];
  const entities = await getAllEntities();
  return entities
    .filter((e) => e.entity_type === type)
    .map((e) => ({
      url: `${SITE_URL}${entityPath(type, e.common_name)}`,
      lastModified: BUILD_TIME,
      changeFrequency: "monthly",
      priority: 0.5,
    }));
}
