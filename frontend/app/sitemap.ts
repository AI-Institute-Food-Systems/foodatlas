import type { MetadataRoute } from "next";

import { getAllEntities } from "@/utils/fetching";
import { ENTITY_TYPES, SITE_URL, entityPath } from "@/utils/site";

// One sitemap per entity type plus one for the static pages, so a crawler
// that only wants foods fetches one file. Numeric ids are what Next 14's
// generateSitemaps supports: /sitemap/0.xml is static, 1–4 follow
// ENTITY_TYPES order (food, chemical, disease, bioactivity). robots.txt
// lists all five; keep the two in sync.
//
// Read from the API at request time (never at build, where there is no
// API_URL), with the entity index cached for a day by getAllEntities.
export const dynamic = "force-dynamic";

const STATIC_PATHS = [
  "/",
  "/about",
  "/developers",
  "/food-composition-downloads",
  "/food-composition-api",
  "/technical-background",
  "/validation",
  "/contact",
];

export async function generateSitemaps() {
  return [0, ...ENTITY_TYPES.map((_, i) => i + 1)].map((id) => ({ id }));
}

export default async function sitemap({
  id,
}: {
  id: number;
}): Promise<MetadataRoute.Sitemap> {
  if (id === 0) {
    return STATIC_PATHS.map((path) => ({
      url: `${SITE_URL}${path}`,
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
      changeFrequency: "monthly",
      priority: 0.5,
    }));
}
