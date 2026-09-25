// Canonical public URLs, for everything that has to tell a machine where
// FoodAtlas lives: robots/sitemap, JSON-LD, the API `rel=alternate` links.
// Server-safe: no "use client", no env lookups.

export const SITE_URL = "https://www.foodatlas.ai";
export const API_URL = "https://api.foodatlas.ai";
export const DOWNLOADS_PATH = "/food-composition-downloads";

export type EntityType = "food" | "chemical" | "disease" | "bioactivity";
export const ENTITY_TYPES: EntityType[] = [
  "food",
  "chemical",
  "disease",
  "bioactivity",
];

// Sitemap ids, in the order `app/sitemap.ts` emits them: 0 is the static page
// list, then one per entity type. The index route and robots.txt both derive
// from this, so adding an entity type cannot leave a sitemap undiscoverable.
export const SITEMAP_IDS = [0, ...ENTITY_TYPES.map((_, i) => i + 1)];

export const sitemapPath = (id: number): string => `/sitemap/${id}.xml`;

// Public /v1 collection for each entity page type.
const API_COLLECTION: Record<EntityType, string> = {
  food: "foods",
  chemical: "chemicals",
  disease: "diseases",
  bioactivity: "bioactivities",
};

// Same slug rule the app uses for its own links (spaces → "--").
export const entityPath = (type: EntityType, commonName: string): string =>
  `/${type}/${encodeURIComponent(commonName.replace(/ /g, "--"))}`;

// The machine-readable twin of an entity page.
export const apiEntityUrl = (type: EntityType, foodatlasId: string): string =>
  `${API_URL}/v1/${API_COLLECTION[type]}/${encodeURIComponent(foodatlasId)}`;
