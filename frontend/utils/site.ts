// Canonical public URLs, for everything that has to tell a machine where
// FoodAtlas lives: robots/sitemap, JSON-LD, the API `rel=alternate` links,
// and the per-page metadata block (buildMetadata).
// Server-safe: no "use client", no env lookups.
import type { Metadata } from "next";

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

// The one URL Google should index for an entity. Query strings (the
// composition filters, the tab id) produce distinct URLs serving the same
// page, so without this every filter combination looks like a duplicate.
// Built from entityPath so it is byte-identical to the sitemap entry — a
// canonical that disagrees with the sitemap is a contradictory signal.
export const canonicalUrl = (type: EntityType, commonName: string): string =>
  `${SITE_URL}${entityPath(type, commonName)}`;

// The machine-readable twin of an entity page.
export const apiEntityUrl = (type: EntityType, foodatlasId: string): string =>
  `${API_URL}/v1/${API_COLLECTION[type]}/${encodeURIComponent(foodatlasId)}`;

export const SITE_NAME = "FoodAtlas";
// Root layout's title.template. Pages give only their own part.
export const TITLE_SEPARATOR = " · ";
export const HOME_TITLE = `${SITE_NAME} | Evidence-Based Food Composition Database`;

// Google cuts titles at about 60 characters; keep each one under that.
export const MAX_TITLE = 60;

// app/opengraph-image.tsx, the site-wide share card. Named on every page,
// because Next replaces (does not merge) a parent's openGraph block, and a
// page that sets openGraph without images would lose the file-based one.
export const OG_IMAGE = {
  url: "/opengraph-image",
  width: 1200,
  height: 630,
  alt: "FoodAtlas: evidence-based food composition knowledge graph",
};

// Shortens `name` at a word boundary so `name + suffix + " · FoodAtlas"`
// fits MAX_TITLE. Long IUPAC chemical names otherwise ran to 91 characters.
export const fitTitle = (name: string, suffix: string): string => {
  const room = MAX_TITLE - TITLE_SEPARATOR.length - SITE_NAME.length - suffix.length;
  if (name.length <= room) return `${name}${suffix}`;
  const cut = name.slice(0, room - 1);
  const atWord = cut.lastIndexOf(" ");
  const head = atWord > room / 2 ? cut.slice(0, atWord) : cut;
  return `${head.replace(/[\s,;:(-]+$/, "")}…${suffix}`;
};

// One metadata block per page: title, description, canonical, the full
// openGraph block and a summary_large_image Twitter card. `path` is
// root-relative or absolute; metadataBase resolves it either way.
export const buildMetadata = ({
  title,
  description,
  path,
  absoluteTitle = false,
  jsonAlternate,
}: {
  title: string;
  description: string;
  path: string;
  // The home page's title already carries the brand.
  absoluteTitle?: boolean;
  // The machine-readable twin of the page, if it has one.
  jsonAlternate?: string;
}): Metadata => {
  const fullTitle = absoluteTitle ? title : `${title}${TITLE_SEPARATOR}${SITE_NAME}`;
  return {
    title: absoluteTitle ? { absolute: title } : title,
    description,
    alternates: {
      canonical: path,
      ...(jsonAlternate && { types: { "application/json": jsonAlternate } }),
    },
    openGraph: {
      type: "website",
      siteName: SITE_NAME,
      locale: "en_US",
      url: path,
      title: fullTitle,
      description,
      images: [OG_IMAGE],
    },
    twitter: {
      card: "summary_large_image",
      title: fullTitle,
      description,
      images: [OG_IMAGE.url],
    },
  };
};
