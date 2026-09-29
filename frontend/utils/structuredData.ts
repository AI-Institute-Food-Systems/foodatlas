// Schema.org objects for the pages that describe the data and its entities. Kept as
// plain functions so the pages stay readable and a test can assert the shape.
import type { DownloadEntry } from "@/types";
import type { Metadata as EntityMetadata } from "@/types/Metadata";
import { CANONICAL_PUBLICATION, doiUrl } from "@/utils/publications";
import {
  API_URL,
  DOWNLOADS_PATH,
  SITE_URL,
  canonicalUrl,
  type EntityType,
} from "@/utils/site";

// Downloads are gated like the API: free, but behind a key. Schema.org has
// a field for exactly that distinction, so crawlers don't try the link raw.
const ACCESS_CONDITIONS = `Free API key required; request one at ${SITE_URL}/contact?api-access`;

const ORGANIZATION = {
  "@type": "Organization",
  name: "AI Institute for Next Generation Food Systems (AIFS), UC Davis",
  url: "https://www.aifs.ucdavis.edu",
};

const LICENSE = "https://www.apache.org/licenses/LICENSE-2.0";

export const datasetJsonLd = (entries: DownloadEntry[]) => ({
  "@context": "https://schema.org",
  "@type": "Dataset",
  name: "FoodAtlas food composition knowledge graph",
  description:
    "Evidence-based knowledge graph of foods, chemicals, diseases and bioactivities, with every association traced to peer-reviewed sources. Versioned bundles of the full graph as parquet tables.",
  url: `${SITE_URL}${DOWNLOADS_PATH}`,
  license: LICENSE,
  isAccessibleForFree: true,
  conditionsOfAccess: ACCESS_CONDITIONS,
  creator: ORGANIZATION,
  citation: doiUrl(CANONICAL_PUBLICATION.doi),
  keywords: [
    "food composition",
    "knowledge graph",
    "nutrition",
    "bioactivity",
    "chemical-disease associations",
  ],
  version: entries[0]?.version,
  distribution: entries.map((e) => ({
    "@type": "DataDownload",
    name: `FoodAtlas ${e.version}`,
    // The gated hop, not the object: GET with a Bearer key → 302 to a
    // signed URL. Raw object URLs are private.
    contentUrl: `${API_URL}/v1/bundles/${e.version}/download`,
    encodingFormat: "application/zip",
    datePublished: e.release_date,
    contentSize: e.file_size,
  })),
});

export const webApiJsonLd = () => ({
  "@context": "https://schema.org",
  "@type": "WebAPI",
  name: "FoodAtlas API",
  description:
    "REST access to the FoodAtlas knowledge graph: foods, chemicals, diseases, bioactivities, triplets and attestations. JSON, paginated, bearer-key authenticated; keys are free.",
  url: `${API_URL}/v1`,
  documentation: `${API_URL}/docs`,
  termsOfService: `${SITE_URL}/developers`,
  conditionsOfAccess: ACCESS_CONDITIONS,
  provider: ORGANIZATION,
  license: LICENSE,
});

export const webSiteJsonLd = () => ({
  "@context": "https://schema.org",
  "@type": "WebSite",
  name: "FoodAtlas",
  url: SITE_URL,
  publisher: ORGANIZATION,
  // Google requires this target to be crawlable, so robots.txt no longer
  // disallows /results. The page carries noindex instead: crawlable so the
  // SearchAction can take effect, unindexed because a thin search-results page
  // does not belong in the index.
  potentialAction: {
    "@type": "SearchAction",
    target: {
      "@type": "EntryPoint",
      urlTemplate: `${SITE_URL}/results?term={search_term_string}`,
    },
    "query-input": "required name=search_term_string",
  },
});

// Entity pages render their tables client-side, so to a crawler that does not
// run the XHRs the page is a header and nothing else. This is the part of the
// entity it can still read. Types: the closest schema.org fit per entity; food
// and bioactivity have none, so they are terms in FoodAtlas's own vocabulary.
const ENTITY_SCHEMA_TYPE: Record<EntityType, string> = {
  food: "DefinedTerm",
  chemical: "MolecularEntity",
  disease: "MedicalCondition",
  bioactivity: "DefinedTerm",
};

// Chemicals carry hundreds of IUPAC variants; the head is what matters.
const MAX_ALTERNATE_NAMES = 25;

// Synonyms include the name itself and, for some foods, raw ontology IRIs
// ("<http://purl.obolibrary.org/...>") that are ids, not names.
const alternateNames = (m: EntityMetadata): string[] => {
  const own = m.common_name.toLowerCase();
  const names = m.synonyms.filter(
    (s) => s && s.toLowerCase() !== own && !/^<?https?:/i.test(s)
  );
  return Array.from(new Set(names)).slice(0, MAX_ALTERNATE_NAMES);
};

// The type comes from the route, not the payload: /bioactivity/metadata
// omits entity_type.
export const entityJsonLd = (type: EntityType, m: EntityMetadata) => {
  const url = canonicalUrl(type, m.common_name);
  const external = Object.values(m.external_ids).flatMap((source) =>
    source.ids.map((i) => ({ ...i, source: source.display_name }))
  );
  const sameAs = Array.from(
    new Set(external.map((i) => i.url).filter((u) => u && /^https?:/.test(u)))
  );
  const synonyms = alternateNames(m);
  const schemaType = ENTITY_SCHEMA_TYPE[type];

  return {
    "@context": "https://schema.org",
    "@type": schemaType,
    "@id": `${url}#entity`,
    name: m.common_name,
    ...(synonyms.length > 0 && { alternateName: synonyms }),
    ...(m.description && { description: m.description }),
    url,
    identifier: [
      { "@type": "PropertyValue", propertyID: "FoodAtlas", value: m.id },
      ...external.map((i) => ({
        "@type": "PropertyValue",
        propertyID: i.source,
        value: i.id,
      })),
    ],
    ...(sameAs.length > 0 && { sameAs }),
    ...(schemaType === "DefinedTerm" && {
      termCode: m.id,
      inDefinedTermSet: {
        "@type": "DefinedTermSet",
        name: `FoodAtlas ${type} vocabulary`,
        url: SITE_URL,
      },
    }),
  };
};
