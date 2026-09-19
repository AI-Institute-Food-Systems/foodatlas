// Schema.org objects for the pages that describe the data itself. Kept as
// plain functions so the pages stay readable and a test can assert the shape.
import type { DownloadEntry } from "@/types";
import { CANONICAL_PUBLICATION, doiUrl } from "@/utils/publications";
import { API_URL, DOWNLOADS_PATH, SITE_URL } from "@/utils/site";

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
  potentialAction: {
    "@type": "SearchAction",
    target: {
      "@type": "EntryPoint",
      urlTemplate: `${SITE_URL}/results?term={search_term_string}`,
    },
    "query-input": "required name=search_term_string",
  },
});
