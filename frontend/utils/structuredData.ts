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

// The institute that builds and funds FoodAtlas. Its profiles are the ones
// in the site footer.
const ORGANIZATION = {
  "@type": "Organization",
  name: "AI Institute for Next Generation Food Systems (AIFS), UC Davis",
  url: "https://www.aifs.ucdavis.edu",
  sameAs: [
    "https://www.linkedin.com/company/aifoodsystems/",
    "https://www.youtube.com/channel/UCyvVBZ6Qx34ElPB0UmoEF2A",
    "https://www.instagram.com/aifoodsystems",
    "https://www.threads.net/@aifoodsystems",
  ],
};

// FoodAtlas itself, for Google's logo and site-name features. Inline, not
// an @id reference: Google does not resolve an @id declared in another
// JSON-LD block. The logo is a raster ≥112 px, as Google requires.
const FOODATLAS = {
  "@type": "Organization",
  "@id": `${SITE_URL}/#organization`,
  name: "FoodAtlas",
  url: SITE_URL,
  logo: {
    "@type": "ImageObject",
    url: `${SITE_URL}/icon-512.png`,
    width: 512,
    height: 512,
  },
  sameAs: ["https://github.com/AI-Institute-Food-Systems/foodatlas"],
  parentOrganization: ORGANIZATION,
};

const LICENSE = "https://www.apache.org/licenses/LICENSE-2.0";

const VARIABLES_MEASURED = [
  {
    "@type": "PropertyValue",
    name: "Chemical concentration in food",
    unitText: "mg/100g",
    description:
      "Concentration of a chemical in a food, from USDA FoodData Central, PTFI and literature extraction, with the source of each value.",
  },
  {
    "@type": "PropertyValue",
    name: "Chemical-disease association",
    description:
      "Whether a chemical is reported to improve or worsen a disease (CTD literature), or is linked to it through shared bioassays.",
  },
  {
    "@type": "PropertyValue",
    name: "Bioactivity measurement",
    description:
      "Assay results of a chemical or food against a bioactivity (for example antioxidant), with endpoint, value and unit.",
  },
];

export const organizationJsonLd = () => ({
  "@context": "https://schema.org",
  ...FOODATLAS,
});

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
  // What the tables measure, one entry per kind of row.
  variableMeasured: VARIABLES_MEASURED,
  // Literature and source databases up to the latest release; the graph
  // has no fixed start date.
  ...(entries[0]?.release_date && {
    temporalCoverage: `../${entries[0].release_date}`,
  }),
  includedInDataCatalog: {
    "@type": "DataCatalog",
    name: "FoodAtlas",
    url: SITE_URL,
  },
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
  publisher: FOODATLAS,
  // No SearchAction: Google retired the sitelinks search box in 2024.
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
