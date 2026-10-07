// The text of /llms.txt (app/llms.txt/route.ts). Counts and the latest
// bundle come from the API at request time; the file used to hand-type them,
// and they drift with every release. Pure, so a test can pin the wording.
import type { DownloadEntry } from "@/types";
import {
  CANONICAL_PUBLICATION,
  citationMarkdown,
  doiUrl,
} from "@/utils/publications";
import { API_URL, DOWNLOADS_PATH, SITE_URL } from "@/utils/site";

// /metadata/statistics, the same numbers the landing page shows.
export type GraphStatistics = {
  foods?: number;
  chemicals?: number;
  diseases?: number;
  bioactivities?: number;
  connections?: number;
};

const n = (value: number) => value.toLocaleString("en-US");

const summary = (s: GraphStatistics | null): string => {
  const parts = [
    s?.foods && `${n(s.foods)} foods`,
    s?.chemicals && `${n(s.chemicals)} chemicals`,
    s?.diseases && `${n(s.diseases)} diseases`,
    s?.bioactivities && `${n(s.bioactivities)} bioactivities`,
  ].filter(Boolean);
  // Without numbers the sentence still says what the graph holds.
  if (parts.length < 4) {
    return "Evidence-based food composition knowledge graph of foods, chemicals, diseases and bioactivities, every association traced to a peer-reviewed source.";
  }
  const links = s?.connections ? `, ${n(s.connections)} associations` : "";
  return `Evidence-based food composition knowledge graph: ${parts.slice(0, 3).join(", ")} and ${parts[3]}${links}, every one traced to a peer-reviewed source.`;
};

const release = (b: DownloadEntry | null): string =>
  b ? ` Latest release: ${b.version} (${b.release_date}).` : "";

export const buildLlmsTxt = (
  stats: GraphStatistics | null,
  latest: DownloadEntry | null
): string => `# FoodAtlas

> ${summary(stats)} Built by the AI Institute for Next Generation Food Systems (AIFS), UC Davis. Data is CC BY-NC 4.0 (non-commercial, attribution required); code is MIT.

Do not crawl the entity pages (/food/*, /chemical/*, /disease/*, /bioactivity/*) to collect the data — they render a small slice each, and the data calls behind them are rate-limited at the edge. Use one of the two sources below; both carry the complete graph and both are gated by the same free API key (request one at ${SITE_URL}/contact?api-access).

## Bulk download

- [Versioned database bundles](${SITE_URL}${DOWNLOADS_PATH}): zip per release with parquet tables (entities, triplets, attestations, bioactivity assays, per-version summary and changelog). Same graph as the website.${release(latest)} Programmatic: \`GET ${API_URL}/v1/bundles\` lists versions; \`GET /v1/bundles/{version}/download\` with your Bearer key 302s to a short-lived signed URL.

## API

- [Public REST API, base ${API_URL}/v1](${API_URL}/docs): foods, chemicals, diseases, bioactivities, triplets, attestations, search, stats. JSON, paginated, \`Authorization: Bearer <key>\`.
- [OpenAPI 3 spec](${API_URL}/openapi.json)
- [Get a free API key](${SITE_URL}/contact?api-access): 60 req/min per key.
- [Developer guide](${SITE_URL}/developers)

## About

- [How the graph is built](${SITE_URL}/technical-background)
- [Team and contact](${SITE_URL}/about)
- Cite: ${citationMarkdown(CANONICAL_PUBLICATION)} ${doiUrl(CANONICAL_PUBLICATION.doi)}
`;
