import { Publication } from "@/types";

// Single source of truth for every FoodAtlas self-citation on the site.
// Deliberately plain TS with no React import and no "use client", so the
// Server Components that render /about, /developers and
// /food-composition-downloads can all read it — same convention as
// components/basic/skeletonTokens.ts and entityTabs.config.ts.
//
// Before this existed the npj reference was retyped in three pages and its
// DOI in five. Every copy was missing the volume/issue/article number and
// silently dropped two of the nine authors. Change the record here, once.

export const PUBLICATIONS: Publication[] = [
  {
    authors:
      "Li, F., Youn, J., Xie, K., Chan, T., Gupta, P., Yoo, A., Gunning, M., Ni, K., & Tagkopoulos, I.",
    year: 2026,
    title:
      "A unified knowledge graph linking foodomics to chemical-disease networks and flavor profiles",
    venue: "npj Science of Food",
    kind: "journal",
    volume: "10",
    issue: "1",
    articleNumber: "33",
    doi: "10.1038/s41538-025-00680-9",
  },
  {
    authors: "Youn, J., Li, F., Simmons, G., Kim, S., & Tagkopoulos, I.",
    year: 2024,
    title:
      "FoodAtlas: Automated knowledge extraction of food and chemicals from literature",
    venue: "Computers in Biology and Medicine",
    kind: "journal",
    volume: "181",
    articleNumber: "109072",
    doi: "10.1016/j.compbiomed.2024.109072",
  },
  {
    authors: "Youn, J., Li, F., & Tagkopoulos, I.",
    year: 2023,
    title: "Semi-automated construction of food composition knowledge base",
    venue: "2nd AAAI Workshop on AI for Agriculture and Food Systems",
    kind: "proceedings",
    doi: "10.48550/arXiv.2301.11322",
  },
];

// The paper users are asked to cite. It describes the platform and the data
// the site actually serves, so it is what the "How to Cite" blocks render.
export const CANONICAL_PUBLICATION: Publication = PUBLICATIONS[0];

export const doiUrl = (doi: string): string => `https://doi.org/${doi}`;
