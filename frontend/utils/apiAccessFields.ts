// Structured questions asked on "API Access Request" submissions.
//
// These exist so the PI's review sheet gets the same columns for every
// request instead of an AI guessing them from a free-text message. The
// option lists are shared by the form and /contact/send, so the two can't
// drift; server-side parsing lives in apiAccessGuard.ts to keep next/server
// out of the client bundle.

export const API_ACCESS_TOPIC = "API Access Request";

export const USE_CATEGORIES = [
  "Academic research",
  "Student project",
  "Commercial",
  "Nonprofit",
  "Personal",
] as const;

export const COMMERCIAL = ["Yes", "No", "Not sure"] as const;

// Mirrors the /v1 resource groups on /developers.
export const DATA_NEEDED = [
  "Foods",
  "Chemicals",
  "Food composition",
  "Diseases",
  "Bioactivity",
  "Search & metadata",
  "Bulk bundles",
] as const;

export const VOLUMES = [
  "<1k requests/day",
  "1k–10k requests/day",
  ">10k requests/day",
  "One-off bulk download",
] as const;

export type UseCategory = (typeof USE_CATEGORIES)[number];
export type Commercial = (typeof COMMERCIAL)[number];
export type DataNeeded = (typeof DATA_NEEDED)[number];
export type Volume = (typeof VOLUMES)[number];

export interface ApiAccess {
  useCategory: UseCategory | "";
  commercial: Commercial | "";
  dataNeeded: DataNeeded[];
  volume: Volume | "";
  projectUrl: string;
}

export const EMPTY_API_ACCESS: ApiAccess = {
  useCategory: "",
  commercial: "",
  dataNeeded: [],
  volume: "",
  projectUrl: "",
};

// Listboxes and a checkbox group have no native `required`, so the form
// checks these itself before posting. The route re-validates regardless.
export const isApiAccessComplete = (v: ApiAccess): boolean =>
  v.useCategory !== "" &&
  v.commercial !== "" &&
  v.dataNeeded.length > 0 &&
  v.volume !== "";
