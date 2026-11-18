// What one composition "data point" is, and how filters count them.
//
// One data point is one extraction: the evidence modal lists extractions,
// and an evidence record (PTFI especially) can carry several. The row's
// "N data points" button, its trust badge and the modal all count through
// these helpers, so the number on the button is the number the modal opens
// on. The API counts the same way (repositories/_sources.py data_points).

import { SOURCE_DISPLAY_NAMES } from "@/components/entities/food/compositionSources";
import type { FoodEvidence, FoodEvidenceExtraction } from "@/types/Evidence";

export interface DataPointFilters {
  // Display names ("FDC", "FoodAtlas", "PTFI"); empty means every source.
  sources: string[];
  lowTrustOnly?: boolean;
  search?: string;
}

/** Page source keys ("fdc", ...) as modal source names; all or none → []. */
export const sourceNamesFor = (keys: string[] | undefined): string[] => {
  const names = (keys ?? []).map((k) => SOURCE_DISPLAY_NAMES[k] ?? k);
  return names.length >= Object.keys(SOURCE_DISPLAY_NAMES).length ? [] : names;
};

export const isLowTrust = (ex: FoodEvidenceExtraction): boolean =>
  Boolean(ex.trust_low);

export const matchesSources = (ev: FoodEvidence, sources: string[]): boolean =>
  sources.length === 0 || sources.includes(ev.reference.source_name);

export const matchesSearch = (
  ev: FoodEvidence,
  ex: FoodEvidenceExtraction,
  q: string
): boolean => {
  if (!q) return true;
  // Deliberately excludes ev.premise — the source text often mentions
  // dozens of unrelated chemicals/foods. Users search on WHAT was
  // extracted, not on what appeared somewhere in the paragraph.
  const haystack = [
    ex.extracted_chemical_name,
    ex.extracted_food_name,
    ex.extracted_concentration,
    ex.method,
    ev.reference.display_name,
    ev.reference.id,
    ...(ex.chemical_candidates ?? []),
    ...(ex.food_candidates ?? []),
  ]
    .filter((s): s is string => Boolean(s))
    .join(" ")
    .toLowerCase();
  return haystack.includes(q);
};

export const extractionMatches = (
  ev: FoodEvidence,
  ex: FoodEvidenceExtraction,
  f: DataPointFilters
): boolean =>
  matchesSources(ev, f.sources) &&
  (!f.lowTrustOnly || isLowTrust(ex)) &&
  matchesSearch(ev, ex, (f.search ?? "").trim().toLowerCase());

export const countDataPoints = (
  evidences: FoodEvidence[] | undefined | null,
  f: DataPointFilters
): number =>
  (evidences ?? []).reduce(
    (n, ev) => n + ev.extraction.filter((ex) => extractionMatches(ev, ex, f)).length,
    0
  );
