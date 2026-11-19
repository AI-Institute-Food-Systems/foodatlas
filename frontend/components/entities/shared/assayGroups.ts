// The Health tab's Signal and Activity filters over a chemical↔disease
// pair's assay groups (API `assay_groups`: the pair's assays split by
// their exact activities + relationships). One assay must match every
// active filter, so counting through the groups is exact. The row filter,
// its facet counts, the row's three count buttons and the three modals
// all go through these helpers, so they cannot disagree.

import type {
  AssayGroup,
  AssayInferredAssociation,
  AssayTarget,
} from "@/types/AssayInferred";
import { ASSAY_LIST_CAP } from "@/types/AssayInferred";

export interface AssayFilters {
  signals: string[];
  activities: string[];
}
export type AssayDimension = "signals" | "activities";

const anyOf = (values: string[], selected: string[]) =>
  selected.length === 0 || values.some((v) => selected.includes(v));

const isOpen = (f: AssayFilters, skip?: AssayDimension) =>
  (skip === "signals" || f.signals.length === 0) &&
  (skip === "activities" || f.activities.length === 0);

export const groupMatches = (
  g: AssayGroup,
  f: AssayFilters,
  skip?: AssayDimension
): boolean =>
  (skip === "signals" || anyOf(g.relationships, f.signals)) &&
  (skip === "activities" || anyOf(g.activities, f.activities));

export const matchingGroups = (
  row: AssayInferredAssociation,
  f: AssayFilters,
  skip?: AssayDimension
): AssayGroup[] =>
  (row.assay_groups ?? []).filter((g) => groupMatches(g, f, skip));

/** Row survives when one of its assays matches every active filter. */
export const rowMatches = (
  row: AssayInferredAssociation,
  f: AssayFilters,
  skip?: AssayDimension
): boolean => isOpen(f, skip) || matchingGroups(row, f, skip).length > 0;

export const filteredAssayCount = (
  row: AssayInferredAssociation,
  f: AssayFilters
): number =>
  isOpen(f)
    ? row.n_assays
    : matchingGroups(row, f).reduce((n, g) => n + g.n_assays, 0);

export const filteredAssays = (
  row: AssayInferredAssociation,
  f: AssayFilters
): string[] =>
  isOpen(f)
    ? row.assays
    : matchingGroups(row, f)
        .flatMap((g) => g.assays)
        .slice(0, ASSAY_LIST_CAP);

export const filteredTargets = (
  row: AssayInferredAssociation,
  f: AssayFilters
): AssayTarget[] => {
  if (isOpen(f)) return row.targets ?? [];
  const seen = new Set<string>();
  const out: AssayTarget[] = [];
  for (const t of matchingGroups(row, f).flatMap((g) => g.targets ?? [])) {
    const key = (t.label ?? t.id).toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(t);
  }
  return out;
};

/** Activities of the matching assays; the Activity filter keeps only its picks. */
export const filteredActivities = (
  row: AssayInferredAssociation,
  f: AssayFilters
): string[] => {
  if (isOpen(f)) return row.bioactivities ?? [];
  const acts = new Set(
    matchingGroups(row, f).flatMap((g) => g.activities)
  );
  return Array.from(acts)
    .filter((a) => f.activities.length === 0 || f.activities.includes(a))
    .sort();
};

/** Rows per signal value, the Activity filter applied (the signal's own skipped). */
export const signalFacet = (
  rows: AssayInferredAssociation[],
  f: AssayFilters,
  universe: string[]
): Record<string, number> =>
  Object.fromEntries(
    universe.map((s) => [
      s,
      rows.filter((r) => rowMatches(r, { ...f, signals: [s] })).length,
    ])
  );

/** Rows per activity, the Signal filter applied (the activity's own skipped). */
export const activityFacet = (
  rows: AssayInferredAssociation[],
  f: AssayFilters,
  universe: string[]
): Record<string, number> =>
  Object.fromEntries(
    universe.map((a) => [
      a,
      rows.filter((r) => rowMatches(r, { ...f, activities: [a] })).length,
    ])
  );
