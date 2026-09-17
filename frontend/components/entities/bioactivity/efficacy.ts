// Efficacy = "given how much of this chemical the food contains, does
// that dose clear the chemical's active threshold for this bioactivity."
// Rendered in the inferred-bioactivities table as a replacement for the
// old chemical-lab-potency "Top measurement" column. See the design
// note at repo root: inferred-bioactivity-efficacy-column.md.

import type { FoodEfficacyRow } from "@/types/Bioactivity";

// Key used to join /food/efficacy rows onto InferredRow rows in the
// inferred-bioactivities table. Both endpoints expose a `chemical`
// foodatlas id + a `bioactivity` foodatlas id; the pair uniquely
// identifies an inferred row.
export const efficacyKey = (chemId: string, bioId: string): string =>
  `${chemId}::${bioId}`;

// Index the efficacy response by join key. Rows where the bioactivity
// is UNCLASSIFIED (bioactivity_id_raw === "UNCLASSIFIED" and empty
// bioactivity_foodatlas_id) are dropped — they can't join to an
// InferredRow anyway.
export const indexEfficacy = (
  rows: FoodEfficacyRow[] | undefined | null
): Map<string, FoodEfficacyRow> => {
  const map = new Map<string, FoodEfficacyRow>();
  if (!rows) return map;
  for (const row of rows) {
    if (!row.bioactivity_foodatlas_id || !row.chemical_foodatlas_id) continue;
    map.set(
      efficacyKey(row.chemical_foodatlas_id, row.bioactivity_foodatlas_id),
      row
    );
  }
  return map;
};

// 0–1 fraction of maximal response at the food's in-food concentration,
// formatted as "99.9%" / "24.3%" / "" if null. Values above 0.99 render
// as ">99%" — the density-1 proxy saturates in that range and the extra
// precision isn't real (per Pranav 2026-08-04). Matches the backend's
// `saturated` bool threshold.
export const formatEfficacyFraction = (value: number | null): string => {
  if (value == null || Number.isNaN(value)) return "";
  if (value > 0.99) return ">99%";
  return `${(value * 100).toFixed(1)}%`;
};
