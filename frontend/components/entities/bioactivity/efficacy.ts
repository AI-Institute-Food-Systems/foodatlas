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

// Formatted log ratio: "+1.99" / "−0.24" (uses the proper minus sign
// so column alignment with the tabular-nums font stays clean).
export const formatDoseOverAc50Log = (value: number | null): string => {
  if (value == null || Number.isNaN(value)) return "";
  const abs = Math.abs(value).toFixed(2);
  return value >= 0 ? `+${abs}` : `−${abs}`;
};
