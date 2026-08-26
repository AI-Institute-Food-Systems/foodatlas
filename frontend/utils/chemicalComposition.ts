// Row shaping, sorting and bar geometry for the chemical page's
// "Foods Containing" table. Deliberately free of React and of any DOM
// dependency so the value->width math — the part that is easy to get
// subtly wrong and impossible to eyeball — is unit-testable on its own.

import { AmbiguitySibling } from "@/types/Metadata";

// Every field the /chemical/composition response adds beyond name+value is
// OPTIONAL. The API and the frontend deploy independently, so a frontend
// running ahead of the API must degrade to a missing count, not a crash.
export type ChemicalCompositionRow = {
  id: string;
  name: string;
  median_concentration?: { value: number; unit: string } | null;
  evidence_count?: number;
  fdc_count?: number;
  foodatlas_count?: number;
  ptfi_count?: number;
  ambiguity_siblings?: AmbiguitySibling[];
};

export type SortColumn = "name" | "median_concentration" | "evidence_count";
export type SortDirection = "asc" | "desc";

// One column spec, consumed by the real table AND by the Suspense
// skeleton. `SkeletonColumn` is a subset of this shape by design, so the
// skeleton's grid cannot drift from the table it stands in for.
export const COLUMNS: {
  key: string;
  label: string;
  width: string;
  align?: "left" | "right";
  sort?: SortColumn;
}[] = [
  { key: "name", label: "Food", width: "w-[36%]", sort: "name" },
  {
    key: "concentration",
    label: "Concentration (mg/100g)",
    width: "w-[44%]",
    sort: "median_concentration",
  },
  {
    key: "evidence",
    label: "Evidence",
    width: "w-[20%]",
    align: "right",
    sort: "evidence_count",
  },
];

export const COLUMN_LABELS = COLUMNS.map((c) => c.label);

export type SourceKey = "fdc" | "foodatlas" | "ptfi";

// Display order matches the food-side composition table's source facet.
export const SOURCES: { key: SourceKey; label: string }[] = [
  { key: "fdc", label: "FDC" },
  { key: "foodatlas", label: "FoodAtlas" },
  { key: "ptfi", label: "PTFI" },
];

const COUNT_FIELD: Record<SourceKey, keyof ChemicalCompositionRow> = {
  fdc: "fdc_count",
  foodatlas: "foodatlas_count",
  ptfi: "ptfi_count",
};

// A bar this short is still a visible nub rather than nothing. Without a
// floor, any food more than ~2 orders of magnitude below the top food
// rounds to a zero-width bar and reads as "no data" instead of "a little".
export const MIN_BAR_PERCENT = 1.5;

export const concentrationValue = (
  row: ChemicalCompositionRow
): number | null => {
  const v = row.median_concentration?.value;
  return typeof v === "number" && Number.isFinite(v) ? v : null;
};

export const evidenceCountOf = (row: ChemicalCompositionRow): number =>
  row.evidence_count ?? 0;

export const sourceCountOf = (
  row: ChemicalCompositionRow,
  key: SourceKey
): number => {
  const raw = row[COUNT_FIELD[key]];
  return typeof raw === "number" ? raw : 0;
};

// Labels for every source that actually contributed a data point to a row.
export const rowSourceLabels = (row: ChemicalCompositionRow): string[] =>
  SOURCES.filter(({ key }) => sourceCountOf(row, key) > 0).map((s) => s.label);

// The scale denominator. Runs over the FULL result set — never a page slice
// and never a search-filtered slice — so a bar means the same length no
// matter which page it is viewed on or what the user has typed into search.
// Passing a filtered array here is the one mistake that silently breaks the
// whole visualisation, which is why it is a standalone tested function.
export const computeMaxValue = (rows: ChemicalCompositionRow[]): number => {
  let max = 0;
  for (const row of rows) {
    const v = concentrationValue(row);
    if (v !== null && v > max) max = v;
  }
  return max;
};

// null => render an em dash and no fill (the food has evidence but no
// measured amount). Distinct from 0, which is a real measurement.
export const barPercent = (
  value: number | null,
  max: number
): number | null => {
  if (value === null || !Number.isFinite(max) || max <= 0) return null;
  if (value <= 0) return null;
  return Math.max(MIN_BAR_PERCENT, Math.min(100, (value / max) * 100));
};

// Below this the two-decimal readout renders "0.00%", which states the
// opposite of the truth: the amount is small, not zero. An earlier pass
// dropped the share entirely at that point, but a blank cell is its own
// wrong answer — it reads as "unknown" when the share is in fact known and
// simply tiny. Render the bound instead.
const MIN_MEANINGFUL_PERCENT = 0.005;
export const TRACE_PERCENT_LABEL = "<0.01%";

// Percentage of the food's mass. mg/100g -> /1000; decimals scale with
// magnitude so trace values don't all collapse to "0%". Mirrors the food
// composition table's readout so the same number reads the same on both
// pages.
export const formatPercentByMass = (
  row: ChemicalCompositionRow
): string | null => {
  const v = concentrationValue(row);
  const unit = row.median_concentration?.unit;
  if (v === null || !unit) return null;
  if (unit.replace(/\s+/g, "").toLowerCase() !== "mg/100g") return null;
  const pct = v / 1000;
  if (pct < MIN_MEANINGFUL_PERCENT) return TRACE_PERCENT_LABEL;
  if (pct >= 10) return `${pct.toFixed(0)}%`;
  if (pct >= 1) return `${pct.toFixed(1)}%`;
  return `${pct.toFixed(2)}%`;
};

export const mergeBuckets = (
  withConcentrations: ChemicalCompositionRow[] | null | undefined,
  withoutConcentrations: ChemicalCompositionRow[] | null | undefined,
  includeUnmeasured: boolean
): ChemicalCompositionRow[] => {
  const measured = withConcentrations ?? [];
  if (!includeUnmeasured) return [...measured];
  // Normalise the unmeasured bucket so downstream code has exactly one
  // representation of "no measured amount" to branch on.
  const unmeasured = (withoutConcentrations ?? []).map((row) => ({
    ...row,
    median_concentration: null,
  }));
  return [...measured, ...unmeasured];
};

export const filterRows = (
  rows: ChemicalCompositionRow[],
  { search, sources }: { search: string; sources: SourceKey[] }
): ChemicalCompositionRow[] => {
  const needle = search.trim().toLowerCase();
  return rows.filter((row) => {
    if (needle && !row.name.toLowerCase().includes(needle)) return false;
    // No source selected means "no filter", matching the food table's
    // facet behaviour — an empty selection shows everything rather than
    // nothing, so clearing the last checkbox can't blank the table.
    if (sources.length === 0) return true;
    return sources.some((key) => sourceCountOf(row, key) > 0);
  });
};

const compareText = (a: string, b: string): number =>
  a.localeCompare(b, undefined, { sensitivity: "base" });

// Rows with no measured concentration sort last in BOTH directions, the
// same way the food-side query orders `NULLS LAST`. Flipping direction is
// meant to reverse the ranking of things that *have* a value; letting the
// blanks surface to the top on asc would bury the actual data.
export const sortRows = (
  rows: ChemicalCompositionRow[],
  column: SortColumn,
  direction: SortDirection
): ChemicalCompositionRow[] => {
  const sign = direction === "asc" ? 1 : -1;
  return [...rows].sort((a, b) => {
    if (column === "name") return sign * compareText(a.name, b.name);

    if (column === "evidence_count") {
      const diff = evidenceCountOf(a) - evidenceCountOf(b);
      return diff !== 0 ? sign * diff : compareText(a.name, b.name);
    }

    const va = concentrationValue(a);
    const vb = concentrationValue(b);
    if (va === null && vb === null) return compareText(a.name, b.name);
    if (va === null) return 1;
    if (vb === null) return -1;
    return va !== vb ? sign * (va - vb) : compareText(a.name, b.name);
  });
};

export const paginate = <T>(
  rows: T[],
  currentPage: number,
  rowsPerPage: number
): T[] => {
  const start = (currentPage - 1) * rowsPerPage;
  return rows.slice(start, start + rowsPerPage);
};
