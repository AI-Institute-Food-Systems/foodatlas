import {
  BioactivityMeasurement,
  BioactivityPotencySummary,
  BioactivityTopMeasurement,
} from "@/types";

function formatNumber(value: number | null | undefined): string {
  if (value === null || value === undefined) return "—";
  return value.toLocaleString(undefined, { maximumSignificantDigits: 3 });
}

// `"None"` is a sentinel the API emits for null/blank units (see
// backend/api/src/repositories/_bioact_hotfix.py normalize_unit) — it must
// render as no unit at all, not as the literal word. The measurements modal
// guards the same value inline; keep both in sync until the upstream
// cleanup lands and the sentinel stops being produced.
export function displayUnit(unit: string | null | undefined): string {
  return unit && unit !== "None" ? ` ${unit}` : "";
}

export function formatTopPotency(
  summary: BioactivityPotencySummary[] | null | undefined
): string {
  if (!summary || summary.length === 0) return "—";
  const top = [...summary].sort((a, b) => b.n - a.n)[0];
  const median = formatNumber(top.median);
  return `${top.endpoint ?? "?"}: ${median}${displayUnit(top.unit)} (n=${top.n})`;
}

// "Headline" measurement surfaced in the bioactivity tables. Backend picks
// it as max-by-value; we render it as "{endpoint}: {value} {unit}" — or
// just "{value} {unit}" if no endpoint label. Same shape on food + chemical
// tables so the columns line up visually.
export function formatTopMeasurement(
  top: BioactivityTopMeasurement | null | undefined
): string {
  if (!top || top.value === null || top.value === undefined) return "—";
  const value = formatNumber(top.value);
  return top.endpoint
    ? `${top.endpoint}: ${value}${displayUnit(top.unit)}`
    : `${value}${displayUnit(top.unit)}`;
}

// Pulls top_measurement from a row — prefers the backend-computed field,
// falls back to scanning the row's `measurements` sample client-side for
// the max-by-value entry. The fallback exists because the new backend
// field isn't deployed to staging yet; once it is, the fast path is hit
// for free and this helper can keep working unchanged.
export function topMeasurementOf(row: {
  top_measurement?: BioactivityTopMeasurement | null;
  measurements?: BioactivityMeasurement[];
}): BioactivityTopMeasurement | null {
  if (
    row.top_measurement &&
    row.top_measurement.value !== null &&
    row.top_measurement.value !== undefined
  ) {
    return row.top_measurement;
  }
  const ms = row.measurements ?? [];
  let top: BioactivityTopMeasurement | null = null;
  for (const m of ms) {
    if (m.value === null || m.value === undefined) continue;
    if (top === null || m.value > (top.value ?? Number.NEGATIVE_INFINITY)) {
      top = { endpoint: m.endpoint, value: m.value, unit: m.unit };
    }
  }
  return top;
}

export function formatFoodMeasurement(
  m: BioactivityMeasurement | undefined
): string {
  if (!m) return "—";
  if (m.value === null || m.value === undefined) return m.outcome ?? "—";
  return `${formatNumber(m.value)}${displayUnit(m.unit)}`;
}

// PubChem's `efficacy_logac50_value` is log10 of the AC50 **in molar**,
// while a row's `value` is in the assay's own unit (uM on ~95% of
// fitted rows, nM on the rest). Printing 10^logAC50 next to the row's
// unit therefore read "AC50 0.0000141 uM" for a 14.1 uM row — six
// orders of magnitude too potent. Shift the log value into the row's
// unit when it is a molar prefix; otherwise say "M" honestly, since the
// number is in molar whatever the assay reported in.
const LOG10_PER_MOLAR_UNIT: Record<string, number> = {
  m: 0,
  molar: 0,
  mm: 3,
  millimolar: 3,
  um: 6,
  "µm": 6,
  "μm": 6,
  micromolar: 6,
  nm: 9,
  nanomolar: 9,
  pm: 12,
  picomolar: 12,
};

export function logAC50InUnit(
  logAC50Molar: number,
  unit: string | null | undefined
): { logAC50: number; unit: string } {
  const key = (unit ?? "").trim().toLowerCase();
  const shift = LOG10_PER_MOLAR_UNIT[key];
  return shift === undefined
    ? { logAC50: logAC50Molar, unit: "M" }
    : { logAC50: logAC50Molar + shift, unit: unit!.trim() };
}
